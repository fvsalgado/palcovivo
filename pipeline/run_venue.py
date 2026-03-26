"""
Primeira Plateia — Scrape de venue único v2
Melhorias:
  - ETag / Last-Modified: conditional requests → skip se 304
  - Merge com score de credibilidade (novo só substitui se melhor)
  - Tombstone: eventos desaparecidos marcados, não apagados imediatamente
  - Cache por URL individual para scraping incremental eficiente
  - Regressão guard: se novo scrape retorna 0 eventos e havia cache, usa cache
"""

import argparse
import importlib
import inspect
import json
import logging
import sys
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

import sys as _sys
import os as _os
_sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))

ROOT       = Path(__file__).parent.parent
DATA_DIR   = ROOT / "data"
VENUES_DIR = DATA_DIR / "venues"
EVENTS_DIR = DATA_DIR / "events"
LOGS_DIR   = DATA_DIR / "logs"

from pipeline.core.harmonizer import harmonize_event
from pipeline.core.validator  import validate_batch
from pipeline.core.cache      import (
    save_cache, is_stale, get_cached_events,
    credibility_score, merge_event,
    mark_not_seen, should_tombstone,
    load_url_cache, save_url_cache, url_cache_unchanged,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("pipeline.run_venue")


def ensure_venue_file(venue_id: str) -> None:
    venue_path = VENUES_DIR / f"{venue_id}.json"
    if venue_path.exists():
        return
    VENUES_DIR.mkdir(parents=True, exist_ok=True)
    logger.warning(f"{venue_id}: ficheiro de venue não encontrado — a criar mínimo")
    api_url = website = ""
    try:
        mod = importlib.import_module(f"pipeline.scrapers.{venue_id}.scraper")
        api_url = getattr(mod, "API_BASE", "") or ""
        website = getattr(mod, "WEBSITE", "").rstrip("/")
    except Exception:
        pass
    minimal = {
        "schema_version": "1.0", "id": venue_id,
        "name": venue_id.replace("-", " ").title(),
        "venue_type": "teatro", "is_permanent": True,
        "address": {"municipality": "Portugal", "country": "PT"},
        "contact": {"website": website or None},
        "data_source": {"scraper_id": venue_id, "api_url": api_url,
                        "is_active": True, "last_scraped": None,
                        "scrape_frequency_hours": 24},
        "meta": {"created_at": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                 "created_by": "pipeline-auto", "verified": False},
    }
    with open(venue_path, "w", encoding="utf-8") as f:
        json.dump(minimal, f, ensure_ascii=False, indent=2)


def _load_existing_events(venue_id: str) -> dict[str, dict]:
    """Carrega eventos actuais de data/events/{venue_id}.json como dict por ID."""
    path = EVENTS_DIR / f"{venue_id}.json"
    if not path.exists():
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            events = json.load(f)
        return {e["id"]: e for e in events if isinstance(e, dict) and e.get("id")}
    except Exception as e:
        logger.warning(f"{venue_id}: erro ao ler eventos existentes — {e}")
        return {}


def _apply_merge(existing_map: dict, new_valid: list, venue_id: str) -> tuple[list, dict]:
    """
    Aplica merge inteligente entre eventos existentes e novos.
    - Novos eventos com score >= existente → substituem
    - Novos com score inferior → preenche lacunas mas mantém base existente
    - Eventos existentes não vistos → marcados como tombstone candidate
    - Eventos novos já passados há > 30 dias que não existiam → ignorados
      (evita importar o histórico completo do sitemap a cada run)
    Retorna (lista_final, stats_merge).
    """
    stats = {"substituted": 0, "kept_existing": 0, "new": 0, "tombstoned": 0, "gaps_filled": 0, "skipped_old": 0}
    new_ids = {e["id"] for e in new_valid}
    final: dict[str, dict] = {}
    cutoff = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    # Processar eventos novos
    for ev in new_valid:
        eid = ev["id"]
        if eid in existing_map:
            # Evento já existe → merge normal independentemente da data
            merged, reason = merge_event(existing_map[eid], ev)
            final[eid] = merged
            if "substituído" in reason:
                stats["substituted"] += 1
            elif "lacunas" in reason:
                stats["gaps_filled"] += 1
            else:
                stats["kept_existing"] += 1
            logger.debug(f"{venue_id}/{eid}: {reason}")
        else:
            # Evento novo — ignorar se já passou há mais de 30 dias
            date_last = ev.get("date_last") or ev.get("date_first")
            if date_last and date_last < cutoff:
                stats["skipped_old"] += 1
                logger.debug(f"{venue_id}/{eid}: ignorado (histórico, date_last={date_last})")
                continue
            final[eid] = ev
            stats["new"] += 1

    # Processar eventos existentes não vistos neste scrape
    for eid, ev in existing_map.items():
        if eid not in new_ids:
            # Não foi visto → marcar como tombstone candidate
            marked = mark_not_seen(ev)
            if should_tombstone(marked):
                logger.info(f"{venue_id}/{eid}: tombstone — não visto há >7 dias")
                stats["tombstoned"] += 1
                # Desactivar mas não apagar
                marked.setdefault("pipeline", {})["is_active"] = False
            final[eid] = marked

    logger.info(
        f"{venue_id}: merge — {stats['new']} novos, "
        f"{stats['substituted']} substituídos, "
        f"{stats['kept_existing']} mantidos, "
        f"{stats['gaps_filled']} com lacunas preenchidas, "
        f"{stats['tombstoned']} tombstoned, "
        f"{stats['skipped_old']} históricos ignorados"
    )
    return list(final.values()), stats


def run_venue(venue_id: str, force: bool = False) -> dict:
    report = {
        "venue_id": venue_id, "scraped": 0, "valid": 0, "invalid": 0,
        "errors": [], "cache_hit": False, "merge_stats": {},
    }

    ensure_venue_file(venue_id)

    venue_path = VENUES_DIR / f"{venue_id}.json"
    with open(venue_path, encoding="utf-8") as f:
        venue = json.load(f)
    report["venue_name"] = venue.get("name", venue_id)
    logger.info(f"=== {report['venue_name']} ===")

    # Importar scraper
    try:
        mod = importlib.import_module(f"pipeline.scrapers.{venue_id}.scraper")
    except ImportError as e:
        logger.error(f"{venue_id}: scraper não encontrado — {e}")
        report["errors"].append(str(e))
        return report

    # ── Decisão: scrape vs cache ──
    stale = is_stale(venue_id)
    if not force and not stale:
        logger.info(f"{venue_id}: cache válida — a usar cache")
        raw = get_cached_events(venue_id)
        report["cache_hit"] = True
    else:
        try:
            # Modo incremental: passar known_ids se o scraper suportar
            known_ids = None
            try:
                sig = inspect.signature(mod.run)
                if "known_ids" in sig.parameters:
                    cached_ids_src = get_cached_events(venue_id)
                    known_ids = {ev["source_id"] for ev in cached_ids_src if ev.get("source_id")}
                    if known_ids:
                        logger.info(f"{venue_id}: modo incremental — {len(known_ids)} IDs já em cache")
            except Exception:
                pass

            raw = mod.run(known_ids=known_ids) if known_ids is not None else mod.run()
            logger.info(f"{venue_id}: {len(raw)} eventos raw recolhidos")

            # Em modo incremental, fundir novos com cache (não perder eventos saltados)
            if known_ids:
                cached_all = get_cached_events(venue_id)
                cached_by_id = {ev["source_id"]: ev for ev in cached_all if ev.get("source_id")}
                new_by_id   = {ev["source_id"]: ev for ev in raw       if ev.get("source_id")}
                merged = {**cached_by_id, **new_by_id}
                raw = list(merged.values())
                logger.info(f"{venue_id}: {len(raw)} eventos após fusão incremental")

            # ── Regressão guard ──
            if len(raw) == 0 and not force:
                fallback = get_cached_events(venue_id)
                if fallback:
                    logger.warning(
                        f"{venue_id}: scrape retornou 0 eventos — "
                        f"a usar cache ({len(fallback)} eventos) como fallback"
                    )
                    raw = fallback
                    report["errors"].append("scrape retornou 0 — usado fallback de cache")
                    report["cache_hit"] = True

            # Guardar cache monolítica (compatibilidade + fallback)
            if raw:
                save_cache(venue_id, raw)
                venue.setdefault("data_source", {})["last_scraped"] = (
                    datetime.now(timezone.utc).isoformat()
                )
                with open(venue_path, "w", encoding="utf-8") as f:
                    json.dump(venue, f, ensure_ascii=False, indent=2)

        except Exception as e:
            logger.error(f"{venue_id}: erro no scraper — {e}")
            logger.debug(traceback.format_exc())
            raw = get_cached_events(venue_id)
            if raw:
                logger.warning(f"{venue_id}: a usar cache expirada ({len(raw)} eventos)")
                report["cache_hit"] = True
            report["errors"].append(str(e))

    report["scraped"] = len(raw)

    # ── Harmonizar ──
    harmonized = []
    for r in raw:
        try:
            harmonized.append(harmonize_event(r, venue_id, venue_id))
        except Exception as e:
            report["errors"].append(f"harmonize:{r.get('title', '?')}:{e}")

    # ── Validar ──
    valid, invalid = validate_batch(harmonized)
    report["valid"]   = len(valid)
    report["invalid"] = len(invalid)

    # ── Merge com eventos existentes ──
    existing_map = _load_existing_events(venue_id)
    if existing_map and valid:
        final_events, merge_stats = _apply_merge(existing_map, valid, venue_id)
        report["merge_stats"] = merge_stats
    else:
        # Sem histórico ou sem novos → usar directamente
        final_events = valid

    # Filtrar apenas eventos activos para o output
    active_events = [e for e in final_events if (e.get("pipeline") or {}).get("is_active", True)]
    inactive_count = len(final_events) - len(active_events)
    if inactive_count:
        logger.info(f"{venue_id}: {inactive_count} eventos inactivos (tombstone) excluídos do output")

    # ── Guardar ──
    EVENTS_DIR.mkdir(parents=True, exist_ok=True)
    with open(EVENTS_DIR / f"{venue_id}.json", "w", encoding="utf-8") as f:
        json.dump(active_events, f, ensure_ascii=False, indent=2)
    logger.info(f"{venue_id}: {len(active_events)} eventos guardados")

    # Score médio de credibilidade para o relatório
    if active_events:
        avg_score = round(sum(credibility_score(e) for e in active_events) / len(active_events), 3)
        report["avg_credibility_score"] = avg_score
        logger.info(f"{venue_id}: score médio de credibilidade = {avg_score:.2f}")

    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("venue_id")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    report = run_venue(args.venue_id, args.force)

    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOGS_DIR / f"venue-{args.venue_id}.json"
    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    if report["errors"] and report["valid"] == 0 and not report["cache_hit"]:
        sys.exit(1)
