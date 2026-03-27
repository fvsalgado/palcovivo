"""
Primeira Plateia — Agregador v3
Melhorias v3:
  - master.json: apenas eventos activos com date_last >= hoje-30d
  - data/archive/YYYY.json: eventos passados agrupados por ano
  - Inclui quality_score do validator no relatório por venue
  - Campos de qualidade do validator (quality_warnings) no relatório
Melhorias v4:
  - Manifest system: skip re-agregação quando nada mudou (--force para forçar)
  - Fragmentação do master.json: data/index.json + data/events/by-month/YYYY-MM.json
"""

import argparse
import hashlib
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import sys as _sys
import os as _os
_sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))

ROOT        = Path(__file__).parent.parent
DATA_DIR    = ROOT / "data"
EVENTS_DIR  = DATA_DIR / "events"
LOGS_DIR    = DATA_DIR / "logs"
ARCHIVE_DIR = DATA_DIR / "archive"
CACHE_DIR   = DATA_DIR / "cache"
MANIFEST_PATH = CACHE_DIR / ".agg_manifest.json"

from pipeline.core.dedup      import deduplicate
from pipeline.core.cache      import credibility_score, should_tombstone
from pipeline.core.validator  import field_quality_report, quality_score

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("pipeline.aggregate")


# ---------------------------------------------------------------------------
# Manifest helpers
# ---------------------------------------------------------------------------

def _compute_manifest() -> dict:
    """Computa {venue_id: md5_hash} para todos os ficheiros de eventos."""
    manifest = {}
    for path in sorted(EVENTS_DIR.glob("*.json")):
        if path.name == ".gitkeep":
            continue
        manifest[path.stem] = hashlib.md5(path.read_bytes()).hexdigest()
    return manifest


def _load_manifest() -> dict:
    """Carrega o manifesto salvo, retorna {} se não existir."""
    if MANIFEST_PATH.exists():
        try:
            with open(MANIFEST_PATH, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def _save_manifest(manifest: dict) -> None:
    """Guarda o manifesto em data/cache/.agg_manifest.json."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    logger.info(f"Manifesto salvo em {MANIFEST_PATH}")


# ---------------------------------------------------------------------------
# Load + quality
# ---------------------------------------------------------------------------

def load_all() -> tuple[list[dict], list[dict]]:
    all_events, venue_reports = [], []
    for path in sorted(EVENTS_DIR.glob("*.json")):
        if path.name == ".gitkeep":
            continue
        try:
            with open(path, encoding="utf-8") as f:
                events = json.load(f)
            venue_id = path.stem
            # Excluir eventos inactivos (tombstoned)
            active   = [e for e in events if (e.get("pipeline") or {}).get("is_active", True)]
            inactive = len(events) - len(active)
            all_events.extend(active)
            logger.info(
                f"{venue_id}: {len(active)} eventos activos carregados"
                + (f" ({inactive} tombstoned excluídos)" if inactive else "")
            )

            # Ler relatório parcial do run_venue
            log_path = LOGS_DIR / f"venue-{venue_id}.json"
            if log_path.exists():
                with open(log_path, encoding="utf-8") as f:
                    venue_reports.append(json.load(f))
            else:
                venue_reports.append({
                    "venue_id": venue_id, "venue_name": venue_id,
                    "scraped": len(active), "valid": len(active),
                    "invalid": 0, "errors": [],
                })
        except Exception as e:
            logger.error(f"Erro ao carregar {path}: {e}")
    return all_events, venue_reports


def quality_stats(events: list[dict]) -> dict:
    """Calcula estatísticas de qualidade de dados."""
    total = len(events) or 1
    return {
        "total": len(events),
        "with_image":       round(sum(1 for e in events if (e.get("media") or {}).get("cover_image")) / total * 100, 1),
        "with_price":       round(sum(1 for e in events if (e.get("price") or {}).get("price_display") or (e.get("price") or {}).get("is_free")) / total * 100, 1),
        "with_description": round(sum(1 for e in events if e.get("description") and len(e["description"]) > 20) / total * 100, 1),
        "with_category":    round(sum(1 for e in events if e.get("category") and e["category"] != "outros") / total * 100, 1),
        "avg_credibility":  round(sum(credibility_score(e) for e in events) / total, 3) if events else 0,
    }


def _split_active_archive(events: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Separa eventos activos (master) de passados (archive).
    Master: activos + passados há <= 30 dias.
    Archive: passados há > 30 dias.
    """
    from datetime import datetime, timedelta
    cutoff = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    active, archive = [], []
    for e in events:
        # Tombstone imediato já deve ter sido aplicado pelo run_venue
        # mas verificamos aqui também como safety net
        if should_tombstone(e):
            e.setdefault("pipeline", {})["is_active"] = False
            archive.append(e)
            continue
        date_last = e.get("date_last") or e.get("date_first") or ""
        if date_last and date_last < cutoff:
            archive.append(e)
        else:
            active.append(e)
    return active, archive


def _write_archive(archive_events: list[dict]) -> None:
    """Agrupa eventos de arquivo por ano e escreve data/archive/YYYY.json."""
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    by_year: dict[str, list] = {}
    for e in archive_events:
        year = (e.get("date_last") or e.get("date_first") or "0000")[:4]
        by_year.setdefault(year, []).append(e)
    for year, evs in by_year.items():
        evs.sort(key=lambda e: e.get("date_first") or "")
        path = ARCHIVE_DIR / f"{year}.json"
        # Ler arquivo existente e fundir (não perder anos anteriores)
        existing = {}
        if path.exists():
            try:
                with open(path, encoding="utf-8") as f:
                    existing_data = json.load(f)
                existing = {e["id"]: e for e in existing_data.get("events", [])}
            except Exception:
                pass
        for e in evs:
            existing[e["id"]] = e
        merged = sorted(existing.values(), key=lambda e: e.get("date_first") or "")
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"year": year, "total": len(merged), "events": merged},
                      f, ensure_ascii=False, indent=2)
        logger.info(f"Archive {year}: {len(merged)} eventos guardados em {path.name}")


# ---------------------------------------------------------------------------
# Master + fragments
# ---------------------------------------------------------------------------

def generate_master(events: list[dict]) -> dict:
    events.sort(key=lambda e: (e.get("date_first") or "9999-99-99"))
    by_domain, by_venue, by_month = {}, {}, {}
    today_ids, free_ids, family_ids = [], [], []
    today = datetime.now().strftime("%Y-%m-%d")

    for e in events:
        eid = e["id"]
        by_domain.setdefault(e.get("domain", "outros"), []).append(eid)
        by_venue.setdefault(e.get("venue_id", ""), []).append(eid)
        df = e.get("date_first", "")
        if df:
            by_month.setdefault(df[:7], []).append(eid)
        for d in e.get("dates", []):
            if d.get("date") == today:
                today_ids.append(eid)
                break
        if (e.get("price") or {}).get("is_free"):
            free_ids.append(eid)
        if (e.get("audience") or {}).get("is_family"):
            family_ids.append(eid)

    return {
        "generated_at":  datetime.now(timezone.utc).isoformat() + "Z",
        "total_events":  len(events),
        "total_venues":  len(by_venue),
        "quality":       quality_stats(events),
        "events":        events,
        "index": {
            "by_domain": by_domain,
            "by_venue":  by_venue,
            "by_month":  by_month,
            "today":     today_ids,
            "free":      free_ids,
            "family":    family_ids,
        },
    }


def write_fragments(master: dict) -> None:
    """
    Gera fragmentos a partir do master.json:
      1. data/index.json  — estrutura completa sem "events"
      2. data/events/by-month/YYYY-MM.json — eventos por mês
    """
    # 1. index.json (lightweight — sem events array)
    index_doc = {
        "generated_at": master["generated_at"],
        "total_events": master["total_events"],
        "total_venues": master["total_venues"],
        "quality":      master["quality"],
        "index":        master["index"],
    }
    index_path = DATA_DIR / "index.json"
    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(index_doc, f, ensure_ascii=False, indent=2)
    logger.info(f"index.json gerado ({index_path})")

    # 2. by-month files
    by_month_dir = EVENTS_DIR / "by-month"
    by_month_dir.mkdir(parents=True, exist_ok=True)

    # Build lookup: id -> event
    events_by_id = {e["id"]: e for e in master["events"]}
    by_month_index = master["index"]["by_month"]

    files_written = 0
    for month, ids in sorted(by_month_index.items()):
        month_events = [events_by_id[eid] for eid in ids if eid in events_by_id]
        month_doc = {
            "month":  month,
            "total":  len(month_events),
            "events": month_events,
        }
        month_path = by_month_dir / f"{month}.json"
        with open(month_path, "w", encoding="utf-8") as f:
            json.dump(month_doc, f, ensure_ascii=False, indent=2)
        files_written += 1

    logger.info(f"by-month: {files_written} ficheiro(s) criado(s) em {by_month_dir}")


# ---------------------------------------------------------------------------
# Main run
# ---------------------------------------------------------------------------

def run(force: bool = False) -> None:
    start = datetime.now(timezone.utc)
    logger.info("=" * 60)
    logger.info("Primeira Plateia — Agregador iniciado")
    logger.info("=" * 60)

    # --- Manifest check ---
    current_manifest = _compute_manifest()
    saved_manifest   = _load_manifest()

    if current_manifest == saved_manifest and not force:
        logger.info("Manifesto idêntico ao anterior — nada mudou. A saltar agregação. (use --force para forçar)")
        return

    if force:
        logger.info("--force: a forçar re-agregação mesmo sem alterações")
    else:
        changed = [k for k in current_manifest if current_manifest.get(k) != saved_manifest.get(k)]
        new_venues = [k for k in current_manifest if k not in saved_manifest]
        logger.info(
            f"Manifesto alterado: {len(changed)} venue(s) modificado(s)"
            + (f", {len(new_venues)} novo(s)" if new_venues else "")
        )

    all_events, venue_reports = load_all()
    logger.info(f"Total: {len(all_events)} eventos de {len(venue_reports)} venues")

    deduped = deduplicate(all_events)
    logger.info(f"Após dedup: {len(deduped)} eventos")

    # Separar activos (master) de passados (archive)
    active, archive = _split_active_archive(deduped)
    logger.info(f"Master: {len(active)} activos | Archive: {len(archive)} passados")

    # Escrever archive por ano
    if archive:
        _write_archive(archive)

    master = generate_master(active)
    with open(DATA_DIR / "master.json", "w", encoding="utf-8") as f:
        json.dump(master, f, ensure_ascii=False, indent=2)
    logger.info(f"master.json gerado ({len(active)} eventos)")

    # Fragmentos: index.json + by-month
    write_fragments(master)

    # Qualidade por venue (credibilidade + quality_score do validator)
    venue_quality = {}
    for vid, ids in master["index"]["by_venue"].items():
        id_set = set(ids)
        evs    = [e for e in active if e["id"] in id_set]
        qs     = field_quality_report(evs) if evs else {}
        venue_quality[vid] = {
            **quality_stats(evs),
            "avg_quality_score": qs.get("global", {}).get("avg_score", 0),
            "field_pct": {
                k: v["pct"]
                for k, v in qs.get("global", {}).get("fields", {}).items()
            },
        }
        logger.info(
            f"{vid}: credibilidade={venue_quality[vid]['avg_credibility']:.2f} "
            f"quality_score={venue_quality[vid]['avg_quality_score']:.3f} "
            f"imagem={venue_quality[vid]['with_image']}% "
            f"preço={venue_quality[vid]['with_price']}%"
        )

    duration = (datetime.now(timezone.utc) - start).total_seconds()
    report = {
        "run_at":           datetime.now(timezone.utc).isoformat() + "Z",
        "duration_seconds": round(duration, 1),
        "summary": {
            "venues_processed":  len(venue_reports),
            "total_scraped":     sum(r.get("scraped", 0) for r in venue_reports),
            "total_valid":       sum(r.get("valid", 0)   for r in venue_reports),
            "total_invalid":     sum(r.get("invalid", 0) for r in venue_reports),
            "total_after_dedup": len(deduped),
            "total_errors":      sum(len(r.get("errors", [])) for r in venue_reports),
            "avg_credibility":   master["quality"]["avg_credibility"],
        },
        "venue_quality": venue_quality,
        "venues":        venue_reports,
    }

    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    ts = start.strftime("%Y%m%d-%H%M%S")
    for p in [LOGS_DIR / f"run-{ts}.json", LOGS_DIR / "latest.json"]:
        with open(p, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

    # Guardar manifesto actualizado
    _save_manifest(current_manifest)

    logger.info(f"Concluído em {duration:.1f}s — {len(deduped)} eventos únicos")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Primeira Plateia — Agregador")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Forçar re-agregação mesmo sem alterações",
    )
    args = parser.parse_args()
    run(force=args.force)
