"""
Primeira Plateia — Orquestrador do Pipeline
Corre diariamente via GitHub Actions.

Fluxo:
  Para cada scraper descoberto em pipeline/scrapers/:
    1. Garantir que existe data/venues/{id}.json (cria mínimo se não existir)
    2. Verificar cache
    3. Scraper → eventos raw
    4. Guardar cache
    5. Harmonizar
    6. Validar
  Global:
    7. Deduplicar
    8. Ordenar
    9. Gerar output JSON para o site
    10. Gerar relatório de execução

Para adicionar um novo venue:
  1. Criar pipeline/scrapers/{id}/__init__.py  (vazio)
  2. Criar pipeline/scrapers/{id}/scraper.py   (com função run())
  Mais nada. O pipeline detecta e integra automaticamente.
"""

import json
import logging
import sys
import traceback
import importlib
import inspect
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Paths
ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "data"
VENUES_DIR = DATA_DIR / "venues"
EVENTS_DIR = DATA_DIR / "events"
LOGS_DIR = DATA_DIR / "logs"
SCRAPERS_DIR = Path(__file__).parent / "scrapers"

# Pipeline modules
from pipeline.core.harmonizer import harmonize_event
from pipeline.core.validator import validate_batch
from pipeline.core.dedup import deduplicate
from pipeline.core.cache import load_cache, save_cache, is_stale, get_cached_events, should_tombstone

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger("pipeline")


# ---------------------------------------------------------------------------
# DESCOBERTA DE SCRAPERS
# ---------------------------------------------------------------------------

def build_scraper_registry() -> dict:
    """
    Descobre automaticamente todos os scrapers em pipeline/scrapers/.
    Qualquer pasta com scraper.py é registada — sem necessidade de editar este ficheiro.
    """
    registry = {}
    for path in sorted(SCRAPERS_DIR.iterdir()):
        if path.is_dir() and (path / "scraper.py").exists():
            venue_id = path.name
            registry[venue_id] = f"pipeline.scrapers.{venue_id}.scraper"
    if registry:
        logger.info(f"Scrapers descobertos: {list(registry.keys())}")
    else:
        logger.warning("Nenhum scraper encontrado em pipeline/scrapers/")
    return registry


# ---------------------------------------------------------------------------
# VENUE MÍNIMO AUTOMÁTICO
# ---------------------------------------------------------------------------

def ensure_venue_file(venue_id: str) -> None:
    """
    Se não existir data/venues/{id}.json, cria um ficheiro mínimo automaticamente.
    Tenta extrair API_BASE e WEBSITE do próprio scraper.
    """
    venue_path = VENUES_DIR / f"{venue_id}.json"
    if venue_path.exists():
        return

    logger.warning(f"{venue_id}: ficheiro de venue não encontrado — a criar mínimo automaticamente")
    VENUES_DIR.mkdir(parents=True, exist_ok=True)

    # Tentar extrair info básica do scraper
    api_url = ""
    website = ""
    try:
        mod = importlib.import_module(f"pipeline.scrapers.{venue_id}.scraper")
        api_url = getattr(mod, "API_BASE", "") or getattr(mod, "SOURCE_URL", "")
        website = getattr(mod, "WEBSITE", "").rstrip("/")
    except Exception:
        pass

    minimal = {
        "schema_version": "1.0",
        "id": venue_id,
        "name": venue_id.replace("-", " ").title(),
        "short_name": None,
        "legal_name": None,
        "acronym": None,
        "aliases": [],
        "venue_type": "teatro",
        "venue_subtypes": [],
        "is_permanent": True,
        "is_multi_space": False,
        "address": {
            "street": None, "number": None, "parish": None,
            "municipality": "Portugal", "district": "Portugal",
            "region": None, "country": "PT",
            "postal_code": "0000-000", "full_address": None,
            "nuts2": None, "nuts3": None,
        },
        "geo": {"lat": 0.0, "lng": 0.0, "google_place_id": None},
        "contact": {
            "website": website or None,
            "email": None, "phone": None,
            "box_office_phone": None, "box_office_email": None,
            "box_office_url": None,
        },
        "social": {
            "instagram": None, "facebook": None, "youtube": None,
            "twitter_x": None, "linkedin": None, "spotify": None,
        },
        "media": {
            "logo_svg": None, "logo_png": None, "logo_dark": None,
            "facade_photo": None, "cover_photo": None,
            "gallery": [], "photo_credits": None,
        },
        "spaces": [],
        "accessibility": {
            "wheelchair": False, "hearing_loop": False,
            "audio_description": False, "sign_language_regular": False,
            "sign_language_occasional": False, "relaxed_performances": False,
            "accessible_toilets": False, "accessible_parking": False,
            "braille_materials": False, "notes": None,
        },
        "transport": {
            "metro": [], "bus": [], "tram": [], "train": [],
            "parking": False, "bike_parking": False, "notes": None,
        },
        "amenities": {
            "cafe": False, "restaurant": False, "shop": False,
            "library": False, "cloakroom": False, "outdoor_space": False,
        },
        "ticketing": {
            "primary_system": None, "ticketing_url": website or None,
            "has_online_sales": False, "has_box_office": False,
            "accepts_lisboa_card": False, "discount_programs": [], "notes": None,
        },
        "data_source": {
            "scraper_id": venue_id,
            "api_type": "rest",
            "api_url": api_url or "",
            "api_requires_auth": False,
            "scraper_notes": "Ficheiro gerado automaticamente. Completar dados em falta.",
            "update_frequency": "daily",
            "last_scraped": None,
            "is_active": True,
        },
        "meta": {
            "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "created_by": "pipeline-auto",
            "verified": False,
            "verified_at": None,
            "notes": "Criado automaticamente pelo pipeline. Completar manualmente.",
        },
    }

    with open(venue_path, "w", encoding="utf-8") as f:
        json.dump(minimal, f, ensure_ascii=False, indent=2)
    logger.info(f"{venue_id}: ficheiro de venue mínimo criado em {venue_path}")


# ---------------------------------------------------------------------------
# LOAD VENUES
# ---------------------------------------------------------------------------

def load_active_venues(registry: dict) -> list[dict]:
    """
    Garante ficheiros de venue para todos os scrapers descobertos,
    depois carrega todos os venues activos.
    """
    for venue_id in registry:
        ensure_venue_file(venue_id)

    venues = []
    for path in sorted(VENUES_DIR.glob("*.json")):
        try:
            with open(path, "r", encoding="utf-8") as f:
                venue = json.load(f)
            if venue.get("data_source", {}).get("is_active", False):
                venues.append(venue)
                logger.info(f"Venue carregado: {venue['id']} ({venue['name']})")
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Erro ao carregar venue {path.name}: {e}")
    return venues


# ---------------------------------------------------------------------------
# RUN SCRAPER
# ---------------------------------------------------------------------------

def run_scraper(venue: dict, registry: dict, force_refresh: bool = False) -> list[dict]:
    """
    Corre o scraper de um venue.
    Se cache válida e não forçado, usa cache.

    Modo incremental: se o scraper aceitar known_ids, passa o conjunto de
    source_ids já em cache para evitar re-scraping de eventos conhecidos.
    """
    venue_id = venue["id"]
    scraper_module_path = registry.get(venue_id)

    if not scraper_module_path:
        logger.warning(f"{venue_id}: sem scraper disponível — a ignorar")
        return []

    # Verificar cache
    if not force_refresh and not is_stale(venue_id):
        logger.info(f"{venue_id}: a usar cache")
        return get_cached_events(venue_id)

    # Importar scraper
    try:
        scraper_mod = importlib.import_module(scraper_module_path)
    except ImportError as e:
        logger.error(f"{venue_id}: erro ao importar scraper — {e}")
        return []

    # Construir known_ids a partir da cache existente (modo incremental)
    # Só passado se o scraper declarar o parâmetro na assinatura de run()
    known_ids: Optional[set] = None
    try:
        sig = inspect.signature(scraper_mod.run)
        if "known_ids" in sig.parameters:
            cached = get_cached_events(venue_id)
            known_ids = {ev["source_id"] for ev in cached if ev.get("source_id")}
            if known_ids:
                logger.info(f"{venue_id}: modo incremental — {len(known_ids)} IDs já em cache")
    except Exception:
        pass  # scraper sem run() válido — falhará na chamada seguinte

    try:
        if known_ids is not None:
            raw_events = scraper_mod.run(known_ids=known_ids)
        else:
            raw_events = scraper_mod.run()

        logger.info(f"{venue_id}: {len(raw_events)} eventos raw recolhidos")

        # Em modo incremental, fundir novos eventos com os já em cache
        # para não perder os que foram ignorados pelo scraper
        if known_ids:
            cached = get_cached_events(venue_id)
            cached_by_id = {ev["source_id"]: ev for ev in cached if ev.get("source_id")}
            new_by_id = {ev["source_id"]: ev for ev in raw_events if ev.get("source_id")}
            # Novos sobrepõem os cached (actualização); cached preenche o resto
            merged = {**cached_by_id, **new_by_id}
            raw_events = list(merged.values())
            logger.info(f"{venue_id}: {len(raw_events)} eventos após fusão incremental")

        save_cache(venue_id, raw_events)

        # Actualizar last_scraped
        venue_path = VENUES_DIR / f"{venue_id}.json"
        if venue_path.exists():
            with open(venue_path, "r", encoding="utf-8") as f:
                venue_data = json.load(f)
            venue_data["data_source"]["last_scraped"] = datetime.now(timezone.utc).isoformat() + "Z"
            with open(venue_path, "w", encoding="utf-8") as f:
                json.dump(venue_data, f, ensure_ascii=False, indent=2)

        return raw_events

    except Exception as e:
        logger.error(f"{venue_id}: erro no scraper — {e}")
        logger.debug(traceback.format_exc())
        cached = get_cached_events(venue_id)
        if cached:
            logger.warning(f"{venue_id}: a usar cache expirada como fallback ({len(cached)} eventos)")
        return cached


# ---------------------------------------------------------------------------
# PROCESS VENUE
# ---------------------------------------------------------------------------

def process_venue(venue: dict, registry: dict, force_refresh: bool = False) -> dict:
    """Pipeline completo para um venue."""
    venue_id = venue["id"]
    report = {
        "venue_id": venue_id,
        "venue_name": venue["name"],
        "scraped": 0,
        "harmonized": 0,
        "valid": 0,
        "invalid": 0,
        "errors": [],
    }

    try:
        # 1. Scrape
        raw_events = run_scraper(venue, registry, force_refresh)
        report["scraped"] = len(raw_events)

        # 2. Harmonizar
        harmonized = []
        for raw in raw_events:
            try:
                event = harmonize_event(raw, venue_id, venue_id)
                harmonized.append(event)
            except Exception as e:
                logger.error(f"{venue_id}: erro ao harmonizar '{raw.get('title', '?')}' — {e}")
                report["errors"].append(f"harmonize: {raw.get('title', '?')}: {e}")
        report["harmonized"] = len(harmonized)

        # 3. Validar
        valid, invalid = validate_batch(harmonized)
        report["valid"] = len(valid)
        report["invalid"] = len(invalid)

        # 4. Aplicar tombstone imediato (eventos passados há > 90 dias)
        final_valid = []
        n_tombstoned = 0
        for ev in valid:
            if should_tombstone(ev):
                ev.setdefault("pipeline", {})["is_active"] = False
                n_tombstoned += 1
            else:
                final_valid.append(ev)
        if n_tombstoned:
            logger.info(f"{venue_id}: {n_tombstoned} evento(s) tombstoned (passados > 90 dias)")

        # 5. Guardar eventos do venue
        EVENTS_DIR.mkdir(parents=True, exist_ok=True)
        out_path = EVENTS_DIR / f"{venue_id}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(final_valid, f, ensure_ascii=False, indent=2)
        logger.info(f"{venue_id}: {len(final_valid)} eventos válidos guardados em {out_path}")

        return {**report, "events": final_valid}

    except Exception as e:
        logger.error(f"{venue_id}: erro inesperado — {e}")
        report["errors"].append(str(e))
        return {**report, "events": []}


# ---------------------------------------------------------------------------
# GENERATE MASTER OUTPUT
# ---------------------------------------------------------------------------

def generate_master(all_events: list[dict]) -> dict:
    """Gera master.json com todos os eventos e índices para o site."""
    all_events.sort(key=lambda e: (e.get("date_first") or "9999-99-99"))

    by_domain: dict[str, list[str]] = {}
    by_venue: dict[str, list[str]] = {}
    by_month: dict[str, list[str]] = {}
    today_ids: list[str] = []
    free_ids: list[str] = []
    family_ids: list[str] = []

    today = datetime.now().strftime("%Y-%m-%d")

    for event in all_events:
        eid = event["id"]
        by_domain.setdefault(event.get("domain", "outros"), []).append(eid)
        by_venue.setdefault(event.get("venue_id", ""), []).append(eid)
        date_first = event.get("date_first", "")
        if date_first:
            by_month.setdefault(date_first[:7], []).append(eid)
        for d in event.get("dates", []):
            if d.get("date") == today:
                today_ids.append(eid)
                break
        if event.get("price", {}).get("is_free"):
            free_ids.append(eid)
        if event.get("audience", {}).get("is_family"):
            family_ids.append(eid)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat() + "Z",
        "total_events": len(all_events),
        "total_venues": len(by_venue),
        "events": all_events,
        "index": {
            "by_domain": by_domain,
            "by_venue": by_venue,
            "by_month": by_month,
            "today": today_ids,
            "free": free_ids,
            "family": family_ids,
        }
    }


# ---------------------------------------------------------------------------
# GENERATE REPORT
# ---------------------------------------------------------------------------

def generate_report(venue_reports: list[dict], all_events: list[dict], duration_seconds: float) -> dict:
    """Gera relatório de execução do pipeline."""
    return {
        "run_at": datetime.now(timezone.utc).isoformat() + "Z",
        "duration_seconds": round(duration_seconds, 1),
        "summary": {
            "venues_processed": len(venue_reports),
            "total_scraped": sum(r["scraped"] for r in venue_reports),
            "total_valid": sum(r["valid"] for r in venue_reports),
            "total_invalid": sum(r["invalid"] for r in venue_reports),
            "total_after_dedup": len(all_events),
            "total_errors": sum(len(r["errors"]) for r in venue_reports),
        },
        "venues": [
            {
                "venue_id": r["venue_id"],
                "venue_name": r["venue_name"],
                "scraped": r["scraped"],
                "valid": r["valid"],
                "invalid": r["invalid"],
                "errors": r["errors"],
            }
            for r in venue_reports
        ],
    }


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def run(force_refresh: bool = False) -> dict:
    """Pipeline completo. Retorna relatório de execução."""
    start_time = datetime.now(timezone.utc)
    logger.info("=" * 60)
    logger.info("Primeira Plateia — Pipeline iniciado")
    logger.info(f"Data: {start_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    logger.info("=" * 60)

    # 1. Descobrir scrapers disponíveis
    registry = build_scraper_registry()

    # 2. Carregar venues (cria ficheiros mínimos se necessário)
    venues = load_active_venues(registry)
    if not venues:
        logger.error("Nenhum venue activo encontrado")
        return {}

    # 3. Processar cada venue
    venue_reports = []
    all_venue_events = []

    for venue in venues:
        logger.info(f"\n--- {venue['name']} ---")
        result = process_venue(venue, registry, force_refresh)
        venue_reports.append(result)
        all_venue_events.extend(result.get("events", []))

    logger.info(f"\nTotal após processamento: {len(all_venue_events)} eventos")

    # 4. Deduplicar
    all_events = deduplicate(all_venue_events)
    logger.info(f"Total após deduplicação: {len(all_events)} eventos")

    # 5. Gerar master output
    master = generate_master(all_events)
    master_path = DATA_DIR / "master.json"
    with open(master_path, "w", encoding="utf-8") as f:
        json.dump(master, f, ensure_ascii=False, indent=2)
    logger.info(f"Master JSON gerado: {master_path}")

    # 6. Relatório — escrito sempre, mesmo com erros parciais
    duration = (datetime.now(timezone.utc) - start_time).total_seconds()
    report = generate_report(venue_reports, all_events, duration)

    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_filename = f"run-{start_time.strftime('%Y%m%d-%H%M%S')}.json"
    latest_path = LOGS_DIR / "latest.json"
    for out_path in [LOGS_DIR / log_filename, latest_path]:
        try:
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
        except IOError as e:
            logger.error(f"Erro ao escrever relatório {out_path}: {e}")

    logger.info("\n" + "=" * 60)
    logger.info(f"Pipeline concluído em {duration:.1f}s")
    logger.info(f"  Venues: {report['summary']['venues_processed']}")
    logger.info(f"  Scraped: {report['summary']['total_scraped']}")
    logger.info(f"  Válidos: {report['summary']['total_valid']}")
    logger.info(f"  Após dedup: {report['summary']['total_after_dedup']}")
    logger.info(f"  Erros: {report['summary']['total_errors']}")
    logger.info("=" * 60)

    return report


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Primeira Plateia Pipeline")
    parser.add_argument("--force", action="store_true", help="Ignorar cache e re-scraping")
    args = parser.parse_args()
    report = run(force_refresh=args.force)
    if report.get("summary", {}).get("venues_processed", 0) == 0:
        sys.exit(1)
