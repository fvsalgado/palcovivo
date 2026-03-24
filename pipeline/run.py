"""
Primeira Plateia — Orquestrador do Pipeline
Corre diariamente via GitHub Actions.

Fluxo:
  Para cada venue activo:
    1. Verificar cache
    2. Scraper → eventos raw
    3. Guardar cache
    4. Harmonizar
    5. Validar
  Global:
    6. Deduplicar
    7. Ordenar
    8. Gerar output JSON para o site
    9. Gerar relatório de execução
"""

import json
import logging
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Optional

# Paths
ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "data"
VENUES_DIR = DATA_DIR / "venues"
EVENTS_DIR = DATA_DIR / "events"
LOGS_DIR = DATA_DIR / "logs"

# Pipeline modules
from pipeline.core.harmonizer import harmonize_event
from pipeline.core.validator import validate_batch
from pipeline.core.dedup import deduplicate
from pipeline.core.cache import load_cache, save_cache, is_stale, get_cached_events

# Scrapers (importação dinâmica por venue)
SCRAPER_REGISTRY = {
    "ccb": "pipeline.scrapers.ccb.scraper",
}

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
# LOAD VENUES
# ---------------------------------------------------------------------------

def load_active_venues() -> list[dict]:
    """Carrega todos os venues activos do diretório data/venues/."""
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

def run_scraper(venue: dict, force_refresh: bool = False) -> list[dict]:
    """
    Corre o scraper de um venue.
    Se cache válida e não forçado, usa cache.
    """
    venue_id = venue["id"]
    scraper_module_path = SCRAPER_REGISTRY.get(venue_id)

    if not scraper_module_path:
        logger.warning(f"Sem scraper registado para venue '{venue_id}'")
        return []

    # Verificar cache
    if not force_refresh and not is_stale(venue_id):
        logger.info(f"{venue_id}: a usar cache")
        return get_cached_events(venue_id)

    # Importar scraper dinamicamente
    try:
        import importlib
        scraper_mod = importlib.import_module(scraper_module_path)
    except ImportError as e:
        logger.error(f"{venue_id}: erro ao importar scraper — {e}")
        return []

    # Correr scraper
    try:
        raw_events = scraper_mod.run()
        logger.info(f"{venue_id}: {len(raw_events)} eventos raw recolhidos")

        # Guardar cache
        save_cache(venue_id, raw_events)

        # Actualizar last_scraped no venue file
        venue_path = VENUES_DIR / f"{venue_id}.json"
        if venue_path.exists():
            with open(venue_path, "r", encoding="utf-8") as f:
                venue_data = json.load(f)
            venue_data["data_source"]["last_scraped"] = datetime.utcnow().isoformat() + "Z"
            with open(venue_path, "w", encoding="utf-8") as f:
                json.dump(venue_data, f, ensure_ascii=False, indent=2)

        return raw_events

    except Exception as e:
        logger.error(f"{venue_id}: erro no scraper — {e}")
        logger.debug(traceback.format_exc())
        # Tentar usar cache mesmo que expirada como fallback
        cached = get_cached_events(venue_id)
        if cached:
            logger.warning(f"{venue_id}: a usar cache expirada como fallback ({len(cached)} eventos)")
        return cached


# ---------------------------------------------------------------------------
# PROCESS VENUE
# ---------------------------------------------------------------------------

def process_venue(venue: dict, force_refresh: bool = False) -> dict:
    """
    Pipeline completo para um venue.
    Retorna relatório {venue_id, scraped, harmonized, valid, invalid, errors}
    """
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
        raw_events = run_scraper(venue, force_refresh)
        report["scraped"] = len(raw_events)

        # 2. Harmonizar
        harmonized = []
        for raw in raw_events:
            try:
                event = harmonize_event(raw, venue_id, venue_id)
                harmonized.append(event)
            except Exception as e:
                logger.error(f"{venue_id}: erro ao harmonizar evento '{raw.get('title', '?')}' — {e}")
                report["errors"].append(f"harmonize: {raw.get('title', '?')}: {e}")
        report["harmonized"] = len(harmonized)

        # 3. Validar
        valid, invalid = validate_batch(harmonized)
        report["valid"] = len(valid)
        report["invalid"] = len(invalid)

        # 4. Guardar eventos do venue
        EVENTS_DIR.mkdir(parents=True, exist_ok=True)
        out_path = EVENTS_DIR / f"{venue_id}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(valid, f, ensure_ascii=False, indent=2)
        logger.info(f"{venue_id}: {len(valid)} eventos válidos guardados em {out_path}")

        return {**report, "events": valid}

    except Exception as e:
        logger.error(f"{venue_id}: erro inesperado — {e}")
        report["errors"].append(str(e))
        return {**report, "events": []}


# ---------------------------------------------------------------------------
# GENERATE MASTER OUTPUT
# ---------------------------------------------------------------------------

def generate_master(all_events: list[dict]) -> dict:
    """
    Gera o ficheiro master.json com todos os eventos para o site.
    Inclui índices por domínio, venue, mês, etc.
    """
    # Ordenar por data_first
    all_events.sort(key=lambda e: (e.get("date_first") or "9999-99-99"))

    # Índices
    by_domain: dict[str, list[str]] = {}
    by_venue: dict[str, list[str]] = {}
    by_month: dict[str, list[str]] = {}
    today_ids: list[str] = []
    free_ids: list[str] = []
    family_ids: list[str] = []

    today = datetime.now().strftime("%Y-%m-%d")
    current_month = datetime.now().strftime("%Y-%m")

    for event in all_events:
        eid = event["id"]
        domain = event.get("domain", "outros")
        venue_id = event.get("venue_id", "")
        date_first = event.get("date_first", "")

        # Por domínio
        by_domain.setdefault(domain, []).append(eid)

        # Por venue
        by_venue.setdefault(venue_id, []).append(eid)

        # Por mês
        if date_first:
            month = date_first[:7]
            by_month.setdefault(month, []).append(eid)

        # Hoje
        for d in event.get("dates", []):
            if d.get("date") == today:
                today_ids.append(eid)
                break

        # Gratuitos
        if event.get("price", {}).get("is_free"):
            free_ids.append(eid)

        # Famílias
        if event.get("audience", {}).get("is_family"):
            family_ids.append(eid)

    master = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
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

    return master


# ---------------------------------------------------------------------------
# GENERATE REPORT
# ---------------------------------------------------------------------------

def generate_report(venue_reports: list[dict], all_events: list[dict], duration_seconds: float) -> dict:
    """Gera relatório de execução do pipeline."""
    total_scraped = sum(r["scraped"] for r in venue_reports)
    total_valid = sum(r["valid"] for r in venue_reports)
    total_invalid = sum(r["invalid"] for r in venue_reports)
    total_errors = sum(len(r["errors"]) for r in venue_reports)

    return {
        "run_at": datetime.utcnow().isoformat() + "Z",
        "duration_seconds": round(duration_seconds, 1),
        "summary": {
            "venues_processed": len(venue_reports),
            "total_scraped": total_scraped,
            "total_valid": total_valid,
            "total_invalid": total_invalid,
            "total_after_dedup": len(all_events),
            "total_errors": total_errors,
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
    start_time = datetime.utcnow()
    logger.info("=" * 60)
    logger.info("Primeira Plateia — Pipeline iniciado")
    logger.info(f"Data: {start_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    logger.info("=" * 60)

    # 1. Carregar venues
    venues = load_active_venues()
    if not venues:
        logger.error("Nenhum venue activo encontrado")
        return {}

    # 2. Processar cada venue
    venue_reports = []
    all_venue_events = []

    for venue in venues:
        logger.info(f"\n--- {venue['name']} ---")
        result = process_venue(venue, force_refresh)
        venue_reports.append(result)
        all_venue_events.extend(result.get("events", []))

    logger.info(f"\nTotal após processamento: {len(all_venue_events)} eventos")

    # 3. Deduplicar (global, entre venues)
    all_events = deduplicate(all_venue_events)
    logger.info(f"Total após deduplicação: {len(all_events)} eventos")

    # 4. Gerar master output
    master = generate_master(all_events)
    master_path = DATA_DIR / "master.json"
    with open(master_path, "w", encoding="utf-8") as f:
        json.dump(master, f, ensure_ascii=False, indent=2)
    logger.info(f"Master JSON gerado: {master_path}")

    # 5. Relatório
    duration = (datetime.utcnow() - start_time).total_seconds()
    report = generate_report(venue_reports, all_events, duration)

    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_filename = f"run-{start_time.strftime('%Y%m%d-%H%M%S')}.json"
    log_path = LOGS_DIR / log_filename
    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    # Guardar também como latest para referência rápida
    latest_path = LOGS_DIR / "latest.json"
    with open(latest_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

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
    # Exit code 1 se houver erros críticos
    if report.get("summary", {}).get("venues_processed", 0) == 0:
        sys.exit(1)
