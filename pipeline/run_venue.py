"""
Primeira Plateia — Scrape de venue único
Invocado pelo job paralelo: python -m pipeline.run_venue {venue_id} [--force]

Corre o scraper, harmoniza, valida e guarda:
  data/events/{venue_id}.json
  data/logs/venue-{venue_id}.json  (relatório parcial para o aggregate)
"""

import argparse
import importlib
import json
import logging
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

ROOT       = Path(__file__).parent.parent
DATA_DIR   = ROOT / "data"
VENUES_DIR = DATA_DIR / "venues"
EVENTS_DIR = DATA_DIR / "events"
LOGS_DIR   = DATA_DIR / "logs"

import sys as _sys
import os as _os
_sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))

from pipeline.core.harmonizer import harmonize_event
from pipeline.core.validator  import validate_batch
from pipeline.core.cache      import save_cache, is_stale, get_cached_events

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
                        "is_active": True, "last_scraped": None},
        "meta": {"created_at": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                 "created_by": "pipeline-auto", "verified": False},
    }
    with open(venue_path, "w", encoding="utf-8") as f:
        json.dump(minimal, f, ensure_ascii=False, indent=2)


def run_venue(venue_id: str, force: bool = False) -> dict:
    report = {"venue_id": venue_id, "scraped": 0, "valid": 0, "invalid": 0, "errors": []}

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

    # Cache ou scrape
    if not force and not is_stale(venue_id):
        logger.info(f"{venue_id}: a usar cache")
        raw = get_cached_events(venue_id)
    else:
        try:
            raw = mod.run()
            logger.info(f"{venue_id}: {len(raw)} eventos raw recolhidos")
            save_cache(venue_id, raw)
            venue.setdefault("data_source", {})["last_scraped"] = datetime.now(timezone.utc).isoformat() + "Z"
            with open(venue_path, "w", encoding="utf-8") as f:
                json.dump(venue, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"{venue_id}: erro no scraper — {e}")
            logger.debug(traceback.format_exc())
            raw = get_cached_events(venue_id)
            if raw:
                logger.warning(f"{venue_id}: a usar cache expirada ({len(raw)} eventos)")
            report["errors"].append(str(e))

    report["scraped"] = len(raw)

    # Harmonizar
    harmonized = []
    for r in raw:
        try:
            harmonized.append(harmonize_event(r, venue_id, venue_id))
        except Exception as e:
            report["errors"].append(f"harmonize:{r.get('title','?')}:{e}")

    # Validar
    valid, invalid = validate_batch(harmonized)
    report["valid"]   = len(valid)
    report["invalid"] = len(invalid)

    # Guardar
    EVENTS_DIR.mkdir(parents=True, exist_ok=True)
    with open(EVENTS_DIR / f"{venue_id}.json", "w", encoding="utf-8") as f:
        json.dump(valid, f, ensure_ascii=False, indent=2)
    logger.info(f"{venue_id}: {len(valid)} eventos válidos guardados")

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

    if report["errors"] and report["valid"] == 0:
        sys.exit(1)
