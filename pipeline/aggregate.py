"""
Primeira Plateia — Agregador
Lê data/events/*.json de todos os venues, faz dedup global,
gera data/master.json + data/logs/latest.json.
"""

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT       = Path(__file__).parent.parent
DATA_DIR   = ROOT / "data"
EVENTS_DIR = DATA_DIR / "events"
LOGS_DIR   = DATA_DIR / "logs"

import sys as _sys
import os as _os
_sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))

from pipeline.core.dedup import deduplicate

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("pipeline.aggregate")


def load_all() -> tuple[list[dict], list[dict]]:
    all_events, venue_reports = [], []
    for path in sorted(EVENTS_DIR.glob("*.json")):
        if path.name == ".gitkeep":
            continue
        try:
            with open(path, encoding="utf-8") as f:
                events = json.load(f)
            venue_id = path.stem
            all_events.extend(events)
            logger.info(f"{venue_id}: {len(events)} eventos carregados")

            # Ler relatório parcial do run_venue
            log_path = LOGS_DIR / f"venue-{venue_id}.json"
            if log_path.exists():
                with open(log_path, encoding="utf-8") as f:
                    venue_reports.append(json.load(f))
            else:
                venue_reports.append({
                    "venue_id": venue_id, "venue_name": venue_id,
                    "scraped": len(events), "valid": len(events),
                    "invalid": 0, "errors": [],
                })
        except Exception as e:
            logger.error(f"Erro ao carregar {path}: {e}")
    return all_events, venue_reports


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
                today_ids.append(eid); break
        if (e.get("price") or {}).get("is_free"):
            free_ids.append(eid)
        if (e.get("audience") or {}).get("is_family"):
            family_ids.append(eid)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat() + "Z",
        "total_events": len(events),
        "total_venues": len(by_venue),
        "events": events,
        "index": {
            "by_domain": by_domain, "by_venue": by_venue,
            "by_month": by_month, "today": today_ids,
            "free": free_ids, "family": family_ids,
        }
    }


def run():
    start = datetime.now(timezone.utc)
    logger.info("=" * 60)
    logger.info("Primeira Plateia — Agregador iniciado")
    logger.info("=" * 60)

    all_events, venue_reports = load_all()
    logger.info(f"Total: {len(all_events)} eventos de {len(venue_reports)} venues")

    deduped = deduplicate(all_events)
    logger.info(f"Após dedup: {len(deduped)} eventos")

    master = generate_master(deduped)
    with open(DATA_DIR / "master.json", "w", encoding="utf-8") as f:
        json.dump(master, f, ensure_ascii=False, indent=2)
    logger.info("master.json gerado")

    duration = (datetime.now(timezone.utc) - start).total_seconds()
    report = {
        "run_at": datetime.now(timezone.utc).isoformat() + "Z",
        "duration_seconds": round(duration, 1),
        "summary": {
            "venues_processed":  len(venue_reports),
            "total_scraped":     sum(r.get("scraped", 0) for r in venue_reports),
            "total_valid":       sum(r.get("valid", 0)   for r in venue_reports),
            "total_invalid":     sum(r.get("invalid", 0) for r in venue_reports),
            "total_after_dedup": len(deduped),
            "total_errors":      sum(len(r.get("errors", [])) for r in venue_reports),
        },
        "venues": venue_reports,
    }

    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    ts = start.strftime("%Y%m%d-%H%M%S")
    for p in [LOGS_DIR / f"run-{ts}.json", LOGS_DIR / "latest.json"]:
        with open(p, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

    logger.info(f"Concluído em {duration:.1f}s — {len(deduped)} eventos únicos")


if __name__ == "__main__":
    run()
