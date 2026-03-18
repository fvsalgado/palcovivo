#!/usr/bin/env python3
"""
Palco Vivo — Orquestrador principal
Scraping → Validação → events.json + validation_report.json
"""
import json, sys, time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

# Scrapers ativos
from scrapers import (
    scraper_teatro_variedades,
    scraper_teatro_capitolio,
    saoluiz,
    mariamatos,
    ccb,
    scraper_culturgest,
)

from scrapers.validator import validate
from scrapers.utils import log


# Scrapers activos: (nome_display, função)
SCRAPERS = [
    ("Teatro Variedades",               scraper_teatro_variedades.scrape),
    ("Capitólio",                       scraper_teatro_capitolio.scrape),
    ("São Luiz Teatro Municipal",       saoluiz.scrape),
    ("Teatro Maria Matos",              mariamatos.scrape),
    ("Culturgest",                          scraper_culturgest.scrape),
    ("CCB — Centro Cultural de Belém",  ccb.scrape),
]


def run():
    t0 = time.time()
    log("=" * 55)
    log("Palco Vivo — início do scraping")
    log("=" * 55)

    raw_events = []
    scraper_stats = {}

    for name, fn in SCRAPERS:
        try:
            evs = fn()
            raw_events.extend(evs)
            scraper_stats[name] = len(evs)
            log(f"  OK  {name}: {len(evs)} eventos raw")
        except Exception as e:
            log(f"  ERRO {name}: {e}")
            scraper_stats[name] = 0

    log(f"\nTotal raw: {len(raw_events)}")

    # Deduplicar por id antes de validar
    seen_ids, deduped = set(), []
    for ev in raw_events:
        eid = ev.get("id", "")
        if eid and eid not in seen_ids:
            seen_ids.add(eid)
            deduped.append(ev)

    # Validação
    valid_events, report = validate(deduped)

    # Ordenar por data de início
    valid_events.sort(key=lambda e: e.get("date_start", "9999"))

    # Escrever events.json
    output = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "total":      len(valid_events),
        "by_theater": scraper_stats,
        "events":     valid_events,
    }
    Path("events.json").write_text(
        json.dumps(output, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    # Escrever validation_report.json
    Path("validation_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    elapsed = round(time.time() - t0, 1)
    log(f"\nevents.json: {len(valid_events)} eventos válidos em {elapsed}s")
    log("─" * 40)
    for name, count in scraper_stats.items():
        log(f"  {name:<35} {count:>3} raw")
    log(f"\n  Rejeitados pela validação: {report['total_rejected']}")
    log(f"  Com avisos:               {report['total_warnings']}")


if __name__ == "__main__":
    run()
