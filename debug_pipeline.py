"""
Script de diagnóstico — corre antes do pipeline principal.
Remove este ficheiro quando o pipeline estiver estável.
"""
import traceback
import sys

from pipeline.core.cache import save_cache
from pipeline.scrapers.ccb.scraper import run as ccb_run

try:
    print("=== DEBUG: a recolher eventos CCB ===")
    events = ccb_run()
    print("Scraped:", len(events))

    save_cache("ccb", events[:2])
    print("Cache OK")

    from pipeline.core.harmonizer import harmonize_event
    h = harmonize_event(events[0], "ccb", "ccb")
    print("Harmonize OK:", h["id"])
    print("=== DEBUG: tudo OK ===")

except Exception as e:
    print("=== DEBUG ERRO ===")
    traceback.print_exc()
    print("=== FIM ERRO ===")
    # Não falhar o job — queremos ver o erro mas continuar
    sys.exit(0)
