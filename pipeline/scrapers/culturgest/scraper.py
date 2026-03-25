"""
Culturgest Scraper — Versão 9 (Diagnóstico Total - 25 Mar 2026)
"""

import re
import time
import logging
import warnings
from datetime import datetime, timezone
from typing import List, Optional

import requests
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", message="Unverified HTTPS request")

logger = logging.getLogger(__name__)

WEBSITE = "https://www.culturgest.pt"
REQUEST_DELAY = 1.5
TIMEOUT = 30

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
}

MONTH_PT = { ... }  # mantém o teu dicionário de meses

TYPOLOGIES = [None, 1, 2, 3, 4, 5, 6, 8]

# ---------------------------------------------------------------------------
def _make_session() -> requests.Session:
    s = requests.Session()
    s.verify = False
    s.headers.update(HEADERS)
    return s

def _get(session, url, params=None):
    try:
        r = session.get(url, params=params, timeout=TIMEOUT)
        r.raise_for_status()
        return r
    except Exception as e:
        logger.error(f"Erro ao aceder {url}: {e}")
        return None

# ---------------------------------------------------------------------------
def _extract_event_links(soup: BeautifulSoup) -> List[str]:
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if "/programacao/" in href and "/por-evento/" not in href:
            full = href if href.startswith("http") else f"{WEBSITE}{href}"
            links.add(full)
    return list(links)

# ---------------------------------------------------------------------------
def run():
    session = _make_session()
    all_events = []
    seen = set()

    logger.info("=== CULTURGEST v9 - DIAGNÓSTICO COMPLETO ===")

    for typ in TYPOLOGIES:
        params = {"typology": typ} if typ is not None else {}
        list_url = f"{WEBSITE}/pt/programacao/por-evento/"

        time.sleep(REQUEST_DELAY)
        resp = _get(session, list_url, params)
        if not resp:
            continue

        soup = BeautifulSoup(resp.text, "lxml")

        # === DEBUG INFO ===
        logger.info(f"Filtro typology={typ or 'todos'}")
        logger.info(f"   Título da página: {soup.title.string.strip() if soup.title else 'Sem título'}")
        logger.info(f"   Container de eventos encontrado: {bool(soup.select_one('.js-eventContainer'))}")
        logger.info(f"   Total de <a> tags na página: {len(soup.find_all('a'))}")
        logger.info(f"   Total de links /programacao/ encontrados: {len(_extract_event_links(soup))}")

        # Dump parcial do HTML para vermos o que realmente vem
        event_container = soup.select_one('.js-eventContainer')
        if event_container:
            logger.info(f"   Conteúdo do container: {len(event_container.get_text(strip=True))} caracteres")
        else:
            logger.info("   Container .js-eventContainer NÃO existe no HTML estático!")

        # Tenta extrair links mesmo assim
        event_links = _extract_event_links(soup)
        logger.info(f"   Links de eventos detectados: {len(event_links)}")

        for link in event_links:
            if link in seen:
                continue
            seen.add(link)
            # (aqui poderíamos chamar _parse_single_event, mas como não há links, fica comentado)

    logger.info(f"Total recolhido: {len(all_events)} eventos")
    return all_events


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    run()
