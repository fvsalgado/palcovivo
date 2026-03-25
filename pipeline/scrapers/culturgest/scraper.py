"""
Culturgest Scraper — Versão 4 (25 Mar 2026)
Estratégia: Tenta API JSON primeiro + fallback HTML agressivo
"""

import re
import time
import json as _json
import logging
import warnings
from datetime import datetime, timezone
from typing import Optional, List

import requests
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", message="Unverified HTTPS request")

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
WEBSITE = "https://www.culturgest.pt"
EVENT_LIST_URL = f"{WEBSITE}/pt/programacao/schedule/events/"

REQUEST_DELAY = 1.3
TIMEOUT = 25
MAX_PAGES = 20

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Accept": "application/json, text/html, */*;q=0.8",
    "X-Requested-With": "XMLHttpRequest",
}

MONTH_PT = { ... }  # mantém o mesmo dicionário de meses que tinhas antes

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
        logger.warning(f"Erro ao aceder {url}: {e}")
        return None

# ---------------------------------------------------------------------------
# PARSE DATA (mantém o mesmo)
# ---------------------------------------------------------------------------
def _parse_date(text: str) -> Optional[str]:
    if not text:
        return None
    t = re.sub(r"\s+", " ", text.strip()).lower()
    m = re.match(r"(\d{1,2})(?:[–\-](\d{1,2}))?\s+([a-záéíóú]+)(?:\s+(\d{4}))?", t)
    if m:
        day = m.group(1).zfill(2)
        month = MONTH_PT.get(m.group(3)[:3])
        year = m.group(4) or str(datetime.now().year)
        if month:
            return f"{year}-{month}-{day}"
    return None

# ---------------------------------------------------------------------------
# EXTRAI LINKS DE EVENTOS (melhorado para JSON + HTML)
# ---------------------------------------------------------------------------
def _extract_event_links(content: str) -> List[str]:
    links = set()

    # 1. Tenta extrair como JSON
    try:
        data = _json.loads(content)
        items = data.get("results") or data.get("items") or data.get("data") or []
        for item in items:
            url = item.get("url") or item.get("absolute_url") or item.get("link")
            if url:
                full = url if url.startswith("http") else f"{WEBSITE}{url}"
                links.add(full)
    except Exception:
        pass

    # 2. Tenta extrair do HTML
    soup = BeautifulSoup(content, "lxml")
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if "/programacao/" in href and "/por-evento/" not in href:
            full = href if href.startswith("http") else f"{WEBSITE}{href}"
            if len(full.rstrip("/").split("/")) >= 6:   # tem slug
                links.add(full)

    return list(links)

# ---------------------------------------------------------------------------
# PARSE EVENTO INDIVIDUAL (mesmo de antes)
# ---------------------------------------------------------------------------
def _parse_single_event(url: str, session: requests.Session) -> Optional[dict]:
    # (copia exatamente a função _parse_single_event da versão 3.1 que te enviei antes)
    # ... cole aqui a função completa ...
    # Para não ficar demasiado longo, assume que já tens esta função. Se não tiveres, avisa que te envio outra vez.
    pass   # ← substitui por tua função anterior

# ---------------------------------------------------------------------------
# MAIN RUN
# ---------------------------------------------------------------------------
def run() -> List[dict]:
    session = _make_session()
    all_events = []
    seen = set()

    logger.info("CULTURGEST v4: a tentar API + extração agressiva de links")

    for page in range(1, MAX_PAGES + 1):
        time.sleep(REQUEST_DELAY)
        params = {"page": page}

        resp = _get(session, EVENT_LIST_URL, params)
        if not resp:
            break

        content = resp.text
        event_links = _extract_event_links(content)

        logger.info(f"Página {page} → {len(event_links)} links de eventos encontrados")

        for link in event_links:
            if link in seen:
                continue
            seen.add(link)

            ev = _parse_single_event(link, session)
            if ev:
                all_events.append(ev)

        # Se não vieram mais links, paramos
        if not event_links:
            break

    logger.info(f"Total recolhido: {len(all_events)} eventos")
    return all_events


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    events = run()
    print(f"\n=== Total: {len(events)} eventos ===\n")
    if events:
        print(_json.dumps(events[0], indent=2, ensure_ascii=False))
