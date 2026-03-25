"""
Culturgest Scraper — Versão 5 (25 Mar 2026) — HTML-only agressivo
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

WEBSITE = "https://www.culturgest.pt"
REQUEST_DELAY = 1.4
TIMEOUT = 25

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

MONTH_PT = {
    "jan": "01", "fev": "02", "mar": "03", "abr": "04", "mai": "05", "jun": "06",
    "jul": "07", "ago": "08", "set": "09", "out": "10", "nov": "11", "dez": "12",
    "janeiro": "01", "fevereiro": "02", "março": "03", "marco": "03", "abril": "04",
    "maio": "05", "junho": "06", "julho": "07", "agosto": "08", "setembro": "09",
    "outubro": "10", "novembro": "11", "dezembro": "12",
}

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
        logger.warning(f"Erro {url}: {e}")
        return None

# ---------------------------------------------------------------------------
def _parse_date(text: str) -> Optional[str]:
    if not text: return None
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
def _extract_event_links(soup: BeautifulSoup) -> List[str]:
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or not href.startswith("/pt/programacao/"):
            continue
        full = f"{WEBSITE}{href}" if not href.startswith("http") else href

        # Aceita qualquer link de evento individual (mais largo)
        if ("/por-evento/" not in full and 
            "/agenda-pdf/" not in full and 
            "/archive/" not in full and 
            len(full.rstrip("/").split("/")) >= 5):
            links.add(full)
    return list(links)

# ---------------------------------------------------------------------------
def _parse_single_event(url: str, session: requests.Session) -> Optional[dict]:
    resp = _get(session, url)
    if not resp:
        logger.debug(f"404 ou erro ao aceder evento: {url}")
        return None

    soup = BeautifulSoup(resp.text, "lxml")
    title = soup.find("h1")
    title = title.get_text(strip=True) if title else ""

    if len(title) < 3:
        meta_title = soup.find("meta", property="og:title")
        title = meta_title["content"].strip() if meta_title else ""
        if len(title) < 3:
            return None

    # Data
    date_str = ""
    for sel in ["time", ".date", ".when", "[class*=data]", ".schedule"]:
        el = soup.select_one(sel)
        if el:
            date_str = el.get("datetime") or el.get_text(strip=True)
            break
    date_open = _parse_date(date_str)

    # Descrição
    desc = ""
    for sel in [".description", ".lead", "article", ".content", ".text"]:
        el = soup.select_one(sel)
        if el and len(el.get_text(strip=True)) > 80:
            desc = el.get_text(separator="\n", strip=True)
            break

    # Imagem + bilheteira (simplificado)
    cover = soup.find("meta", property="og:image")
    cover = cover["content"] if cover else None

    ticketing = None
    for a in soup.find_all("a", href=True):
        if any(w in a.get_text(strip=True).lower() for w in ["bilhete", "comprar", "reservar", "ticket"]):
            ticketing = a["href"]
            if not ticketing.startswith("http"):
                ticketing = f"{WEBSITE}{ticketing}"
            break

    logger.info(f"✓ Evento extraído: {title[:80]}...")

    return {
        "source_id": url.rstrip("/").split("/")[-1],
        "source_url": url,
        "title": title,
        "description": desc or None,
        "dates": [{"date": date_open}] if date_open else [],
        "date_open": date_open,
        "ticketing_url": ticketing,
        "cover_image": cover,
        "location": "Culturgest",
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "_method": "culturgest-html-v5",
    }

# ---------------------------------------------------------------------------
def run() -> List[dict]:
    session = _make_session()
    all_events = []
    seen = set()

    logger.info("CULTURGEST v5: scraping HTML principal + links individuais")

    for typ in TYPOLOGIES:
        params = {"typology": typ} if typ is not None else {}
        url = f"{WEBSITE}/pt/programacao/por-evento/"

        time.sleep(REQUEST_DELAY)
        resp = _get(session, url, params)
        if not resp:
            continue

        soup = BeautifulSoup(resp.text, "lxml")
        event_links = _extract_event_links(soup)

        logger.info(f"Filtro typology={typ or 'todos'} → {len(event_links)} links encontrados")

        for link in event_links:
            if link in seen:
                continue
            seen.add(link)

            ev = _parse_single_event(link, session)
            if ev:
                all_events.append(ev)

            time.sleep(REQUEST_DELAY)

    logger.info(f"Total recolhido: {len(all_events)} eventos")
    return all_events


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    events = run()

    print(f"\n=== TOTAL FINAL: {len(events)} eventos ===\n")
    if events:
        print(_json.dumps(events[0], indent=2, ensure_ascii=False))
