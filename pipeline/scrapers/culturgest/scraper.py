"""
Culturgest Scraper — Versão 10 (FINAL - 25 Mar 2026)
Funciona com o endpoint AJAX real do site
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
EVENT_LIST_URL = f"{WEBSITE}/pt/programacao/schedule/events/"

REQUEST_DELAY = 1.2
TIMEOUT = 25
MAX_PAGES = 30

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "X-Requested-With": "XMLHttpRequest",          # ← essencial
    "Referer": f"{WEBSITE}/pt/programacao/por-evento/",
}

MONTH_PT = {
    "jan": "01", "fev": "02", "mar": "03", "abr": "04", "mai": "05", "jun": "06",
    "jul": "07", "ago": "08", "set": "09", "out": "10", "nov": "11", "dez": "12",
    "janeiro": "01", "fevereiro": "02", "março": "03", "marco": "03", "abril": "04",
    "maio": "05", "junho": "06", "julho": "07", "agosto": "08", "setembro": "09",
    "outubro": "10", "novembro": "11", "dezembro": "12",
}

# ---------------------------------------------------------------------------
def _make_session() -> requests.Session:
    s = requests.Session()
    s.verify = False
    s.headers.update(HEADERS)
    return s

def _get(session: requests.Session, url: str, params: dict = None):
    try:
        r = session.get(url, params=params, timeout=TIMEOUT)
        r.raise_for_status()
        return r
    except Exception as e:
        logger.warning(f"Erro {url}: {e}")
        return None

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
def _extract_event_links(html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    links = set()

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if "/programacao/" in href and "/por-evento/" not in href:
            full = href if href.startswith("http") else f"{WEBSITE}{href}"
            # só links que parecem eventos individuais
            if len(full.rstrip("/").split("/")) >= 6:
                links.add(full)

    return list(links)

# ---------------------------------------------------------------------------
def _parse_single_event(url: str, session: requests.Session) -> Optional[dict]:
    resp = _get(session, url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "lxml")

    # Título
    title = soup.find("h1")
    title = title.get_text(strip=True) if title else ""
    if not title:
        meta = soup.find("meta", property="og:title")
        title = meta["content"].strip() if meta else ""

    if len(title) < 4:
        return None

    # Data
    date_str = ""
    for sel in ["time", ".date", ".when", ".event-date", "[class*=data]", ".schedule"]:
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

    # Imagem + bilheteira
    cover = soup.find("meta", property="og:image")
    cover = cover["content"] if cover else None

    ticketing = None
    for a in soup.find_all("a", href=True):
        txt = a.get_text(strip=True).lower()
        if any(w in txt for w in ["bilhete", "comprar", "reservar", "ticket"]):
            ticketing = a["href"]
            if not ticketing.startswith("http"):
                ticketing = f"{WEBSITE}{ticketing}"
            break

    logger.info(f"✓ Extraído: {title[:100]}")

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
        "_method": "culturgest-ajax-v10",
    }

# ---------------------------------------------------------------------------
def run() -> List[dict]:
    session = _make_session()
    all_events = []
    seen = set()

    logger.info("CULTURGEST v10 — AJAX endpoint ativado")

    page = 1
    while page <= MAX_PAGES:
        time.sleep(REQUEST_DELAY)
        resp = _get(session, EVENT_LIST_URL, {"page": page})
        if not resp:
            break

        links = _extract_event_links(resp.text)
        logger.info(f"Página {page} → {len(links)} links de eventos")

        for link in links:
            if link in seen:
                continue
            seen.add(link)

            ev = _parse_single_event(link, session)
            if ev:
                all_events.append(ev)

        if len(links) == 0:
            break
        page += 1

    logger.info(f"Total recolhido: {len(all_events)} eventos")
    return all_events


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    events = run()

    print(f"\n=== TOTAL FINAL: {len(events)} eventos ===\n")
    if events:
        print(_json.dumps(events[0], indent=2, ensure_ascii=False))
