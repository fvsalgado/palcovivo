"""
Culturgest Scraper — Versão 7 (testada - 25 Mar 2026)
Extração agressiva + logs detalhados
"""

import re
import time
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
        logger.warning(f"Erro ao aceder {url}: {e}")
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
# EXTRAÇÃO DE LINKS - MUITO AGRESSIVA
# ---------------------------------------------------------------------------
def _extract_event_links(soup: BeautifulSoup) -> List[str]:
    links = set()

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or not href.startswith("/pt/programacao/"):
            continue

        full = f"{WEBSITE}{href}" if not href.startswith("http") else href

        # Exclui páginas de listagem
        if any(x in full for x in ["/por-evento/", "/agenda-pdf/", "/archive/", "/filtrar/"]):
            continue

        # Aceita links que parecem eventos individuais
        parts = full.rstrip("/").split("/")
        if len(parts) >= 6 and len(parts[-1]) > 4 and not parts[-1].isdigit():
            links.add(full)

    # Busca adicional dentro de cards de eventos
    for container in soup.find_all(["div", "article", "li"], class_=re.compile(r"event|card|item|listing|program", re.I)):
        for a in container.find_all("a", href=True):
            href = a["href"].strip()
            if href.startswith("/pt/programacao/") and "/por-evento/" not in href:
                full = f"{WEBSITE}{href}" if not href.startswith("http") else href
                links.add(full)

    return list(links)

# ---------------------------------------------------------------------------
def _parse_single_event(url: str, session: requests.Session) -> Optional[dict]:
    resp = _get(session, url)
    if not resp:
        logger.debug(f"Não acedido: {url}")
        return None

    soup = BeautifulSoup(resp.text, "lxml")

    # Título
    title = ""
    h1 = soup.find("h1")
    if h1:
        title = h1.get_text(strip=True)
    if not title:
        meta = soup.find("meta", property="og:title")
        title = meta.get("content", "").strip() if meta else ""

    if len(title) < 4:
        return None

    # Data
    date_str = ""
    for sel in ["time", ".date", ".when", ".event-date", "[class*=data]", ".schedule"]:
        el = soup.select_one(sel)
        if el:
            date_str = el.get("datetime") or el.get_text(strip=True)
            if date_str:
                break
    date_open = _parse_date(date_str)

    # Descrição
    desc = ""
    for sel in [".description", ".lead", "article", ".content", ".text", ".body"]:
        el = soup.select_one(sel)
        if el and len(el.get_text(strip=True)) > 100:
            desc = el.get_text(separator="\n", strip=True)
            break

    # Imagem e bilheteira
    cover = None
    meta_img = soup.find("meta", property="og:image")
    if meta_img:
        cover = meta_img.get("content")

    ticketing = None
    for a in soup.find_all("a", href=True):
        txt = a.get_text(strip=True).lower()
        if any(w in txt for w in ["bilhete", "comprar", "reservar", "ticket"]):
            ticketing = a["href"]
            if not ticketing.startswith("http"):
                ticketing = f"{WEBSITE}{ticketing}"
            break

    logger.info(f"✓ Extraído com sucesso: {title[:100]}")

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
        "_method": "culturgest-html-v7",
    }

# ---------------------------------------------------------------------------
def run() -> List[dict]:
    session = _make_session()
    all_events = []
    seen = set()

    logger.info("CULTURGEST v7: extração agressiva de links (test version)")

    for typ in TYPOLOGIES:
        params = {"typology": typ} if typ is not None else {}
        list_url = f"{WEBSITE}/pt/programacao/por-evento/"

        time.sleep(REQUEST_DELAY)
        resp = _get(session, list_url, params)
        if not resp:
            continue

        soup = BeautifulSoup(resp.text, "lxml")
        event_links = _extract_event_links(soup)

        logger.info(f"Filtro typology={typ or 'todos'} → {len(event_links)} links encontrados")

        for i, link in enumerate(event_links):
            if link in seen:
                continue
            seen.add(link)

            logger.debug(f"Processando link {i+1}/{len(event_links)}: {link}")
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
