"""
Culturgest Scraper — Versão 3 (2026) — HTML-only (API morreu)
Venue: Culturgest | culturgest.pt
"""

import re
import time
import json as _json
import logging
import warnings
from datetime import datetime, timezone
from typing import Optional, List, Dict

import requests
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", message="Unverified HTTPS request")

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
VENUE_ID = "culturgest"
WEBSITE = "https://www.culturgest.pt"

MAX_PAGES = 15              # número máximo de páginas/filtros a tentar
REQUEST_DELAY = 1.3
TIMEOUT = 25

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-PT,pt;q=0.9,en;q=0.8",
}

MONTH_PT = {
    "jan": "01", "fev": "02", "mar": "03", "abr": "04", "mai": "05", "jun": "06",
    "jul": "07", "ago": "08", "set": "09", "out": "10", "nov": "11", "dez": "12",
    "janeiro": "01", "fevereiro": "02", "março": "03", "marco": "03", "abril": "04",
    "maio": "05", "junho": "06", "julho": "07", "agosto": "08", "setembro": "09",
    "outubro": "10", "novembro": "11", "dezembro": "12",
}

# Filtros úteis (typology)
TYPOLOGIES = [None, 1, 2, 3, 4, 5, 6, 8]   # None = todos, 1=Teatro, 2=Dança, 8=Música, etc.

# ---------------------------------------------------------------------------
# SESSION
# ---------------------------------------------------------------------------
def _make_session() -> requests.Session:
    s = requests.Session()
    s.verify = False
    s.headers.update(HEADERS)
    return s

def _get(session: requests.Session, url: str, params: dict = None) -> Optional[requests.Response]:
    try:
        resp = session.get(url, timeout=TIMEOUT, params=params)
        resp.raise_for_status()
        return resp
    except Exception as e:
        logger.warning(f"CULTURGEST: erro ao aceder {url} → {e}")
        return None

# ---------------------------------------------------------------------------
# PARSE DE DATA
# ---------------------------------------------------------------------------
def _parse_date(text: str) -> Optional[str]:
    if not text:
        return None
    t = re.sub(r"\s+", " ", text.strip()).lower()

    # "23–25 ABR 2026" ou "23 ABR 2026"
    m = re.match(r"(\d{1,2})(?:[–\-](\d{1,2}))?\s+([a-záéíóú]+)(?:\s+(\d{4}))?", t)
    if m:
        day = m.group(1).zfill(2)
        month = MONTH_PT.get(m.group(3)[:3])
        year = m.group(4) or str(datetime.now().year)
        if month:
            return f"{year}-{month}-{day}"

    # "MAR 2026"
    m = re.match(r"([a-záéíóú]+)\s+(\d{4})", t)
    if m:
        month = MONTH_PT.get(m.group(1)[:3])
        if month:
            return f"{m.group(2)}-{month}-01"

    return None

# ---------------------------------------------------------------------------
# EXTRAI LINKS DE EVENTOS DA PÁGINA DE LISTAGEM
# ---------------------------------------------------------------------------
def _extract_event_links(soup: BeautifulSoup) -> List[str]:
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or "/programacao/" not in href:
            continue
        full_url = href if href.startswith("http") else f"{WEBSITE}{href}"

        # Aceita URLs de eventos individuais (ex: /pt/programacao/teatro/nome-do-evento/)
        if re.search(r"/pt/programacao/[^/]+/[^/?#]+/?$", full_url):
            if full_url not in links and "/por-evento/" not in full_url:
                links.add(full_url)
    return list(links)

# ---------------------------------------------------------------------------
# PARSE DE PÁGINA INDIVIDUAL DE EVENTO
# ---------------------------------------------------------------------------
def _parse_single_event(url: str, session: requests.Session) -> Optional[dict]:
    resp = _get(session, url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "lxml")
    ft = soup.get_text(" ", strip=True).lower()

    # Título
    title = ""
    for sel in ["h1", ".event-title", ".title", "main h1", ".page-header h1"]:
        el = soup.select_one(sel)
        if el:
            title = el.get_text(strip=True)
            break
    if not title:
        meta = soup.find("meta", property="og:title")
        title = meta["content"].strip() if meta else ""

    if len(title) < 3:
        return None

    # Data
    date_str = ""
    for sel in ["time", ".date", ".event-date", ".when", "[class*='data']", ".schedule"]:
        el = soup.select_one(sel)
        if el:
            date_str = el.get("datetime") or el.get_text(strip=True)
            if date_str:
                break
    date_open = _parse_date(date_str)

    # Descrição longa
    desc = ""
    for sel in [".description", ".event-description", ".lead", ".text", "article", ".content"]:
        el = soup.select_one(sel)
        if el and len(el.get_text(strip=True)) > 60:
            desc = el.get_text(separator="\n", strip=True)
            break
    if not desc:
        meta = soup.find("meta", property="og:description")
        desc = meta["content"].strip() if meta else ""

    # Imagem
    cover = None
    meta_img = soup.find("meta", property="og:image")
    if meta_img:
        cover = meta_img["content"]
    if not cover:
        img = soup.select_one("img[src*='/media/'], img[src*='filer_public']")
        if img and img.get("src"):
            cover = img["src"]
            if not cover.startswith("http"):
                cover = f"{WEBSITE}{cover}"

    # Preço e bilheteira
    price_raw = ""
    ticketing_url = None
    for a in soup.find_all("a", href=True):
        txt = a.get_text(strip=True).lower()
        if any(word
