"""
Culturgest Scraper — Versão Completa (v2)
Venue: Culturgest | culturgest.pt
Estratégia: API JSON + Fallback HTML completo
"""

import re
import time
import json as _json
import logging
import warnings
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

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

# Endpoints
EVENT_LIST_API = f"{WEBSITE}/pt/programacao/schedule/events/"
FILTER_URL     = f"{WEBSITE}/pt/programacao/filtrar/"   # não usado diretamente, mas útil para futuro

MAX_PAGES = 40
REQUEST_DELAY = 1.1
TIMEOUT = 25

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Accept": "application/json, text/html, */*;q=0.8",
    "Accept-Language": "pt-PT,pt;q=0.9,en;q=0.8",
    "Referer": f"{WEBSITE}/pt/programacao/por-evento/",
    "X-Requested-With": "XMLHttpRequest",
}

MONTH_PT = {
    "jan": "01", "fev": "02", "mar": "03", "abr": "04", "mai": "05", "jun": "06",
    "jul": "07", "ago": "08", "set": "09", "out": "10", "nov": "11", "dez": "12",
    "janeiro": "01", "fevereiro": "02", "março": "03", "marco": "03", "abril": "04",
    "maio": "05", "junho": "06", "julho": "07", "agosto": "08", "setembro": "09",
    "outubro": "10", "novembro": "11", "dezembro": "12",
}

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
        logger.warning(f"CULTURGEST: erro {url} → {e}")
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
# MÉTODO 1 — API JSON (mais rápido e rico)
# ---------------------------------------------------------------------------
def _scrape_api(session: requests.Session, typology: Optional[int] = None, public: Optional[int] = None) -> List[dict]:
    logger.info(f"CULTURGEST: scraping via API (typology={typology}, public={public})")

    events = []
    page = 1
    params = {}
    if typology:
        params["typology"] = typology
    if public:
        params["public"] = public

    while page <= MAX_PAGES:
        time.sleep(REQUEST_DELAY)
        resp = _get(session, EVENT_LIST_API, {**params, "page": page})
        if not resp:
            break

        try:
            data = resp.json()
        except Exception:
            logger.warning(f"Página {page} não é JSON")
            break

        items = data.get("results") or data.get("items") or data.get("data") or []
        if not items:
            logger.info(f"Página {page} vazia → fim")
            break

        logger.info(f"API página {page} → {len(items)} eventos")

        for item in items:
            ev = _parse_event_from_api(item)
            if ev:
                events.append(ev)

        # Próxima página?
        if not data.get("next") and not data.get("has_next"):
            break
        page += 1

    return events

def _parse_event_from_api(item: Dict[str, Any]) -> Optional[dict]:
    try:
        title = item.get("title") or item.get("name") or ""
        if not title:
            return None

        url = item.get("url") or item.get("absolute_url") or item.get("link")
        if url and not url.startswith("http"):
            url = f"{WEBSITE}{url}"

        # Datas
        date_str = (item.get("date") or item.get("start_date") or 
                   item.get("when") or item.get("dates"))
        date_open = _parse_date(date_str) if isinstance(date_str, str) else None

        # Categorias / Tipologia
        typology = item.get("typology_name") or item.get("typology") or item.get("category")
        categories = [typology] if typology else []

        # Imagem
        image = item.get("image") or item.get("cover_image") or item.get("photo")
        if image and isinstance(image, str) and not image.startswith("http"):
            image = f"{WEBSITE}{image}"

        desc = (item.get("description") or item.get("lead") or 
                item.get("short_description") or item.get("text"))

        price = item.get("price") or item.get("ticket_price") or item.get("preco")

        ticketing = (item.get("ticket_url") or item.get("buy_url") or 
                    item.get("bilheteira"))

        return {
            "source_id": str(item.get("id") or item.get("slug") or ""),
            "source_url": url,
            "title": title.strip(),
            "subtitle": item.get("subtitle"),
            "description": desc.strip() if isinstance(desc, str) else None,
            "categories": categories,
            "tags": item.get("tags") or [],
            "dates": [{"date": date_open, "time_start": None}] if date_open else [],
            "date_open": date_open,
            "date_close": None,  # pode ser melhorado se API trouxer período
            "price_raw": price,
            "ticketing_url": ticketing,
            "audience": item.get("audience") or item.get("publico"),
            "cover_image": image,
            "location": item.get("venue") or item.get("space") or "Culturgest",
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "_method": "culturgest-api-v2",
            "_raw": item,   # útil para debug
        }
    except Exception as e:
        logger.debug(f"Erro parse API item: {e}")
        return None

# ---------------------------------------------------------------------------
# MÉTODO 2 — FALLBACK HTML (caso API falhe)
# ---------------------------------------------------------------------------
def _scrape_html_fallback(session: requests.Session) -> List[dict]:
    logger.info("CULTURGEST: API falhou → fallback para scraping HTML")

    all_events = []
    seen_urls = set()

    # Raspar a página principal e com alguns filtros comuns
    base_urls = [
        f"{WEBSITE}/pt/programacao/por-evento/",
        f"{WEBSITE}/pt/programacao/por-evento/?typology=1",   # Teatro
        f"{WEBSITE}/pt/programacao/por-evento/?typology=2",   # Dança
        f"{WEBSITE}/pt/programacao/por-evento/?typology=8",   # Música
    ]

    for base_url in base_urls:
        time.sleep(REQUEST_DELAY)
        resp = _get(session, base_url)
        if not resp:
            continue

        soup = BeautifulSoup(resp.text, "lxml")

        # Encontrar todos os links de eventos
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if not href or "/programacao/" not in href:
                continue
            full_url = href if href.startswith("http") else f"{WEBSITE}{href}"

            if full_url in seen_urls or "/por-evento/" in full_url:
                continue

            # Filtro aproximado de URL de evento individual
            if re.search(r"/pt/programacao/[^/]+/[^/?#]+/?$", full_url):
                seen_urls.add(full_url)
                ev = _parse_single_event_page(full_url, session)
                if ev:
                    all_events.append(ev)

    logger.info(f"Fallback HTML: {len(all_events)} eventos recolhidos")
    return all_events

def _parse_single_event_page(url: str, session: requests.Session) -> Optional[dict]:
    resp = _get(session, url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "lxml")
    ft = soup.get_text(" ", strip=True)

    # Título
    title = ""
    for sel in ["h1", ".event-title", ".title", ".page-header h1"]:
        el = soup.select_one(sel)
        if el:
            title = el.get_text(strip=True)
            break
    if not title:
        title = soup.find("meta", property="og:title")
        title = title["content"] if title else ""

    if not title:
        return None

    # Data
    date_str = ""
    for sel in ["time", ".date", ".event-date", "[class*='data']", ".when"]:
        el = soup.select_one(sel)
        if el:
            date_str = el.get_text(strip=True) or el.get("datetime", "")
            break
    date_open = _parse_date(date_str)

    # Descrição
    desc = ""
    for sel in [".description", ".event-description", ".lead", ".text", "article"]:
        el = soup.select_one(sel)
        if el and len(el.get_text(strip=True)) > 50:
            desc = el.get_text(separator="\n", strip=True)
            break
    if not desc:
        desc = soup.find("meta", property="og:description")
        desc = desc["content"] if desc else ""

    # Imagem
    cover = soup.find("meta", property="og:image")
    cover = cover["content"] if cover else None
    if not cover:
        img = soup.select_one("img[src*='media']")
        if img:
            cover = img["src"]
            if not cover.startswith("http"):
                cover = f"{WEBSITE}{cover}"

    # Preço e bilheteira
    price = ""
    ticketing = None
    for a in soup.find_all("a", href=True):
        txt = a.get_text(strip=True).lower()
        if any(x in txt for x in ["bilhete", "comprar", "reservar", "ticket"]):
            ticketing = a["href"]
            if not ticketing.startswith("http"):
                ticketing = f"{WEBSITE}{ticketing}"
            break

    if not price and re.search(r"gratuito|entrada livre|free", ft.lower()):
        price = "Entrada livre"

    return {
        "source_id": url.rstrip("/").split("/")[-1],
        "source_url": url,
        "title": title.strip(),
        "description": desc.strip() if desc else None,
        "categories": [],
        "dates": [{"date": date_open}] if date_open else [],
        "date_open": date_open,
        "price_raw": price,
        "ticketing_url": ticketing,
        "cover_image": cover,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "_method": "culturgest-html-fallback",
    }

# ---------------------------------------------------------------------------
# ENTRY POINT — O MELHOR POSSÍVEL
# ---------------------------------------------------------------------------
def run(typology: Optional[int] = None, public: Optional[int] = None) -> List[dict]:
    session = _make_session()

    # 1. Tentar API primeiro (mais completa)
    events = _scrape_api(session, typology=typology, public=public)

    # 2. Se não vier nada, usar fallback HTML
    if len(events) < 5:   # limiar baixo para detetar falha
        logger.warning("Poucos eventos via API → ativando fallback HTML")
        html_events = _scrape_html_fallback(session)
        # Combinar evitando duplicados
        seen = {e["source_url"] for e in events}
        for ev in html_events:
            if ev["source_url"] not in seen:
                events.append(ev)

    logger.info(f"Total final: {len(events)} eventos")
    return events


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    # Exemplos de uso:
    events = run()                    # tudo
    # events = run(typology=1)        # só Teatro
    # events = run(typology=8)        # só Música

    print(f"\n=== Total recolhido: {len(events)} eventos ===\n")
    if events:
        print(_json.dumps(events[0], indent=2, ensure_ascii=False))
