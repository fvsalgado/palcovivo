"""
Primeira Plateia — Scraper CCB v2
Estratégia: API REST para listing + enriquecimento HTML por evento
"""

import requests
import time
import logging
import re
from datetime import datetime, timezone
from typing import Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
VENUE_ID = "ccb"
SCRAPER_ID = "ccb"
API_BASE = "https://www.ccb.pt/wp-json/tribe/events/v1/events"
PER_PAGE = 50
MAX_PAGES = 20
REQUEST_DELAY = 2.0
TIMEOUT = 45

# Controlo de enriquecimento HTML
# True = fetch página de detalhe para TODOS os eventos (mais completo, mais lento)
# False = apenas campos da API (rápido, mas com gaps)
ENRICH_DETAIL_PAGES = True
DETAIL_DELAY = 1.0   # delay entre fetches de detalhe (mais generoso)
DETAIL_BATCH_SIZE = 50  # max eventos a enriquecer por run (None = todos)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "pt-PT,pt;q=0.9,en;q=0.8",
    "Referer": "https://www.ccb.pt/",
}

HEADERS_HTML = {
    **HEADERS,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


# ---------------------------------------------------------------------------
# SPACE MAP — expandido com todos os espaços visíveis no HTML
# ---------------------------------------------------------------------------
SPACE_MAP = {
    "Grande Auditório": "grande-auditorio",
    "Pequeno Auditório": "pequeno-auditorio",
    "MAC/CCB": "mac-ccb",
    "Museu de Arte Contemporânea": "mac-ccb",
    "MAC": "mac-ccb",
    "Garagem Sul": "garagem-sul",
    "Sala de Exposições": "sala-exposicoes",
    "Jardins": "jardins",
    "Átrio": "atrio",
    "Black Box": "black-box",
    "Lopes-Graça": "sala-lopes-graca",
    "Centro de Arquitetura": "centro-arquitetura",
    "Luís de Freitas Branco": "sala-luis-freitas-branco",
    "Palco do GA": "palco-grande-auditorio",
    "Centro de Congressos": "ccr",
    "CCR": "ccr",
    "Espaço Fábrica": "fabrica-das-artes",
    "Fábrica das Artes": "fabrica-das-artes",
}


def _resolve_space(text: str) -> Optional[str]:
    if not text:
        return None
    text_lower = text.lower()
    for key, space_id in SPACE_MAP.items():
        if key.lower() in text_lower:
            return space_id
    return None


# ---------------------------------------------------------------------------
# SESSION
# ---------------------------------------------------------------------------

def _make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(HEADERS)
    return session


# ---------------------------------------------------------------------------
# API FETCH
# ---------------------------------------------------------------------------

def fetch_page(page: int, start_date: Optional[str] = None,
               session: Optional[requests.Session] = None) -> dict:
    params = {
        "page": page,
        "per_page": PER_PAGE,
        "status": "publish",
    }
    if start_date:
        params["start_date"] = start_date

    s = session or _make_session()
    try:
        resp = s.get(API_BASE, params=params, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.Timeout:
        logger.error(f"CCB timeout página {page}")
        return {}
    except requests.exceptions.HTTPError as e:
        logger.error(f"CCB HTTP error página {page}: {e}")
        return {}
    except requests.exceptions.RequestException as e:
        logger.error(f"CCB erro página {page}: {e}")
        return {}
    except ValueError as e:
        logger.error(f"CCB JSON decode error página {page}: {e}")
        return {}


def fetch_all_events(start_date: Optional[str] = None) -> list[dict]:
    now = datetime.now(timezone.utc)
    if start_date is None:
        start_date = now.strftime("%Y-%m-%d 00:00:00")

    logger.info(f"CCB: a iniciar recolha a partir de {start_date}")
    all_events = []
    page = 1
    session = _make_session()

    while page <= MAX_PAGES:
        logger.info(f"CCB: página {page}...")
        data = fetch_page(page, start_date, session)

        if not data or "events" not in data:
            logger.info(f"CCB: sem mais eventos na página {page}")
            break

        events = data.get("events", [])
        if not events:
            break

        all_events.extend(events)
        logger.info(f"CCB: {len(events)} eventos na página {page} (total: {len(all_events)})")

        total_pages = data.get("total_pages", 1)
        if page >= total_pages:
            break

        page += 1
        time.sleep(REQUEST_DELAY)

    logger.info(f"CCB: recolha completa — {len(all_events)} eventos raw")
    return all_events


# ---------------------------------------------------------------------------
# HTML DETAIL FETCH + PARSE
# ---------------------------------------------------------------------------

def fetch_detail_html(url: str, session: requests.Session) -> Optional[BeautifulSoup]:
    """Faz fetch da página de detalhe de um evento e devolve BeautifulSoup."""
    try:
        resp = session.get(url, headers=HEADERS_HTML, timeout=TIMEOUT)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")
    except Exception as e:
        logger.warning(f"CCB: erro ao fazer fetch de {url}: {e}")
        return None


def parse_detail_subtitle(soup: BeautifulSoup) -> Optional[str]:
    """Extrai o subtítulo (h2.h2__subpages)."""
    el = soup.select_one("h2.h2__subpages")
    if el:
        return el.get_text(strip=True) or None
    return None


def parse_detail_space(soup: BeautifulSoup) -> Optional[str]:
    """Extrai o espaço a partir do filtro de sala na barra de filtros."""
    # Procura no filtro bar: <span class="title__filter"> que não tem link
    for el in soup.select(".filter_choose .title__filter"):
        icon = el.select_one(".icon-localizacao, .icons.icon-localizacao")
        if icon:
            text = el.get_text(strip=True)
            resolved = _resolve_space(text)
            if resolved:
                return resolved
    return None


def parse_detail_dates(soup: BeautifulSoup) -> list[dict]:
    """
    Extrai datas múltiplas a partir do bloco .data__info__detail.
    Cada <p class="info__data"> é uma sessão.
    """
    dates = []
    container = soup.select_one(".data__info__detail")
    if not container:
        return dates

    for p in container.select("p.info__data"):
        strong = p.select_one("strong.spotlight")
        date_text = strong.get_text(strip=True) if strong else ""
        rest = p.get_text(strip=True).replace(date_text, "").strip()

        # Extrai data
        date_str = _parse_portuguese_date(date_text)

        # Extrai hora
        time_match = re.search(r'(\d{1,2}):(\d{2})', rest)
        time_start = f"{time_match.group(1).zfill(2)}:{time_match.group(2)}" if time_match else None

        # Detecta "DATA EXTRA"
        is_extra = "EXTRA" in rest.upper() or "EXTRA" in date_text.upper()

        dates.append({
            "date": date_str,
            "time_start": time_start,
            "time_end": None,
            "duration_minutes": None,
            "is_cancelled": False,
            "is_sold_out": False,
            "is_extra": is_extra,
            "notes": rest if rest else None,
        })

    return dates


def _parse_portuguese_date(text: str) -> Optional[str]:
    """Converte 'Quinta-feira, 2 abril de 2026' → '2026-04-02'."""
    months = {
        "janeiro": 1, "fevereiro": 2, "março": 3, "abril": 4,
        "maio": 5, "junho": 6, "julho": 7, "agosto": 8,
        "setembro": 9, "outubro": 10, "novembro": 11, "dezembro": 12,
    }
    text_lower = text.lower()
    m = re.search(
        r'(\d{1,2})\s+(' + "|".join(months.keys()) + r')\s+(?:de\s+)?(\d{4})',
        text_lower
    )
    if m:
        day = int(m.group(1))
        month = months[m.group(2)]
        year = int(m.group(3))
        return f"{year:04d}-{month:02d}-{day:02d}"
    return None


def parse_detail_ticketing(soup: BeautifulSoup) -> Optional[str]:
    """Extrai URL de compra de bilhetes (botão 'Comprar Bilhete')."""
    btn = soup.select_one("button[onclick*='ccb.bol.pt'], button[onclick*='bol.pt']")
    if btn:
        onclick = btn.get("onclick", "")
        m = re.search(r"window\.open\('([^']+)'\)", onclick)
        if m:
            return m.group(1)
    # Alternativa: link direto
    link = soup.select_one("a[href*='ccb.bol.pt'], a[href*='bol.pt/Comprar']")
    if link:
        return link.get("href")
    return None


def parse_detail_is_free(soup: BeautifulSoup) -> bool:
    """Detecta se o evento é entrada livre."""
    # Botão "Entrada Livre" na barra de filtros
    free_btn = soup.select_one(".card_free, .btn.card_free")
    if free_btn:
        return True
    # Texto "Entrada Livre" ou "Gratuito" na descrição
    desc = soup.select_one("#first-content")
    if desc:
        text = desc.get_text().lower()
        if "entrada livre" in text or "entrada gratuita" in text:
            return True
    return False


def parse_detail_is_sold_out(soup: BeautifulSoup) -> bool:
    """Detecta se os bilhetes estão esgotados."""
    for el in soup.select(".card_text_button, span.card_text_button"):
        if "esgotado" in el.get_text().lower():
            return True
    return False


def parse_detail_audience_age(soup: BeautifulSoup) -> Optional[str]:
    """Extrai restrição de idade ('+6', 'M/12 anos', etc.)."""
    container = soup.select_one("#idades")
    if container:
        p = container.find_next("p", class_="text__info__detail")
        if p:
            return p.get_text(strip=True)
    return None


def parse_detail_credits(soup: BeautifulSoup) -> Optional[str]:
    """Extrai ficha técnica como texto raw."""
    # Secção "Ficha Técnica"
    for title in soup.select("p.__titles"):
        if "ficha técnica" in title.get_text().lower():
            # Pega os divs de texto a seguir
            parts = []
            for sib in title.find_next_siblings():
                if sib.name == "p" and "__titles" in sib.get("class", []):
                    break
                if sib.name in ("div", "p"):
                    text = sib.get_text(" ", strip=True)
                    if text:
                        parts.append(text)
            return " | ".join(parts) if parts else None
    return None


def parse_detail_coproduction(soup: BeautifulSoup) -> Optional[str]:
    """Extrai coprodução."""
    for el in soup.select(".spotlight__title"):
        if "coprod" in el.get_text().lower():
            desc = el.find_next(".spotlight__desc")
            if desc:
                return desc.get_text(strip=True)
    return None


def enrich_from_detail(raw: dict, session: requests.Session) -> dict:
    """
    Faz fetch da página de detalhe e enriquece o evento raw com dados adicionais.
    Mantém valores existentes da API se o detail não trouxer nada melhor.
    """
    url = raw.get("source_url", "")
    if not url:
        return raw

    soup = fetch_detail_html(url, session)
    if not soup:
        return raw

    # Subtitle — quase sempre ausente da API
    subtitle = parse_detail_subtitle(soup)
    if subtitle:
        raw["subtitle"] = subtitle

    # Space — mais fiável no HTML que na API
    space = parse_detail_space(soup)
    if space:
        raw["space_id"] = space

    # Datas múltiplas — sobrepõe as da API se encontrar
    dates = parse_detail_dates(soup)
    if dates:
        raw["dates"] = dates

    # Ticketing URL
    ticketing = parse_detail_ticketing(soup)
    if ticketing:
        raw["ticketing_url"] = ticketing

    # Entrada livre
    raw["is_free"] = parse_detail_is_free(soup)

    # Esgotado
    # Propaga para cada data se global
    if parse_detail_is_sold_out(soup):
        for d in raw.get("dates", []):
            d["is_sold_out"] = True

    # Idade
    age = parse_detail_audience_age(soup)
    if age:
        raw["audience_age"] = age

    # Ficha técnica
    credits = parse_detail_credits(soup)
    if credits:
        raw["credits_raw"] = credits

    # Coprodução
    coproduction = parse_detail_coproduction(soup)
    if coproduction:
        raw["coproduction"] = coproduction

    return raw


# ---------------------------------------------------------------------------
# PARSERS API (mantidos do original, com melhorias)
# ---------------------------------------------------------------------------

def parse_ccb_categories(event: dict) -> list[str]:
    cats = []
    for cat in event.get("categories", []):
        if isinstance(cat, dict):
            name = cat.get("name", "")
            if name:
                cats.append(name)
        elif isinstance(cat, str):
            cats.append(cat)
    for tag in event.get("tags", []):
        if isinstance(tag, dict):
            name = tag.get("name", "")
            if name:
                cats.append(name)
    return cats


def parse_ccb_dates_api(event: dict) -> list[dict]:
    """Parse de datas a partir da API (fallback se detail não disponível)."""
    dates = []
    start_raw = event.get("start_date", "")
    end_raw = event.get("end_date", "")

    if start_raw:
        parts = start_raw.split(" ")
        date_str = parts[0] if parts else ""
        time_str = parts[1][:5] if len(parts) > 1 else None

        end_parts = end_raw.split(" ") if end_raw else []
        end_time = end_parts[1][:5] if len(end_parts) > 1 else None

        dates.append({
            "date": date_str,
            "time_start": time_str,
            "time_end": end_time if end_parts and end_parts[0] == date_str else None,
            "duration_minutes": None,
            "is_cancelled": False,
            "is_sold_out": False,
            "is_extra": False,
            "notes": None,
        })

    return dates


def parse_ccb_price(event: dict) -> str:
    cost = event.get("cost", "")
    if isinstance(cost, (int, float)):
        return f"{cost}€"
    return str(cost) if cost else ""


def parse_ccb_image(event: dict) -> Optional[str]:
    image = event.get("image", {})
    if isinstance(image, dict):
        return (
            image.get("url")
            or image.get("sizes", {}).get("large", {}).get("url")
            or image.get("sizes", {}).get("full", {}).get("url")
        )
    return None


def parse_ccb_venue_space_api(event: dict) -> Optional[str]:
    venue_data = event.get("venue", {})
    if isinstance(venue_data, dict):
        venue_name = venue_data.get("venue", "")
        return _resolve_space(venue_name)
    return None


def parse_ccb_accessibility(event: dict) -> dict:
    desc = event.get("description", "") or ""
    excerpt = event.get("excerpt", "") or ""
    text = f"{desc} {excerpt}".lower()

    return {
        "has_sign_language": bool("lgp" in text or "língua gestual" in text),
        "has_audio_description": bool("audiodescri" in text or "áudio descri" in text),
        "has_subtitles": bool("legenda" in text),
        "is_relaxed_performance": bool("sessão relaxada" in text or "relaxed" in text),
        "wheelchair_accessible": True,
        "notes": None,
    }


# ---------------------------------------------------------------------------
# CONVERTER EVENTO API → RAW NORMALIZADO
# ---------------------------------------------------------------------------

def ccb_event_to_raw(event: dict) -> dict:
    now = datetime.now(timezone.utc).isoformat()

    return {
        "source_id": str(event.get("id", "")),
        "source_url": event.get("url", ""),
        "title": event.get("title", ""),
        "subtitle": None,                           # preenchido pelo enrich
        "description": event.get("description", ""),
        "excerpt": event.get("excerpt", ""),
        "categories": parse_ccb_categories(event),
        "tags": [t.get("name", "") for t in event.get("tags", []) if isinstance(t, dict)],
        "dates": parse_ccb_dates_api(event),        # sobreposto pelo enrich se detail disponível
        "price_raw": parse_ccb_price(event),
        "ticketing_url": event.get("website", "") or None,
        "is_free": False,                           # preenchido pelo enrich
        "audience": event.get("cost_description", "") or "",
        "audience_age": None,                       # preenchido pelo enrich
        "cover_image": parse_ccb_image(event),
        "space_id": parse_ccb_venue_space_api(event),
        "accessibility": parse_ccb_accessibility(event),
        "credits_raw": None,                        # preenchido pelo enrich
        "coproduction": None,                       # preenchido pelo enrich
        "is_ongoing": False,
        "scraped_at": now,
        "_ccb_slug": event.get("slug", ""),
        "_ccb_all_day": event.get("all_day", False),
        "_ccb_featured": event.get("featured", False),
    }


# ---------------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------------

def run(start_date: Optional[str] = None) -> list[dict]:
    raw_events = fetch_all_events(start_date)
    normalized = [ccb_event_to_raw(e) for e in raw_events]

    if not ENRICH_DETAIL_PAGES:
        logger.info("CCB: enriquecimento HTML desativado")
        return normalized

    # Enriquecimento via páginas de detalhe
    to_enrich = normalized
    if DETAIL_BATCH_SIZE:
        to_enrich = normalized[:DETAIL_BATCH_SIZE]

    session = _make_session()
    logger.info(f"CCB: a enriquecer {len(to_enrich)} eventos via HTML...")

    for i, event in enumerate(to_enrich):
        url = event.get("source_url", "")
        if not url:
            continue
        logger.debug(f"CCB: enrich {i+1}/{len(to_enrich)} — {url}")
        enrich_from_detail(event, session)
        time.sleep(DETAIL_DELAY)

    logger.info("CCB: enriquecimento completo")
    return normalized


if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO)
    events = run()
    print(json.dumps(events[:2], indent=2, ensure_ascii=False))
    print(f"\nTotal: {len(events)} eventos")
