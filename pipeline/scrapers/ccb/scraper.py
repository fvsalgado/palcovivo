"""
Primeira Plateia — Scraper CCB
Fonte: The Events Calendar REST API (wp-json/tribe/events/v1/events)
"""

import requests
import time
import logging
from datetime import datetime, timezone
from typing import Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
VENUE_ID = "ccb"
SCRAPER_ID = "ccb"
API_BASE = "https://www.ccb.pt/wp-json/tribe/events/v1/events"
PER_PAGE = 50
MAX_PAGES = 20
REQUEST_DELAY = 2.0   # segundos entre pedidos
TIMEOUT = 45          # aumentado: CCB pode ser lento a responder

# User-Agent de browser real — alguns servidores bloqueiam user-agents de bots
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


def _make_session() -> requests.Session:
    """Session com retry automático (3 tentativas, backoff exponencial)."""
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=2,        # espera 2s, 4s, 8s entre tentativas
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(HEADERS)
    return session


# ---------------------------------------------------------------------------
# FETCH
# ---------------------------------------------------------------------------

def fetch_page(page: int, start_date: Optional[str] = None, session: Optional[requests.Session] = None) -> dict:
    """Busca uma página de eventos da API do CCB."""
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
        logger.error(f"CCB timeout na página {page} (>{TIMEOUT}s)")
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
    """
    Busca todos os eventos da API do CCB com paginação automática.
    start_date: "YYYY-MM-DD" — se None, busca todos os eventos futuros
    """
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
# PARSERS DE CAMPOS ESPECÍFICOS DO CCB
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


def parse_ccb_dates(event: dict) -> list[dict]:
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


def parse_ccb_venue_space(event: dict) -> Optional[str]:
    venue_data = event.get("venue", {})
    if isinstance(venue_data, dict):
        venue_name = venue_data.get("venue", "")
        space_map = {
            "Grande Auditório": "grande-auditorio",
            "Pequeno Auditório": "pequeno-auditorio",
            "MAC/CCB": "mac-ccb",
            "Museu de Arte Contemporânea": "mac-ccb",
            "Garagem Sul": "garagem-sul",
            "Sala de Exposições": "sala-exposicoes",
            "Jardins": "jardins",
            "Átrio": "atrio",
        }
        for key, space_id in space_map.items():
            if key.lower() in venue_name.lower():
                return space_id
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
# CONVERTER EVENTO CCB → RAW NORMALIZADO
# ---------------------------------------------------------------------------

def ccb_event_to_raw(event: dict) -> dict:
    now = datetime.now(timezone.utc).isoformat()

    return {
        "source_id": str(event.get("id", "")),
        "source_url": event.get("url", ""),
        "title": event.get("title", ""),
        "subtitle": None,
        "description": event.get("description", ""),
        "excerpt": event.get("excerpt", ""),
        "categories": parse_ccb_categories(event),
        "tags": [t.get("name", "") for t in event.get("tags", []) if isinstance(t, dict)],
        "dates": parse_ccb_dates(event),
        "price_raw": parse_ccb_price(event),
        "ticketing_url": event.get("website", "") or None,
        "audience": event.get("cost_description", "") or "",
        "cover_image": parse_ccb_image(event),
        "space_id": parse_ccb_venue_space(event),
        "accessibility": parse_ccb_accessibility(event),
        "credits_raw": None,
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
    return [ccb_event_to_raw(e) for e in raw_events]


if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO)
    events = run()
    print(json.dumps(events[:2], indent=2, ensure_ascii=False))
    print(f"\nTotal: {len(events)} eventos")
