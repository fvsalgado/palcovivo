"""
Primeira Plateia — Scraper Theatro Circo (Braga)
Venue: Theatro Circo | theatrocirco.com

Estratégia de fallback em cascata:
  1. The Events Calendar REST API  →  /wp-json/tribe/events/v1/events
  2. Sitemap XML                   →  /sitemap.xml ou /sitemap_index.xml
                                       filtra URLs /event/ e faz parse HTML de cada uma
  3. Página de programação HTML    →  /programacao/ (parse directo)

O scraper tenta o método 1. Se falhar (timeout, 404, bloqueio),
tenta o método 2. Se falhar, tenta o método 3.
Cada fallback é registado no log para diagnóstico.
"""

import re
import time
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Optional

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
VENUE_ID    = "theatro-circo"
SCRAPER_ID  = "theatro-circo"
WEBSITE     = "https://theatrocirco.com"

# Método 1 — The Events Calendar REST API
API_BASE    = f"{WEBSITE}/wp-json/tribe/events/v1/events"
PER_PAGE    = 50
MAX_PAGES   = 20

# Método 2 — Sitemap
SITEMAP_URLS = [
    f"{WEBSITE}/sitemap.xml",
    f"{WEBSITE}/sitemap_index.xml",
    f"{WEBSITE}/wp-sitemap.xml",
    f"{WEBSITE}/event-sitemap.xml",
]
EVENT_URL_PATTERN = re.compile(r"/event/[^/]+/?$")

# Método 3 — HTML directo
PROGRAMME_URL = f"{WEBSITE}/programacao/"

REQUEST_DELAY = 0.5   # reduzido: sitemap já é lento, não agravar
TIMEOUT       = 30

# Filtro de datas — só URLs modificadas nos últimos N dias
# Evita processar 1000+ eventos de arquivo
SITEMAP_MAX_AGE_DAYS = 365    # ignorar eventos sem actividade há mais de 12 meses
# Data mínima de lastmod para considerar uma URL relevante
from datetime import timedelta
SITEMAP_MIN_LASTMOD  = (datetime.now(timezone.utc) - timedelta(days=SITEMAP_MAX_AGE_DAYS)).strftime("%Y-%m-%d")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/json,*/*;q=0.8",
    "Accept-Language": "pt-PT,pt;q=0.9,en;q=0.8",
    "Referer": f"{WEBSITE}/",
}


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
# MÉTODO 1 — THE EVENTS CALENDAR REST API
# ---------------------------------------------------------------------------

def _fetch_api_page(page: int, start_date: str, session: requests.Session) -> dict:
    params = {"page": page, "per_page": PER_PAGE, "status": "publish",
              "start_date": start_date}
    try:
        resp = session.get(API_BASE, params=params, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.Timeout:
        logger.warning(f"TC API: timeout na página {page}")
        return {}
    except requests.exceptions.HTTPError as e:
        logger.warning(f"TC API: HTTP {e.response.status_code} na página {page}")
        return {}
    except (requests.exceptions.RequestException, ValueError) as e:
        logger.warning(f"TC API: erro na página {page}: {e}")
        return {}


def _scrape_via_api(start_date: str, session: requests.Session) -> list[dict]:
    """Tenta recolher eventos via The Events Calendar REST API."""
    logger.info("TC: método 1 — API WP (The Events Calendar)")
    all_events = []
    page = 1

    while page <= MAX_PAGES:
        data = _fetch_api_page(page, start_date, session)
        if not data or "events" not in data:
            break
        events = data.get("events", [])
        if not events:
            break
        all_events.extend(events)
        logger.info(f"TC API: página {page} → {len(events)} eventos (total: {len(all_events)})")
        if page >= data.get("total_pages", 1):
            break
        page += 1
        time.sleep(REQUEST_DELAY)

    if all_events:
        logger.info(f"TC API: sucesso — {len(all_events)} eventos recolhidos")
    else:
        logger.warning("TC API: sem eventos — a tentar fallback")
    return all_events


def _parse_api_event(event: dict) -> dict:
    """Converte evento da API WP para formato raw intermédio."""
    start_raw = event.get("start_date", "")
    end_raw   = event.get("end_date", "")
    parts     = start_raw.split(" ") if start_raw else []
    end_parts = end_raw.split(" ")   if end_raw   else []

    dates = []
    if parts:
        dates.append({
            "date":             parts[0],
            "time_start":       parts[1][:5] if len(parts) > 1 else None,
            "time_end":         end_parts[1][:5] if end_parts and end_parts[0] == parts[0] else None,
            "duration_minutes": None,
            "is_cancelled":     False,
            "is_sold_out":      False,
            "notes":            None,
        })

    cats = []
    for cat in event.get("categories", []):
        if isinstance(cat, dict) and cat.get("name"):
            cats.append(cat["name"])

    image = event.get("image", {})
    cover = None
    if isinstance(image, dict):
        cover = (image.get("url")
                 or image.get("sizes", {}).get("large", {}).get("url")
                 or image.get("sizes", {}).get("full", {}).get("url"))

    desc  = event.get("description", "") or ""
    text  = (desc + (event.get("excerpt", "") or "")).lower()

    return {
        "source_id":    str(event.get("id", "")),
        "source_url":   event.get("url", ""),
        "title":        event.get("title", ""),
        "subtitle":     None,
        "description":  desc,
        "categories":   cats,
        "tags":         [t.get("name", "") for t in event.get("tags", []) if isinstance(t, dict)],
        "dates":        dates,
        "price_raw":    str(event.get("cost", "")) if event.get("cost") else "",
        "ticketing_url": event.get("website") or None,
        "audience":     "",
        "cover_image":  cover,
        "space_id":     None,
        "accessibility": {
            "has_sign_language":    "lgp" in text or "língua gestual" in text,
            "has_audio_description": "audiodescri" in text,
            "has_subtitles":        "legenda" in text,
            "is_relaxed_performance": "sessão relaxada" in text or "relaxed" in text,
            "wheelchair_accessible": True,
            "notes":                None,
        },
        "credits_raw":  None,
        "is_ongoing":   False,
        "scraped_at":   datetime.now(timezone.utc).isoformat(),
        "_method":      "api",
    }


# ---------------------------------------------------------------------------
# MÉTODO 2 — SITEMAP XML
# ---------------------------------------------------------------------------

def _fetch_sitemap_event_urls(session: requests.Session) -> list[str]:
    """
    Tenta encontrar URLs de eventos no sitemap.
    Filtra por lastmod (só eventos recentes) e limita o total de URLs.
    Suporta sitemap simples e sitemap index com sub-sitemaps.
    """
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

    def extract_urls_with_lastmod(xml_text: str) -> list[tuple[str, str]]:
        """Extrai (url, lastmod) de um sitemap XML. lastmod pode ser ''."""
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError:
            return []
        results = []
        for url_el in (root.findall("sm:url", ns) or root.findall("url")):
            loc = url_el.find("sm:loc", ns) or url_el.find("loc")
            lmod = url_el.find("sm:lastmod", ns) or url_el.find("lastmod")
            if loc is not None and loc.text:
                url = loc.text.strip()
                lastmod = lmod.text.strip()[:10] if lmod is not None and lmod.text else ""
                if EVENT_URL_PATTERN.search(url):
                    results.append((url, lastmod))
        return results

    def filter_and_sort(url_lastmod_pairs: list[tuple[str, str]]) -> list[str]:
        """
        Filtra URLs por lastmod >= SITEMAP_MIN_LASTMOD.
        Ordena por lastmod decrescente (mais recentes primeiro).
        Limita a SITEMAP_MAX_URLS.
        """
        # Separar com e sem lastmod
        with_date = [(u, d) for u, d in url_lastmod_pairs if d >= SITEMAP_MIN_LASTMOD]
        without_date = [u for u, d in url_lastmod_pairs if not d]

        # Ordenar os datados por mais recente
        with_date.sort(key=lambda x: x[1], reverse=True)
        sorted_urls = [u for u, _ in with_date] + without_date

        total_raw = len(url_lastmod_pairs)
        total_filtered = len(sorted_urls)
        if total_raw > total_filtered:
            logger.info(
                f"TC Sitemap: {total_raw} URLs totais → "
                f"{total_filtered} recentes (>= {SITEMAP_MIN_LASTMOD})"
            )

        return sorted_urls

    for sitemap_url in SITEMAP_URLS:
        try:
            resp = session.get(sitemap_url, timeout=TIMEOUT)
            if resp.status_code != 200:
                continue
            logger.info(f"TC Sitemap: encontrado em {sitemap_url}")
            root = ET.fromstring(resp.text)

            # Sitemap index → seguir sub-sitemaps de eventos
            sub_sitemaps = root.findall("sm:sitemap/sm:loc", ns) or root.findall("sitemap/loc")
            if sub_sitemaps:
                all_pairs = []
                for sub_el in sub_sitemaps:
                    sub_url = sub_el.text.strip() if sub_el.text else ""
                    if not sub_url or "event" not in sub_url.lower():
                        continue
                    try:
                        sub_resp = session.get(sub_url, timeout=TIMEOUT)
                        pairs = extract_urls_with_lastmod(sub_resp.text)
                        all_pairs.extend(pairs)
                        logger.info(f"TC Sitemap: {len(pairs)} URLs em {sub_url}")
                    except Exception as e:
                        logger.warning(f"TC Sitemap: erro sub-sitemap {sub_url}: {e}")
                if all_pairs:
                    return filter_and_sort(all_pairs)

            # Sitemap simples
            pairs = extract_urls_with_lastmod(resp.text)
            if pairs:
                logger.info(f"TC Sitemap: {len(pairs)} URLs de eventos no sitemap")
                return filter_and_sort(pairs)

        except requests.exceptions.RequestException as e:
            logger.debug(f"TC Sitemap: {sitemap_url} inacessível: {e}")
        except ET.ParseError as e:
            logger.debug(f"TC Sitemap: erro XML em {sitemap_url}: {e}")

    logger.warning("TC Sitemap: nenhum sitemap acessível")
    return []


def _parse_event_page(url: str, session: requests.Session) -> Optional[dict]:
    """Faz parse de uma página individual de evento do Theatro Circo."""
    try:
        resp = session.get(url, timeout=TIMEOUT)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.debug(f"TC parse: erro ao aceder {url}: {e}")
        return None

    soup = BeautifulSoup(resp.text, "lxml")

    # Título
    title = ""
    for sel in ["h1.tribe-events-single-event-title", "h1.entry-title", "h1"]:
        el = soup.select_one(sel)
        if el:
            title = el.get_text(strip=True)
            break
    if not title:
        return None

    # Datas — The Events Calendar usa atributos datetime nos <abbr> ou <time>
    dates = []
    for abbr in soup.select("abbr.tribe-events-abbr, time[datetime]"):
        dt_str = abbr.get("datetime") or abbr.get("title", "")
        if dt_str and len(dt_str) >= 10:
            date_part = dt_str[:10]
            time_part = dt_str[11:16] if len(dt_str) > 10 else None
            dates.append({
                "date":             date_part,
                "time_start":       time_part,
                "time_end":         None,
                "duration_minutes": None,
                "is_cancelled":     False,
                "is_sold_out":      False,
                "notes":            None,
            })

    # Fallback de data: texto visível
    if not dates:
        for sel in [".tribe-events-start-datetime", ".tribe-event-date-start",
                    ".tribe-events-schedule"]:
            el = soup.select_one(sel)
            if el:
                dates.append({
                    "date":             None,
                    "time_start":       None,
                    "time_end":         None,
                    "duration_minutes": None,
                    "is_cancelled":     False,
                    "is_sold_out":      False,
                    "notes":            el.get_text(strip=True),
                })
                break

    # Descrição
    desc = ""
    for sel in [".tribe-events-single-description", ".entry-content", ".tribe-events-content"]:
        el = soup.select_one(sel)
        if el:
            desc = el.get_text(separator="\n", strip=True)
            break

    # Imagem
    cover = None
    og_img = soup.find("meta", property="og:image")
    if og_img:
        cover = og_img.get("content")
    if not cover:
        el = soup.select_one(".tribe-events-event-image img, .wp-post-image")
        if el:
            cover = el.get("src")

    # Preço
    price_raw = ""
    for sel in [".tribe-events-event-cost", ".tribe-venue-cost", ".tribe-ticket-cost"]:
        el = soup.select_one(sel)
        if el:
            price_raw = el.get_text(strip=True)
            break

    # Categorias
    cats = [el.get_text(strip=True) for el in soup.select(
        ".tribe-events-event-categories a, .tribe-cat a"
    )]

    # Schema.org JSON-LD (mais fiável quando existe)
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            import json as _json
            data = _json.loads(script.string or "")
            if isinstance(data, list):
                data = data[0]
            if data.get("@type") == "Event":
                start = data.get("startDate", "")
                end   = data.get("endDate", "")
                if start:
                    dates = [{
                        "date":             start[:10],
                        "time_start":       start[11:16] if len(start) > 10 else None,
                        "time_end":         end[11:16]   if len(end)   > 10 else None,
                        "duration_minutes": None,
                        "is_cancelled":     False,
                        "is_sold_out":      False,
                        "notes":            None,
                    }]
                if not price_raw:
                    offers = data.get("offers", {})
                    if isinstance(offers, list):
                        offers = offers[0]
                    price_raw = str(offers.get("price", "")) if offers else ""
        except Exception:
            pass

    text = desc.lower()
    return {
        "source_id":    url.rstrip("/").split("/")[-1],
        "source_url":   url,
        "title":        title,
        "subtitle":     None,
        "description":  desc,
        "categories":   cats,
        "tags":         [],
        "dates":        dates,
        "price_raw":    price_raw,
        "ticketing_url": url,
        "audience":     "",
        "cover_image":  cover,
        "space_id":     None,
        "accessibility": {
            "has_sign_language":      "lgp" in text or "língua gestual" in text,
            "has_audio_description":  "audiodescri" in text,
            "has_subtitles":          "legenda" in text,
            "is_relaxed_performance": "sessão relaxada" in text or "relaxed" in text,
            "wheelchair_accessible":  True,
            "notes":                  None,
        },
        "credits_raw":  None,
        "is_ongoing":   False,
        "scraped_at":   datetime.now(timezone.utc).isoformat(),
        "_method":      "sitemap",
    }


def _scrape_via_sitemap(session: requests.Session) -> list[dict]:
    """Tenta recolher eventos via sitemap + parse HTML individual."""
    logger.info("TC: método 2 — Sitemap XML + parse HTML")
    event_urls = _fetch_sitemap_event_urls(session)
    if not event_urls:
        return []

    events = []
    for i, url in enumerate(event_urls):
        event = _parse_event_page(url, session)
        if event:
            events.append(event)
        if (i + 1) % 10 == 0:
            logger.info(f"TC Sitemap: {i+1}/{len(event_urls)} páginas processadas")
        time.sleep(0.5)

    logger.info(f"TC Sitemap: {len(events)} eventos recolhidos de {len(event_urls)} URLs")
    return events


# ---------------------------------------------------------------------------
# MÉTODO 3 — HTML DA PÁGINA DE PROGRAMAÇÃO
# ---------------------------------------------------------------------------

def _scrape_via_html(session: requests.Session) -> list[dict]:
    """
    Último recurso: faz parse da página /programacao/ e segue links de eventos.
    Mais frágil mas funciona se os outros métodos falharem.
    """
    logger.info("TC: método 3 — HTML directo da página de programação")
    try:
        resp = session.get(PROGRAMME_URL, timeout=TIMEOUT)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"TC HTML: erro ao aceder programação: {e}")
        return []

    soup = BeautifulSoup(resp.text, "lxml")

    # Encontrar todos os links para eventos
    event_links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if EVENT_URL_PATTERN.search(href):
            url = href if href.startswith("http") else f"{WEBSITE}{href}"
            event_links.add(url)

    logger.info(f"TC HTML: {len(event_links)} links de eventos encontrados")
    if not event_links:
        return []

    events = []
    for url in sorted(event_links):
        event = _parse_event_page(url, session)
        if event:
            event["_method"] = "html"
            events.append(event)
        time.sleep(0.5)

    logger.info(f"TC HTML: {len(events)} eventos recolhidos")
    return events


# ---------------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------------

def run(start_date: Optional[str] = None) -> list[dict]:
    """
    Ponto de entrada do scraper Theatro Circo.
    Tenta os três métodos em cascata, parando no primeiro que funcionar.
    """
    if start_date is None:
        start_date = datetime.now(timezone.utc).strftime("%Y-%m-%d 00:00:00")

    session = _make_session()
    raw_events = []

    # Método 1 — API WP
    raw_events = _scrape_via_api(start_date, session)
    if raw_events:
        return [_parse_api_event(e) for e in raw_events]

    # Método 2 — Sitemap
    raw_events = _scrape_via_sitemap(session)
    if raw_events:
        return raw_events

    # Método 3 — HTML
    raw_events = _scrape_via_html(session)
    if raw_events:
        return raw_events

    logger.error("TC: todos os métodos falharam — sem eventos recolhidos")
    return []


if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO)
    events = run()
    print(json.dumps(events[:2], indent=2, ensure_ascii=False))
    print(f"\nTotal: {len(events)} eventos | Método: {events[0].get('_method') if events else 'N/A'}")
