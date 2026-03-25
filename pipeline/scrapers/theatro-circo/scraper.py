"""
Primeira Plateia — Scraper Theatro Circo (Braga)
Venue: Theatro Circo | theatrocirco.com

Estratégia de fallback em cascata:
  1. The Events Calendar REST API  →  /wp-json/tribe/events/v1/events
  2. Sitemap XML (com filtro de lastmod)  →  event-sitemap.xml
     Parse JSON-LD de cada página (schema.org Event)
  3. Página de programação HTML  →  /programacao/ ou /programme/

Optimizações de performance:
  - Sitemap ordenado por lastmod DESC → processa os mais recentes primeiro
  - SITEMAP_MAX_AGE_DAYS: só URLs com lastmod nos últimos N dias
  - Paragem antecipada: para de processar quando encontra URLs muito antigas
  - JSON-LD como método primário de extracção (mais rápido que scraping HTML)
  - REQUEST_DELAY reduzido: 0.3s (suficiente para ser educado)
"""

import re
import json as _json
import time
import logging
from datetime import datetime, timezone, timedelta
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
EVENT_URL_PATTERN = re.compile(r"(?<!/en)/event/[^/]+/?$")  # exclui /en/event/

# Filtro de lastmod — só processar URLs modificadas nos últimos N dias
SITEMAP_MAX_AGE_DAYS = 365  # 12 meses

# Método 3 — HTML directo
PROGRAMME_URLS = [
    f"{WEBSITE}/programa/",        # URL real confirmada
    f"{WEBSITE}/programme/",       # versão EN (links podem dar 404)
    f"{WEBSITE}/programacao/",
    f"{WEBSITE}/agenda/",
]

REQUEST_DELAY = 0.3
TIMEOUT       = 20

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "pt-PT,pt;q=0.9,en;q=0.8",
}

# ---------------------------------------------------------------------------
# SESSION com retry automático
# ---------------------------------------------------------------------------

def _make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3, backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://",  adapter)
    session.headers.update(HEADERS)
    return session


def _get(session, url, timeout=TIMEOUT, params=None):
    """GET com tratamento de erros. Aceita params opcionais para query string."""
    try:
        resp = session.get(url, timeout=timeout, params=params)
        resp.raise_for_status()
        return resp
    except requests.exceptions.Timeout:
        logger.warning(f"TC: timeout — {url}")
    except requests.exceptions.HTTPError as e:
        logger.warning(f"TC API: HTTP {e.response.status_code} — {url}")
    except requests.exceptions.RequestException as e:
        logger.debug(f"TC: erro — {url}: {e}")
    return None


# ---------------------------------------------------------------------------
# MÉTODO 1 — The Events Calendar REST API
# ---------------------------------------------------------------------------

def _scrape_via_api(start_date: str, session: requests.Session) -> list[dict]:
    logger.info("TC: método 1 — API WP (The Events Calendar)")
    all_events = []
    page = 1

    while page <= MAX_PAGES:
        resp = _get(session, API_BASE, params={
            "page": page, "per_page": PER_PAGE,
            "status": "publish", "start_date": start_date
        })
        if not resp:
            break
        data = resp.json()
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
        logger.info(f"TC API: {len(all_events)} eventos")
    else:
        logger.warning("TC API: sem eventos — a tentar fallback")
    return all_events


def _parse_api_event(event: dict) -> dict:
    start_raw = event.get("start_date", "")
    end_raw   = event.get("end_date",   "")
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
    text = (event.get("description") or "").lower()
    return {
        "source_id":    str(event.get("id", "")),
        "source_url":   event.get("url", ""),
        "title":        event.get("title", ""),
        "subtitle":     None,
        "description":  event.get("description", ""),
        "categories":   [c.get("name") for c in event.get("categories", [])],
        "tags":         [t.get("name") for t in event.get("tags", [])],
        "dates":        dates,
        "date_open":    parts[0] if parts else None,
        "date_close":   None,
        "is_ongoing":   False,
        "price_raw":    event.get("cost", ""),
        "ticketing_url": event.get("url", ""),
        "audience":     None,
        "cover_image":  (event.get("image") or {}).get("url"),
        "space_id":     None,
        "credits_raw":  None,
        "accessibility": {
            "has_sign_language":      "lgp" in text or "língua gestual" in text,
            "has_audio_description":  "audiodescri" in text,
            "has_subtitles":          False,
            "is_relaxed_performance": False,
            "wheelchair_accessible":  True,
            "notes":                  None,
        },
        "scraped_at":   datetime.now(timezone.utc).isoformat(),
        "_method":      "api",
    }


# ---------------------------------------------------------------------------
# MÉTODO 2 — Sitemap XML com filtro de lastmod
# ---------------------------------------------------------------------------

def _fetch_sitemap_event_urls(session: requests.Session) -> list[str]:
    """
    Lê sitemap e retorna URLs de eventos ordenadas por lastmod DESC.
    Filtra URLs com lastmod mais velho que SITEMAP_MAX_AGE_DAYS.
    URLs sem lastmod são incluídas no fim.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=SITEMAP_MAX_AGE_DAYS)).strftime("%Y-%m-%d")

    loc_re   = re.compile(r"<loc[^>]*>(https?://[^<]+)</loc>")
    lmod_re  = re.compile(r"<lastmod[^>]*>([0-9]{4}-[0-9]{2}-[0-9]{2})[^<]*</lastmod>")
    block_re = re.compile(r"<url[^>]*>(.*?)</url>", re.DOTALL)
    smap_re  = re.compile(r"<sitemap[^>]*>(.*?)</sitemap>", re.DOTALL)

    def extract_event_pairs(xml_text: str) -> list[tuple[str, str]]:
        pairs = []
        seen  = set()
        for block in block_re.findall(xml_text):
            loc_m  = loc_re.search(block)
            lmod_m = lmod_re.search(block)
            if loc_m:
                url     = loc_m.group(1).strip()
                lastmod = lmod_m.group(1) if lmod_m else ""
                if url not in seen and EVENT_URL_PATTERN.search(url):
                    pairs.append((url, lastmod))
                    seen.add(url)
        if not pairs:
            for url in loc_re.findall(xml_text):
                url = url.strip()
                if url not in seen and EVENT_URL_PATTERN.search(url):
                    pairs.append((url, ""))
                    seen.add(url)
        return pairs

    def filter_sort(pairs: list[tuple[str, str]]) -> list[str]:
        recent   = [(u, d) for u, d in pairs if d >= cutoff]
        no_date  = [u     for u, d in pairs if not d]
        too_old  = [(u, d) for u, d in pairs if d and d < cutoff]
        recent.sort(key=lambda x: x[1], reverse=True)
        result = [u for u, _ in recent] + no_date
        logger.info(
            f"TC Sitemap: {len(pairs)} URLs totais → "
            f"{len(recent)} recentes + {len(no_date)} sem data "
            f"({len(too_old)} ignoradas por serem > {SITEMAP_MAX_AGE_DAYS} dias)"
        )
        return result

    all_pairs: list[tuple[str, str]] = []

    for sitemap_url in SITEMAP_URLS:
        resp = _get(session, sitemap_url)
        if not resp:
            continue
        xml = resp.text
        logger.info(f"TC Sitemap: encontrado em {sitemap_url}")

        sub_blocks = smap_re.findall(xml)
        if sub_blocks:
            for block in sub_blocks:
                loc_m = loc_re.search(block)
                if not loc_m:
                    continue
                sub_url = loc_m.group(1).strip()
                if "event" not in sub_url.lower():
                    continue
                sub_resp = _get(session, sub_url)
                if not sub_resp:
                    continue
                pairs = extract_event_pairs(sub_resp.text)
                logger.info(f"TC Sitemap: {len(pairs)} URLs em {sub_url}")
                all_pairs.extend(pairs)
                time.sleep(0.2)
            if all_pairs:
                return filter_sort(all_pairs)

        pairs = extract_event_pairs(xml)
        if pairs:
            logger.info(f"TC Sitemap: {len(pairs)} URLs de eventos")
            return filter_sort(pairs)

    logger.warning("TC Sitemap: nenhum sitemap acessível")
    return []


def _parse_event_page(url: str, session: requests.Session) -> Optional[dict]:
    """
    Faz parse de uma página de evento do TC.
    Tenta JSON-LD primeiro (schema.org Event) — mais rápido e fiável.
    Fallback para selectores HTML genéricos.
    """
    resp = _get(session, url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "lxml")

    dates     = []
    price_raw = ""
    cover     = None
    desc      = ""
    title     = ""

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = _json.loads(script.string or "")
            if isinstance(data, list):
                data = next((d for d in data if d.get("@type") == "Event"), {})
            if data.get("@type") == "Event":
                title = data.get("name", "")
                desc  = data.get("description", "")
                start = data.get("startDate", "")
                end   = data.get("endDate",   "")
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
                offers = data.get("offers", {})
                if isinstance(offers, list):
                    offers = offers[0] if offers else {}
                price_raw = str(offers.get("price", "")) if offers else ""
                img = data.get("image", "")
                cover = img if isinstance(img, str) else (img[0] if isinstance(img, list) else "")
        except Exception:
            pass

    if not title:
        for sel in ["h1.entry-title", ".event-title h1", "h1"]:
            el = soup.select_one(sel)
            if el:
                title = el.get_text(strip=True)
                break

    if not dates:
        for t in soup.select("time[datetime]"):
            dt = t.get("datetime", "")
            if dt and len(dt) >= 10:
                dates.append({
                    "date":             dt[:10],
                    "time_start":       dt[11:16] if len(dt) > 10 else None,
                    "time_end":         None,
                    "duration_minutes": None,
                    "is_cancelled":     False,
                    "is_sold_out":      False,
                    "notes":            None,
                })

    if not desc:
        for sel in [".entry-content", ".event-description", ".event-content", "article .content"]:
            el = soup.select_one(sel)
            if el:
                desc = el.get_text(separator="\n", strip=True)
                break

    if not cover:
        og = soup.find("meta", property="og:image")
        if og:
            cover = og.get("content", "")

    if not title:
        return None

    # Filtrar apenas eventos muito antigos (> 7 dias passados)
    # Usar margem pequena para não perder espectáculos em curso
    if dates and dates[0].get("date"):
        cutoff_past = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
        if dates[0]["date"] < cutoff_past:
            return None

    text = (desc + " " + soup.get_text()).lower()
    return {
        "source_id":    url.rstrip("/").split("/")[-1],
        "source_url":   url,
        "title":        title,
        "subtitle":     None,
        "description":  desc,
        "categories":   [],
        "tags":         [],
        "dates":        dates,
        "date_open":    dates[0]["date"] if dates else None,
        "date_close":   None,
        "is_ongoing":   False,
        "price_raw":    price_raw,
        "ticketing_url": url,
        "audience":     None,
        "cover_image":  cover or None,
        "space_id":     None,
        "credits_raw":  None,
        "accessibility": {
            "has_sign_language":      "lgp" in text or "língua gestual" in text,
            "has_audio_description":  "audiodescri" in text,
            "has_subtitles":          bool(re.search(r"legenda[sd]?", text)),
            "is_relaxed_performance": "relaxed" in text or "descontraíd" in text,
            "wheelchair_accessible":  True,
            "notes":                  None,
        },
        "scraped_at":   datetime.now(timezone.utc).isoformat(),
        "_method":      "sitemap",
    }


def _scrape_via_sitemap(session: requests.Session) -> list[dict]:
    logger.info("TC: método 2 — Sitemap XML + parse HTML/JSON-LD")
    urls = _fetch_sitemap_event_urls(session)
    if not urls:
        return []

    events = []
    skipped = 0
    for i, url in enumerate(urls):
        event = _parse_event_page(url, session)
        if event:
            events.append(event)
        else:
            skipped += 1
        if (i + 1) % 10 == 0:
            logger.info(f"TC Sitemap: {i+1}/{len(urls)} páginas processadas ({len(events)} eventos, {skipped} ignorados)")
        time.sleep(REQUEST_DELAY)

    logger.info(f"TC Sitemap: {len(events)} eventos recolhidos de {len(urls)} URLs")
    return events


# ---------------------------------------------------------------------------
# MÉTODO 3 — HTML directo
# ---------------------------------------------------------------------------

def _scrape_via_html(session: requests.Session) -> list[dict]:
    logger.info("TC: método 3 — HTML directo da página de programação")
    for prog_url in PROGRAMME_URLS:
        resp = _get(session, prog_url)
        if not resp:
            continue
        soup = BeautifulSoup(resp.text, "lxml")
        event_links = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            full = href if href.startswith("http") else f"{WEBSITE}{href}"
            if EVENT_URL_PATTERN.search(full):
                event_links.add(full)
        if not event_links:
            continue
        logger.info(f"TC HTML: {len(event_links)} links em {prog_url}")
        events = []
        for url in sorted(event_links):
            ev = _parse_event_page(url, session)
            if ev:
                events.append(ev)
            time.sleep(REQUEST_DELAY)
        return events
    logger.error("TC HTML: todas as URLs de programação falharam")
    return []


# ---------------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------------

def run(start_date: Optional[str] = None) -> list[dict]:
    session   = _make_session()
    start_str = start_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Método 1: API (raramente funciona no TC)
    raw = _scrape_via_api(start_str, session)
    if raw:
        return [_parse_api_event(e) for e in raw]

    # Método 2: Sitemap + JSON-LD
    events = _scrape_via_sitemap(session)
    if events:
        return events

    # Método 3: HTML directo
    return _scrape_via_html(session)


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    events = run()
    print(f"\nTotal: {len(events)} eventos")
    if events:
        print(_json.dumps(events[0], indent=2, ensure_ascii=False))
