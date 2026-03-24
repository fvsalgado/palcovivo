"""
Primeira Plateia — Scraper Teatro Nacional D. Maria II (Lisboa)
Venue: TNDM | tndm.pt

Infraestrutura: site custom (não WordPress/The Events Calendar)
URL de espetáculos: /pt/programacao/espetaculos/{slug}/

Estratégia de fallback em cascata:
  1. Sitemap Yoast  →  /sitemap_index.xml  →  sub-sitemap de espetáculos
                       filtra /programacao/espetaculos/ e faz parse HTML de cada URL
  2. Página HTML   →  /pt/programacao/espetaculos/  (listagem)
                       extrai links e faz parse de cada página
  3. Outras secções → /pt/programacao/participacao/, /pt/programacao/oficinas-e-formacao/
                       idem

Notas técnicas:
  - SSL inválido → verify=False com warning suprimido
  - Site bilingue PT/EN → preferir URLs /pt/
  - Datas em texto português ("27 mar - 18 abr 2026")
  - JSON-LD schema.org Event presente em algumas páginas
"""

import re
import ssl
import time
import logging
import warnings
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Optional

import requests
import urllib3
from bs4 import BeautifulSoup

# Suprimir avisos de SSL inválido — o tndm.pt tem certificado problemático
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", message="Unverified HTTPS request")

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
VENUE_ID    = "tndm"
SCRAPER_ID  = "tndm"
WEBSITE     = "https://www.tndm.pt"

# Método 1 — Sitemap Yoast
SITEMAP_INDEX_URL = f"{WEBSITE}/sitemap_index.xml"
SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

# Padrões de URL de evento (PT e EN)
EVENT_URL_PATTERNS = [
    re.compile(r"/pt/programacao/espetaculos/[^/]+/?$"),
    re.compile(r"/pt/programacao/participacao/[^/]+/?$"),
    re.compile(r"/pt/programacao/livros-e-pensamento/[^/]+/?$"),
    re.compile(r"/pt/programacao/oficinas-e-formacao/[^/]+/?$"),
    re.compile(r"/pt/programacao/toda-a-programacao/[^/]+/?$"),
    re.compile(r"/en/programme/[^/]+/?$"),
]

# Método 2 — Páginas de listagem HTML
LISTING_URLS = [
    f"{WEBSITE}/pt/programacao/espetaculos/",
    f"{WEBSITE}/pt/programacao/participacao/",
    f"{WEBSITE}/pt/programacao/livros-e-pensamento/",
    f"{WEBSITE}/pt/programacao/oficinas-e-formacao/",
]

REQUEST_DELAY = 1.5
TIMEOUT       = 30

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-PT,pt;q=0.9,en;q=0.8",
    "Referer": f"{WEBSITE}/",
}

# Meses em português para parsing de datas
MONTH_PT = {
    "jan": "01", "fev": "02", "mar": "03", "abr": "04",
    "mai": "05", "jun": "06", "jul": "07", "ago": "08",
    "set": "09", "out": "10", "nov": "11", "dez": "12",
    "janeiro": "01", "fevereiro": "02", "março": "03", "abril": "04",
    "maio": "05", "junho": "06", "julho": "07", "agosto": "08",
    "setembro": "09", "outubro": "10", "novembro": "11", "dezembro": "12",
}


# ---------------------------------------------------------------------------
# SESSION  — SSL verify=False para tolerar certificado inválido do TNDM
# ---------------------------------------------------------------------------

def _make_session() -> requests.Session:
    session = requests.Session()
    session.verify = False  # TNDM tem certificado com cadeia incompleta
    session.headers.update(HEADERS)
    return session


def _get(session: requests.Session, url: str, timeout: int = TIMEOUT) -> Optional[requests.Response]:
    try:
        resp = session.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp
    except requests.exceptions.Timeout:
        logger.warning(f"TNDM: timeout — {url}")
        return None
    except requests.exceptions.HTTPError as e:
        logger.warning(f"TNDM: HTTP {e.response.status_code} — {url}")
        return None
    except requests.exceptions.RequestException as e:
        logger.warning(f"TNDM: erro — {url}: {e}")
        return None


# ---------------------------------------------------------------------------
# PARSING DE DATAS TNDM
# Formatos encontrados no site:
#   "27 mar - 18 abr 2026"
#   "27 de março de 2026"
#   "27 mar 2026 • 21h30"
#   "27 mar 2026 a 18 abr 2026"
# ---------------------------------------------------------------------------

def _parse_tndm_date_range(text: str) -> list[dict]:
    """
    Tenta extrair período de um texto de data TNDM.
    Retorna lista de sessões (normalmente 1 entrada com date_open/date_close).
    """
    if not text:
        return []

    text = text.strip().lower()
    year_now = datetime.now(timezone.utc).year

    # Extrair hora se presente
    time_match = re.search(r"(\d{1,2})h(\d{2})?", text)
    time_str = None
    if time_match:
        h  = time_match.group(1).zfill(2)
        mi = time_match.group(2) or "00"
        time_str = f"{h}:{mi}"

    def parse_single_date(s: str) -> Optional[str]:
        """Extrai YYYY-MM-DD de um fragmento de texto."""
        s = s.strip()
        # "27 mar 2026" ou "27 de março de 2026"
        m = re.match(
            r"(\d{1,2})\s+(?:de\s+)?([a-záéíóúâêôãõç]+)(?:\s+(?:de\s+)?(\d{4}))?",
            s
        )
        if m:
            day   = m.group(1).zfill(2)
            month = MONTH_PT.get(m.group(2)[:3])
            year  = m.group(3) or str(year_now)
            if month:
                return f"{year}-{month}-{day}"
        return None

    # Tentar separadores de período: " - ", " a ", " até "
    period_match = re.split(r"\s+(?:-|a|até)\s+", text, maxsplit=1)
    if len(period_match) == 2:
        d1 = parse_single_date(period_match[0])
        d2 = parse_single_date(period_match[1])
        if d1:
            return [{
                "date":             d1,
                "time_start":       time_str,
                "time_end":         None,
                "duration_minutes": None,
                "is_cancelled":     False,
                "is_sold_out":      False,
                "notes":            f"Até {d2}" if d2 else None,
                "_date_close":      d2,
            }]

    # Data única
    d = parse_single_date(text)
    if d:
        return [{
            "date":             d,
            "time_start":       time_str,
            "time_end":         None,
            "duration_minutes": None,
            "is_cancelled":     False,
            "is_sold_out":      False,
            "notes":            None,
        }]

    return []


# ---------------------------------------------------------------------------
# PARSE DE PÁGINA INDIVIDUAL DE ESPETÁCULO
# ---------------------------------------------------------------------------

def _parse_event_page(url: str, session: requests.Session) -> Optional[dict]:
    """Faz parse de uma página individual de espetáculo do TNDM."""
    resp = _get(session, url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "lxml")

    # ── Título ──
    title = ""
    for sel in [
        "h1.show-title", "h1.espetaculo-title", "h1.production-title",
        ".page-header h1", "main h1", "h1"
    ]:
        el = soup.select_one(sel)
        if el:
            title = el.get_text(strip=True)
            break
    if not title:
        # Fallback: og:title
        og = soup.find("meta", property="og:title")
        if og:
            title = og.get("content", "").strip()
    if not title:
        return None

    # Remover sufixo " - Teatro Nacional D. Maria II" se presente
    title = re.sub(r"\s*[-–]\s*Teatro Nacional D\. Maria II\s*$", "", title).strip()

    # ── Datas ──
    dates = []
    date_close = None

    # Tentar JSON-LD primeiro (mais estruturado)
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            import json as _json
            data = _json.loads(script.string or "")
            if isinstance(data, list):
                data = data[0]
            if isinstance(data, dict) and data.get("@type") == "Event":
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
                    date_close = end[:10] if end else None
        except Exception:
            pass

    # Tentar selectores de data TNDM se JSON-LD não encontrou
    if not dates:
        for sel in [
            ".show-dates", ".espetaculo-dates", ".dates",
            ".date-range", ".production-dates", ".show-info .date",
            "[class*='date']", "[class*='data']",
        ]:
            el = soup.select_one(sel)
            if el:
                date_text = el.get_text(strip=True)
                parsed = _parse_tndm_date_range(date_text)
                if parsed:
                    date_close = parsed[0].pop("_date_close", None)
                    dates = parsed
                    break

    # ── Descrição ──
    desc = ""
    for sel in [
        ".show-description", ".espetaculo-description", ".production-description",
        ".show-content", ".entry-content", "main .content",
        "article .description", ".synopsis",
    ]:
        el = soup.select_one(sel)
        if el:
            desc = el.get_text(separator="\n", strip=True)
            break
    if not desc:
        # Fallback: og:description
        og_desc = soup.find("meta", property="og:description")
        if og_desc:
            desc = og_desc.get("content", "").strip()

    # ── Imagem ──
    cover = None
    og_img = soup.find("meta", property="og:image")
    if og_img:
        cover = og_img.get("content")
    if not cover:
        for sel in [
            ".show-image img", ".espetaculo-image img",
            ".production-image img", ".show-header img", "main img"
        ]:
            el = soup.select_one(sel)
            if el and el.get("src"):
                src = el["src"]
                cover = src if src.startswith("http") else f"{WEBSITE}{src}"
                break

    # ── Preço / Bilhetes ──
    price_raw = ""
    ticketing_url = None
    for sel in [
        ".show-price", ".price", ".ticket-price",
        "[class*='preco']", "[class*='price']", "[class*='bilhete']"
    ]:
        el = soup.select_one(sel)
        if el:
            price_raw = el.get_text(strip=True)
            break
    # Procurar link de bilhetes
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text_a = a.get_text(strip=True).lower()
        if any(kw in text_a for kw in ["bilhete", "comprar", "reservar", "ticket", "buy"]):
            ticketing_url = href if href.startswith("http") else f"{WEBSITE}{href}"
            break

    # ── Categorias ──
    cats = []
    # Inferir categoria a partir da URL
    if "/espetaculos/" in url:
        cats.append("Espetáculos")
    elif "/participacao/" in url:
        cats.append("Participação")
    elif "/livros-e-pensamento/" in url:
        cats.append("Livros e Pensamento")
    elif "/oficinas-e-formacao/" in url:
        cats.append("Oficinas e Formação")
    # Tentar extrair da página
    for sel in [".show-category", ".category", ".tipo", "[class*='category']"]:
        for el in soup.select(sel):
            t = el.get_text(strip=True)
            if t and t not in cats:
                cats.append(t)

    # ── Ficha técnica / elenco ──
    credits_raw = ""
    for sel in [
        ".show-credits", ".ficha-tecnica", ".credits",
        ".show-team", ".elenco", "[class*='ficha']", "[class*='credit']"
    ]:
        el = soup.select_one(sel)
        if el:
            credits_raw = el.get_text(separator="\n", strip=True)
            break

    # ── Acessibilidade ──
    full_text = (desc + " " + credits_raw + " " + soup.get_text()).lower()
    accessibility = {
        "has_sign_language":      "lgp" in full_text or "língua gestual" in full_text,
        "has_audio_description":  "audiodescri" in full_text or "áudio descri" in full_text,
        "has_subtitles":          re.search(r"legenda[sd]?", full_text) is not None,
        "is_relaxed_performance": "sessão relaxada" in full_text or "relaxed" in full_text,
        "wheelchair_accessible":  True,  # TNDM tem acessibilidade geral
        "notes":                  None,
    }

    # ── Espaço ──
    space_id = None
    space_text = full_text
    if "sala garrett"    in space_text: space_id = "sala-garrett"
    elif "sala estúdio"  in space_text: space_id = "sala-estudio"
    elif "sala estudio"  in space_text: space_id = "sala-estudio"
    elif "grande salão"  in space_text: space_id = "grande-salao"
    elif "sala D"        in space_text: space_id = "sala-d"

    # ── Audience ──
    audience_raw = ""
    for sel in [".age-rating", ".classificacao", "[class*='idade']", "[class*='age']"]:
        el = soup.select_one(sel)
        if el:
            audience_raw = el.get_text(strip=True)
            break
    if not audience_raw:
        # Procurar padrão M/NN no texto
        m = re.search(r"M[/\s]?(\d+)\s*anos?", resp.text, re.IGNORECASE)
        if m:
            audience_raw = f"M/{m.group(1)} anos"

    return {
        "source_id":    url.rstrip("/").split("/")[-1],
        "source_url":   url,
        "title":        title,
        "subtitle":     None,
        "description":  desc,
        "categories":   cats,
        "tags":         [],
        "dates":        dates,
        "date_open":    dates[0]["date"] if dates else None,
        "date_close":   date_close,
        "is_ongoing":   bool(date_close),
        "price_raw":    price_raw,
        "ticketing_url": ticketing_url,
        "audience":     audience_raw,
        "cover_image":  cover,
        "space_id":     space_id,
        "credits_raw":  credits_raw,
        "accessibility": accessibility,
        "scraped_at":   datetime.now(timezone.utc).isoformat(),
        "_method":      "sitemap",
    }


# ---------------------------------------------------------------------------
# MÉTODO 1 — SITEMAP YOAST
# Yoast gera sitemap_index.xml com sub-sitemaps por tipo de conteúdo.
# Procuramos o sub-sitemap que contém URLs de espetáculos.
# ---------------------------------------------------------------------------

def _is_event_url(url: str) -> bool:
    return any(p.search(url) for p in EVENT_URL_PATTERNS)


def _fetch_sitemap_urls(session: requests.Session) -> list[str]:
    """Lê sitemap_index.xml e extrai todas as URLs de eventos."""

    logger.info(f"TNDM: a ler sitemap index — {SITEMAP_INDEX_URL}")
    resp = _get(session, SITEMAP_INDEX_URL)
    if not resp:
        return []

    try:
        root = ET.fromstring(resp.text)
    except ET.ParseError as e:
        logger.warning(f"TNDM Sitemap: erro XML no index: {e}")
        return []

    # Encontrar sub-sitemaps — Yoast usa <sitemapindex>/<sitemap>/<loc>
    sub_sitemaps = (
        root.findall("sm:sitemap/sm:loc", SITEMAP_NS)
        or root.findall("{http://www.sitemaps.org/schemas/sitemap/0.9}sitemap/{http://www.sitemaps.org/schemas/sitemap/0.9}loc")
        or root.findall("sitemap/loc")
    )

    if not sub_sitemaps:
        # Pode ser um sitemap simples (não index)
        logger.info("TNDM Sitemap: não é um index, a tentar como sitemap simples")
        return _extract_event_urls_from_sitemap(resp.text)

    logger.info(f"TNDM Sitemap: {len(sub_sitemaps)} sub-sitemaps encontrados")

    event_urls = []
    for loc_el in sub_sitemaps:
        sub_url = loc_el.text.strip() if loc_el.text else ""
        if not sub_url:
            continue

        # Priorizar sub-sitemaps com "espetaculo", "programacao", "post", "page" no nome
        priority_keywords = ["espetaculo", "programacao", "show", "post", "page-sitemap"]
        is_priority = any(kw in sub_url.lower() for kw in priority_keywords)
        # Ignorar sub-sitemaps claramente irrelevantes
        skip_keywords = ["image", "video", "news", "author", "tag", "category", "taxonomy"]
        if any(kw in sub_url.lower() for kw in skip_keywords):
            logger.debug(f"TNDM Sitemap: a ignorar {sub_url}")
            continue

        sub_resp = _get(session, sub_url)
        if not sub_resp:
            continue

        urls = _extract_event_urls_from_sitemap(sub_resp.text)
        if urls:
            logger.info(f"TNDM Sitemap: {len(urls)} URLs em {sub_url}")
            event_urls.extend(urls)
        time.sleep(0.5)

    # Deduplicar mantendo ordem
    seen = set()
    unique = []
    for u in event_urls:
        if u not in seen:
            seen.add(u)
            unique.append(u)

    logger.info(f"TNDM Sitemap: total {len(unique)} URLs de eventos únicas")
    return unique


def _extract_event_urls_from_sitemap(xml_text: str) -> list[str]:
    """Extrai URLs de eventos de um ficheiro sitemap XML."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    # Tentar diferentes namespaces
    locs = (
        root.findall("sm:url/sm:loc", SITEMAP_NS)
        or root.findall("{http://www.sitemaps.org/schemas/sitemap/0.9}url/{http://www.sitemaps.org/schemas/sitemap/0.9}loc")
        or root.findall("url/loc")
    )

    urls = []
    for loc in locs:
        url = loc.text.strip() if loc.text else ""
        if url and _is_event_url(url):
            urls.append(url)
    return urls


def _scrape_via_sitemap(session: requests.Session) -> list[dict]:
    """Recolhe eventos via sitemap Yoast."""
    logger.info("TNDM: método 1 — Sitemap Yoast")
    event_urls = _fetch_sitemap_urls(session)

    if not event_urls:
        logger.warning("TNDM Sitemap: sem URLs de eventos — a tentar fallback")
        return []

    events = []
    for i, url in enumerate(event_urls):
        event = _parse_event_page(url, session)
        if event:
            events.append(event)
        if (i + 1) % 10 == 0:
            logger.info(f"TNDM Sitemap: {i+1}/{len(event_urls)} páginas processadas ({len(events)} eventos)")
        time.sleep(REQUEST_DELAY)

    logger.info(f"TNDM Sitemap: {len(events)} eventos recolhidos")
    return events


# ---------------------------------------------------------------------------
# MÉTODO 2 — HTML DAS PÁGINAS DE LISTAGEM
# ---------------------------------------------------------------------------

def _collect_event_links_from_listing(url: str, session: requests.Session) -> set[str]:
    """Extrai links de eventos de uma página de listagem."""
    resp = _get(session, url)
    if not resp:
        return set()

    soup = BeautifulSoup(resp.text, "lxml")
    links = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        full_url = href if href.startswith("http") else f"{WEBSITE}{href}"
        if _is_event_url(full_url):
            links.add(full_url)

    return links


def _scrape_via_html(session: requests.Session) -> list[dict]:
    """Recolhe eventos via parse HTML das páginas de listagem."""
    logger.info("TNDM: método 2 — HTML das páginas de listagem")

    all_links: set[str] = set()
    for listing_url in LISTING_URLS:
        links = _collect_event_links_from_listing(listing_url, session)
        logger.info(f"TNDM HTML: {len(links)} links em {listing_url}")
        all_links.update(links)
        time.sleep(REQUEST_DELAY)

    if not all_links:
        logger.warning("TNDM HTML: sem links encontrados")
        return []

    logger.info(f"TNDM HTML: total {len(all_links)} URLs únicas")

    events = []
    for i, url in enumerate(sorted(all_links)):
        event = _parse_event_page(url, session)
        if event:
            event["_method"] = "html"
            events.append(event)
        if (i + 1) % 10 == 0:
            logger.info(f"TNDM HTML: {i+1}/{len(all_links)} páginas processadas")
        time.sleep(REQUEST_DELAY)

    logger.info(f"TNDM HTML: {len(events)} eventos recolhidos")
    return events


# ---------------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------------

def run(start_date: Optional[str] = None) -> list[dict]:
    """
    Ponto de entrada do scraper TNDM.
    Cascata: Sitemap Yoast → HTML de listagem
    """
    session = _make_session()

    # Método 1 — Sitemap
    events = _scrape_via_sitemap(session)
    if events:
        return events

    # Método 2 — HTML
    events = _scrape_via_html(session)
    if events:
        return events

    logger.error("TNDM: todos os métodos falharam — sem eventos recolhidos")
    return []


if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO)
    events = run()
    if events:
        print(json.dumps(events[0], indent=2, ensure_ascii=False))
    print(f"\nTotal: {len(events)} eventos | Método: {events[0].get('_method') if events else 'N/A'}")
