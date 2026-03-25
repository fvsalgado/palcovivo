"""
Primeira Plateia — Scraper Teatro Nacional D. Maria II (Lisboa)
Venue: TNDM | tndm.pt  —  v5

Problema anterior: agenda-geral só tem 6 links únicos (um por espetáculo).
A fonte correcta é /pt/programacao/toda-a-programacao/ com paginação:
  ?tipo=1&cat=1&p=2, &p=3, etc.

Cada página lista itens com: DATA • LOCAL • TÍTULO • "Saiba mais" (link)
O scraper itera todas as páginas até não encontrar mais itens,
recolhe todos os links únicos de espetáculos, depois visita cada um.

Estratégia:
  1. /pt/programacao/toda-a-programacao/ com paginação completa
  2. Secções individuais (/espetaculos/, /participacao/, etc.) como fallback
"""

import re
import time
import json as _json
import logging
import warnings
from datetime import datetime, timezone
from typing import Optional

import requests
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", message="Unverified HTTPS request")

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
VENUE_ID   = "tndm"
SCRAPER_ID = "tndm"
WEBSITE    = "https://www.tndm.pt"

# Fonte principal — toda a programação com paginação
TODA_BASE  = f"{WEBSITE}/pt/programacao/toda-a-programacao/"
MAX_PAGES  = 20  # segurança — nunca mais de 20 páginas

# Fallback por secção
LISTING_URLS = [
    f"{WEBSITE}/pt/programacao/espetaculos/",
    f"{WEBSITE}/pt/programacao/participacao/",
    f"{WEBSITE}/pt/programacao/livros-e-pensamento/",
    f"{WEBSITE}/pt/programacao/oficinas-e-formacao/",
]

# Padrões de URL de espetáculo válido
EVENT_URL_PATTERNS = [
    re.compile(r"/pt/programacao/espetaculos/[^/?#]+/?$"),
    re.compile(r"/pt/programacao/participacao/[^/?#]+/?$"),
    re.compile(r"/pt/programacao/livros-e-pensamento/[^/?#]+/?$"),
    re.compile(r"/pt/programacao/oficinas-e-formacao/[^/?#]+/?$"),
]

REQUEST_DELAY = 1.5
TIMEOUT       = 30

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "pt-PT,pt;q=0.9,en;q=0.8",
    "Referer":         f"{WEBSITE}/",
}

MONTH_PT = {
    "jan": "01", "fev": "02", "mar": "03", "abr": "04",
    "mai": "05", "jun": "06", "jul": "07", "ago": "08",
    "set": "09", "out": "10", "nov": "11", "dez": "12",
    "janeiro": "01", "fevereiro": "02", "março": "03", "marco": "03",
    "abril": "04", "maio": "05", "junho": "06", "julho": "07",
    "agosto": "08", "setembro": "09", "outubro": "10",
    "novembro": "11", "dezembro": "12",
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
    except requests.exceptions.Timeout:
        logger.warning(f"TNDM: timeout — {url}")
    except requests.exceptions.HTTPError as e:
        logger.warning(f"TNDM: HTTP {e.response.status_code} — {url}")
    except requests.exceptions.RequestException as e:
        logger.warning(f"TNDM: erro — {url}: {e}")
    return None


def _is_event_url(url: str) -> bool:
    return any(p.search(url) for p in EVENT_URL_PATTERNS)


# ---------------------------------------------------------------------------
# PARSING DE DATAS TNDM
#
# Formatos encontrados em toda-a-programacao:
#   "22 MAR"                   → dia mês-abrev maiúsculas
#   "27 mar - 18 abr 2026"     → período minúsculas com ano
#   "9 - 10 MAI"               → período mesmo mês
#   "20 - 24 ABR"              → período mesmo mês
#   "4, 6, 7, 20 - 21 JUN"    → múltiplos dias → usar primeiro
#   "SET 2025 - JUL 2026"      → só mês+ano
# ---------------------------------------------------------------------------

def _infer_year(month_num: str, month_end: str = None) -> str:
    now = datetime.now()
    ref = int(month_end) if month_end else int(month_num)
    # Se o mês de referência já passou, ainda usar ano actual
    # (eventos passados recentes são válidos para o histórico)
    return str(now.year) if ref >= 1 else str(now.year + 1)


def _parse_date_fragment(fragment: str) -> Optional[str]:
    f = fragment.strip().lower()
    f = re.sub(r"\bde\b", " ", f)
    f = re.sub(r"\s+", " ", f).strip()

    # DD MMMM [YYYY]
    m = re.match(r"^(\d{1,2})\s+([a-záéíóúâêôãõç]+)(?:\s+(\d{4}))?$", f)
    if m:
        month = MONTH_PT.get(m.group(2)[:3])
        if month:
            year = m.group(3) or str(datetime.now().year)
            return f"{year}-{month}-{m.group(1).zfill(2)}"

    # MMMM YYYY
    m = re.match(r"^([a-záéíóúâêôãõç]+)\s+(\d{4})$", f)
    if m:
        month = MONTH_PT.get(m.group(1)[:3])
        if month:
            return f"{m.group(2)}-{month}-01"

    return None


def _parse_tndm_date(text: str) -> tuple[Optional[str], Optional[str]]:
    """
    Parseia texto de data TNDM. Retorna (date_first_iso, date_close_iso).
    Formatos:
      "22 MAR"             → data única sem ano
      "27 mar - 18 abr 2026" → período com ano
      "9 - 10 MAI"         → período mesmo mês
      "4, 6, 7, 20 - 21 JUN" → múltiplos → usar primeiro e último
      "SET 2025 - JUL 2026" → mês+ano
    """
    if not text:
        return None, None

    t = text.strip()
    t = re.sub(r"\s+", " ", t).strip()
    now_year = str(datetime.now().year)

    # Período com dois meses diferentes: "27 mar - 18 abr 2026" ou "SET 2025 - JUL 2026"
    m = re.match(
        r"^(\d{1,2})?\s*([a-záéíóúA-Z]+)\s*(?:(\d{4})\s*)?[-–]\s*(\d{1,2})?\s*([a-záéíóúA-Z]+)\s*(?:(\d{4}))?",
        t, re.IGNORECASE
    )
    if m and m.group(2) and m.group(5):
        m1 = MONTH_PT.get(m.group(2).lower()[:3])
        m2 = MONTH_PT.get(m.group(5).lower()[:3])
        if m1 and m2 and m1 != m2:
            y1   = m.group(3) or m.group(6) or now_year
            y2   = m.group(6) or y1
            day1 = (m.group(1) or "01").zfill(2)
            day2 = (m.group(4) or "01").zfill(2)
            return f"{y1}-{m1}-{day1}", f"{y2}-{m2}-{day2}"

    # Período mesmo mês: "9 - 10 MAI" ou "4, 6, 7, 20 - 21 JUN"
    # Extrair todos os números antes do mês e o mês
    month_m = re.search(r"\b([a-záéíóúA-Z]{3,})\b", t)
    if month_m:
        month = MONTH_PT.get(month_m.group(1).lower()[:3])
        if month:
            year_m = re.search(r"\d{4}", t)
            year   = year_m.group() if year_m else now_year
            nums   = re.findall(r"\d+", t[:month_m.start()])
            if nums:
                day_first = nums[0].zfill(2)
                day_last  = nums[-1].zfill(2) if len(nums) > 1 else None
                d1 = f"{year}-{month}-{day_first}"
                d2 = f"{year}-{month}-{day_last}" if day_last and day_last != day_first else None
                return d1, d2

    # Data única com mês por extenso: "22 MAR 2026" ou "28 ABR"
    m = re.match(r"^(\d{1,2})\s+([a-záéíóúA-Z]+)(?:\s+(\d{4}))?$", t)
    if m:
        month = MONTH_PT.get(m.group(2).lower()[:3])
        if month:
            year = m.group(3) or now_year
            return f"{year}-{month}-{m.group(1).zfill(2)}", None

    return None, None


# ---------------------------------------------------------------------------
# MÉTODO 1 — TODA A PROGRAMAÇÃO COM PAGINAÇÃO
# ---------------------------------------------------------------------------

def _extract_links_from_page(soup: BeautifulSoup) -> dict[str, dict]:
    """
    Extrai links de espetáculos e metadados de uma página de listagem.
    Retorna dict {url: {title, date_text, categories}}.
    """
    entries: dict[str, dict] = {}

    # Estratégia: encontrar todos os links "Saiba mais" com href para espetáculo
    for a in soup.find_all("a", href=True):
        href     = a["href"]
        full_url = href if href.startswith("http") else f"{WEBSITE}{href}"

        if not _is_event_url(full_url):
            continue

        # Subir na DOM até encontrar o bloco com data e título
        title     = ""
        date_text = ""
        cats      = []

        node = a
        for _ in range(8):
            node = node.parent
            if not node:
                break
            text = node.get_text(" ", strip=True)

            # Procurar padrão de data TNDM no bloco
            # Padrões: "22 MAR", "27 mar - 18 abr 2026", "SET 2025 - JUL 2026"
            date_m = re.search(
                r"\b(\d{1,2}\s*[-–]\s*\d{1,2}\s+[A-Za-záéíóú]{3,}(?:\s+\d{4})?|"  # "9 - 10 MAI"
                r"\d{1,2}\s+[A-Za-záéíóú]{3,}(?:\s*[-–]\s*\d{1,2}\s+[A-Za-záéíóú]{3,})?(?:\s+\d{4})?|"  # "27 mar - 18 abr 2026"
                r"[A-Z]{3}\s+\d{4}\s*[-–]\s*[A-Z]{3}\s+\d{4})\b",  # "SET 2025 - JUL 2026"
                text
            )
            if date_m and not date_text:
                date_text = date_m.group(0).strip()

            # Procurar título: elemento <h2>, <h3>, <h4> ou strong dentro do bloco
            if not title:
                for tag in node.find_all(["h2", "h3", "h4", "strong"]):
                    t = tag.get_text(strip=True)
                    if len(t) > 3 and "saiba" not in t.lower() and not re.match(r"^\d", t):
                        title = t
                        break

            # Categorias: Espetáculos, Participação, etc.
            for cat in ["Espetáculos", "Participação", "Livros e Pensamento", "Oficinas e Formação",
                        "Infância e Juventude", "Público em geral"]:
                if cat.lower() in text.lower() and cat not in cats:
                    cats.append(cat)

            # Se encontrámos tanto data como título, parar de subir
            if date_text and title:
                break

        # Fallback: inferir título do slug
        if not title:
            slug  = full_url.rstrip("/").split("/")[-1]
            title = slug.replace("-", " ").title()

        if len(title) < 3:
            continue

        if full_url not in entries:
            entries[full_url] = {
                "source_url": full_url,
                "title":      title,
                "date_text":  date_text,
                "categories": cats,
            }
        else:
            # Actualizar com informação mais completa
            if len(title) > len(entries[full_url]["title"]):
                entries[full_url]["title"] = title
            if date_text and not entries[full_url]["date_text"]:
                entries[full_url]["date_text"] = date_text

    return entries


def _scrape_toda_programacao(session: requests.Session) -> list[dict]:
    """
    Raspa toda-a-programacao com paginação completa.
    Parâmetros de paginação: ?tipo=1&cat=1&p=N
    """
    logger.info(f"TNDM: método 1 — Toda a Programação com paginação ({TODA_BASE})")
    all_entries: dict[str, dict] = {}

    # Página 1 (sem parâmetros)
    resp = _get(session, TODA_BASE)
    if not resp:
        logger.warning("TNDM: toda-a-programacao inacessível")
        return []

    soup    = BeautifulSoup(resp.text, "lxml")
    page_entries = _extract_links_from_page(soup)
    all_entries.update(page_entries)
    logger.info(f"TNDM: página 1 — {len(page_entries)} itens ({len(all_entries)} total)")

    # Detectar número total de páginas a partir de links de paginação
    max_page = 1
    for a in soup.find_all("a", href=True):
        href = a["href"]
        pm = re.search(r"[?&]p=(\d+)", href)
        if pm:
            max_page = max(max_page, int(pm.group(1)))

    # Se não detectou paginação, tentar à bruta até página vazia
    if max_page == 1:
        max_page = MAX_PAGES

    logger.info(f"TNDM: máximo de páginas detectado: {max_page}")

    # Iterar páginas 2..N
    for page in range(2, min(max_page + 1, MAX_PAGES + 1)):
        time.sleep(REQUEST_DELAY)
        resp = _get(session, TODA_BASE, params={"tipo": "1", "cat": "1", "p": str(page)})
        if not resp:
            logger.warning(f"TNDM: página {page} inacessível — a parar")
            break

        soup         = BeautifulSoup(resp.text, "lxml")
        page_entries = _extract_links_from_page(soup)

        if not page_entries:
            logger.info(f"TNDM: página {page} sem itens — paginação completa")
            break

        new_count = sum(1 for k in page_entries if k not in all_entries)
        all_entries.update(page_entries)
        logger.info(f"TNDM: página {page} — {len(page_entries)} itens ({new_count} novos, {len(all_entries)} total)")

        # Parar se não há itens novos (loop de paginação)
        if new_count == 0:
            logger.info(f"TNDM: sem itens novos na página {page} — a parar")
            break

    logger.info(f"TNDM: {len(all_entries)} espetáculos únicos encontrados")

    if not all_entries:
        return []

    # Visitar cada página individual
    events  = []
    total   = len(all_entries)
    for i, (url, meta) in enumerate(all_entries.items()):
        ev = _parse_event_page(
            url, session,
            title_hint=meta.get("title"),
            date_hint=meta.get("date_text"),
            cats_hint=meta.get("categories", []),
        )
        if ev:
            events.append(ev)
        if (i + 1) % 5 == 0 or (i + 1) == total:
            logger.info(f"TNDM: {i+1}/{total} páginas processadas ({len(events)} válidos)")
        time.sleep(REQUEST_DELAY)

    logger.info(f"TNDM: {len(events)} eventos recolhidos")
    return events


# ---------------------------------------------------------------------------
# MÉTODO 2 — LISTAGENS POR SECÇÃO (fallback)
# ---------------------------------------------------------------------------

def _scrape_via_listings(session: requests.Session) -> list[dict]:
    logger.info("TNDM: método 2 — listagens por secção")
    all_urls: set[str] = set()

    for listing_url in LISTING_URLS:
        resp = _get(session, listing_url)
        if not resp:
            continue
        soup = BeautifulSoup(resp.text, "lxml")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            full = href if href.startswith("http") else f"{WEBSITE}{href}"
            if _is_event_url(full) and full != listing_url:
                all_urls.add(full)
        logger.info(f"TNDM: {len(all_urls)} links após {listing_url}")
        time.sleep(REQUEST_DELAY)

    events = []
    for url in sorted(all_urls):
        ev = _parse_event_page(url, session)
        if ev:
            events.append(ev)
        time.sleep(REQUEST_DELAY)

    logger.info(f"TNDM listagens: {len(events)} eventos")
    return events


# ---------------------------------------------------------------------------
# PARSE DE PÁGINA INDIVIDUAL
# ---------------------------------------------------------------------------

def _parse_event_page(
    url: str,
    session: requests.Session,
    title_hint: str = None,
    date_hint: str = None,
    cats_hint: list = None,
) -> Optional[dict]:
    resp = _get(session, url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "lxml")
    ft   = soup.get_text(" ", strip=True)
    ftl  = ft.lower()

    # ── Título ──
    title = ""
    for sel in ["h1.show-title", "h1.espetaculo-title", ".page-header h1", "main h1", "h1"]:
        el = soup.select_one(sel)
        if el:
            t = el.get_text(strip=True)
            if len(t) > 2:
                title = t
                break
    if not title:
        og = soup.find("meta", property="og:title")
        if og:
            title = og.get("content", "").strip()
    if not title and title_hint:
        title = title_hint
    if not title:
        return None
    title = re.sub(r"\s*[-–|]\s*Teatro Nacional.*$", "", title, flags=re.IGNORECASE).strip()
    title = re.sub(r"\s*[-–|]\s*TNDM.*$",           "", title, flags=re.IGNORECASE).strip()

    # ── Datas ──
    dates      = []
    date_close = None

    # 1. JSON-LD
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = _json.loads(script.string or "")
            if isinstance(data, list):
                data = next((d for d in data if isinstance(d, dict) and d.get("@type") == "Event"), {})
            if isinstance(data, dict) and data.get("@type") == "Event":
                start = data.get("startDate", "")
                end   = data.get("endDate",   "")
                if start and len(start) >= 10:
                    dates      = [_mdate(start[:10], start[11:16] if len(start) > 10 else None)]
                    date_close = end[:10] if end and len(end) >= 10 else None
                    break
        except Exception:
            pass

    # 2. Selectores HTML
    if not dates:
        for sel in [".show-date", ".event-date", "[class*='date']", "[class*='data']", "time[datetime]"]:
            for el in soup.select(sel):
                raw = el.get("datetime") or el.get_text(strip=True)
                d1, d2 = _parse_tndm_date(raw)
                if d1:
                    dates = [_mdate(d1)]; date_close = d2; break
            if dates:
                break

    # 3. Regex no texto — padrões TNDM
    if not dates:
        for pat in [
            r"(\d{1,2}\s+[a-záéíóúA-Z]{3,}\s*[-–]\s*\d{1,2}\s+[a-záéíóúA-Z]{3,}\s+\d{4})",
            r"(\d{1,2}\s*[-–]\s*\d{1,2}\s+[A-ZÁÉÍÓÚ]{3,}(?:\s+\d{4})?)",
            r"(\d{1,2}\s+[a-záéíóúA-Z]{3,}\s+\d{4})\b",
            r"\b(\d{1,2}\s+[A-ZÁÉÍÓÚ]{3,4})\b",
        ]:
            m = re.search(pat, ft)
            if m:
                d1, d2 = _parse_tndm_date(m.group(1))
                if d1:
                    dates = [_mdate(d1)]; date_close = d2; break

    # 4. Hint da listagem
    if not dates and date_hint:
        d1, d2 = _parse_tndm_date(date_hint)
        if d1:
            dates = [_mdate(d1)]; date_close = d2

    # ── Descrição ──
    desc = ""
    for sel in [".show-description", ".event-description", ".synopsis",
                ".entry-content", ".description", "main p"]:
        el = soup.select_one(sel)
        if el:
            d = el.get_text(separator="\n", strip=True)
            if len(d) > 40:
                desc = d; break
    if not desc:
        og = soup.find("meta", property="og:description")
        if og:
            desc = og.get("content", "").strip()

    # ── Imagem ──
    cover = None
    og_img = soup.find("meta", property="og:image")
    if og_img:
        cover = og_img.get("content")
    if not cover:
        for sel in [".show-image img", ".event-image img", "main img[src]"]:
            el = soup.select_one(sel)
            if el and el.get("src") and not el["src"].endswith((".svg", ".gif")):
                src = el["src"]
                cover = src if src.startswith("http") else f"{WEBSITE}{src}"
                break

    # ── Categorias ──
    cats = cats_hint or []
    if not cats:
        ul = url.lower()
        if "/espetaculos/"           in ul: cats = ["Espetáculos"]
        elif "/participacao/"        in ul: cats = ["Participação"]
        elif "/livros-e-pensamento/" in ul: cats = ["Livros e Pensamento"]
        elif "/oficinas-e-formacao/" in ul: cats = ["Oficinas e Formação"]

    # ── Preço ──
    price_raw = ""
    for sel in [".price", "[class*='preco']", "[class*='price']"]:
        el = soup.select_one(sel)
        if el:
            price_raw = el.get_text(strip=True); break
    if not price_raw and re.search(r"entrada\s+livre|gratuito|acesso\s+livre", ftl):
        price_raw = "Entrada livre"

    # ── Bilheteira ──
    ticketing_url = None
    for a in soup.find_all("a", href=True):
        at = a.get_text(strip=True).lower()
        h  = a["href"]
        if any(k in at for k in ["bilhete", "comprar", "reservar", "ticket"]) or "bol.pt" in h:
            ticketing_url = h if h.startswith("http") else f"{WEBSITE}{h}"
            break

    # ── Créditos ──
    credits_raw = ""
    for sel in [".credits", ".ficha-tecnica", "[class*='ficha']", ".team"]:
        el = soup.select_one(sel)
        if el:
            credits_raw = el.get_text(separator="\n", strip=True); break

    # ── Acessibilidade ──
    accessibility = {
        "has_sign_language":      "lgp" in ftl or "língua gestual" in ftl,
        "has_audio_description":  "audiodescri" in ftl or "audiodescrição" in ftl,
        "has_subtitles":          bool(re.search(r"legenda[sd]?", ftl)),
        "is_relaxed_performance": "sessão descontraída" in ftl or "relaxed" in ftl,
        "wheelchair_accessible":  True,
        "notes":                  None,
    }

    # ── Espaço ──
    space_id = None
    if "sala garrett"          in ftl: space_id = "sala-garrett"
    elif "sala estúdio"        in ftl: space_id = "sala-estudio"
    elif "teatro variedades"   in ftl: space_id = "teatro-variedades"
    elif "jardins do bombarda" in ftl: space_id = "jardins-bombarda"

    # ── Público ──
    audience_raw = ""
    age_m = re.search(r"M[/\s]?(\d+)\s*anos?", resp.text, re.IGNORECASE)
    if age_m:
        audience_raw = f"M/{age_m.group(1)} anos"

    return {
        "source_id":     url.rstrip("/").split("/")[-1],
        "source_url":    url,
        "title":         title,
        "subtitle":      None,
        "description":   desc,
        "categories":    cats,
        "tags":          [],
        "dates":         dates,
        "date_open":     dates[0]["date"] if dates else None,
        "date_close":    date_close,
        "is_ongoing":    bool(date_close and dates and date_close > dates[0]["date"]),
        "price_raw":     price_raw,
        "ticketing_url": ticketing_url,
        "audience":      audience_raw,
        "cover_image":   cover,
        "space_id":      space_id,
        "credits_raw":   credits_raw,
        "accessibility": accessibility,
        "scraped_at":    datetime.now(timezone.utc).isoformat(),
        "_method":       "tndm-v5",
    }


def _mdate(date: str, time_start: str = None) -> dict:
    return {
        "date":             date,
        "time_start":       time_start,
        "time_end":         None,
        "duration_minutes": None,
        "is_cancelled":     False,
        "is_sold_out":      False,
        "notes":            None,
    }


# ---------------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------------

def run(start_date: Optional[str] = None) -> list[dict]:
    session = _make_session()

    # Método 1: toda-a-programacao com paginação
    events = _scrape_toda_programacao(session)
    if events:
        return events

    # Método 2: secções individuais
    logger.warning("TNDM: toda-a-programacao sem resultados — a tentar secções")
    events = _scrape_via_listings(session)
    if events:
        return events

    logger.error("TNDM: todos os métodos falharam")
    return []


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    events = run()
    print(f"\nTotal: {len(events)} eventos")
    if events:
        print(_json.dumps(events[0], indent=2, ensure_ascii=False))
