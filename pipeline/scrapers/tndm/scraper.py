"""
Primeira Plateia — Scraper Teatro Nacional D. Maria II (Lisboa)
Venue: TNDM | tndm.pt

Infraestrutura: site custom (não WordPress/The Events Calendar)
URLs de espetáculos: /pt/programacao/espetaculos/{slug}/

Estratégia:
  1. Agenda geral  →  /pt/programacao/agenda-geral/
                      Lista todas as datas de todos os espetáculos.
                      É a fonte mais completa e estruturada.
  2. Sitemap Yoast →  /sitemap_index.xml  (403 bloqueado — fallback)
  3. Listagens HTML → /pt/programacao/espetaculos/ etc.
                      Encontra URLs de espetáculos, visita cada uma.

Notas técnicas:
  - SSL inválido → verify=False
  - Datas em PT: "28 MAR", "27 mar - 18 abr 2026", "SET 2025 - JUL 2026"
  - Sitemap bloqueado com 403 — não usar como método primário
  - Espetáculos itinerantes: mesma página, múltiplas datas em vários locais
"""

import re
import time
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
VENUE_ID = "tndm"
SCRAPER_ID = "tndm"
WEBSITE  = "https://www.tndm.pt"

AGENDA_URL = f"{WEBSITE}/pt/programacao/agenda-geral/"

LISTING_URLS = [
    f"{WEBSITE}/pt/programacao/espetaculos/",
    f"{WEBSITE}/pt/programacao/participacao/",
    f"{WEBSITE}/pt/programacao/livros-e-pensamento/",
    f"{WEBSITE}/pt/programacao/oficinas-e-formacao/",
]

EVENT_URL_PATTERNS = [
    re.compile(r"/pt/programacao/espetaculos/[^/]+/?$"),
    re.compile(r"/pt/programacao/participacao/[^/]+/?$"),
    re.compile(r"/pt/programacao/livros-e-pensamento/[^/]+/?$"),
    re.compile(r"/pt/programacao/oficinas-e-formacao/[^/]+/?$"),
    re.compile(r"/pt/programacao/toda-a-programacao/[^/]+/?$"),
]

REQUEST_DELAY = 1.5
TIMEOUT = 30

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "pt-PT,pt;q=0.9,en;q=0.8",
    "Referer": f"{WEBSITE}/",
}

# Meses PT — abreviados (3 letras) e completos, maiúsculas e minúsculas
MONTH_PT = {
    "jan": "01", "fev": "02", "mar": "03", "abr": "04",
    "mai": "05", "jun": "06", "jul": "07", "ago": "08",
    "set": "09", "out": "10", "nov": "11", "dez": "12",
    "janeiro": "01", "fevereiro": "02", "março": "03", "abril": "04",
    "maio": "05", "junho": "06", "julho": "07", "agosto": "08",
    "setembro": "09", "outubro": "10", "novembro": "11", "dezembro": "12",
}


# ---------------------------------------------------------------------------
# SESSION
# ---------------------------------------------------------------------------

def _make_session() -> requests.Session:
    s = requests.Session()
    s.verify = False  # TNDM tem certificado com cadeia incompleta
    s.headers.update(HEADERS)
    return s


def _get(session: requests.Session, url: str) -> Optional[requests.Response]:
    try:
        resp = session.get(url, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp
    except requests.exceptions.Timeout:
        logger.warning(f"TNDM: timeout — {url}")
    except requests.exceptions.HTTPError as e:
        logger.warning(f"TNDM: HTTP {e.response.status_code} — {url}")
    except requests.exceptions.RequestException as e:
        logger.warning(f"TNDM: erro — {url}: {e}")
    return None


# ---------------------------------------------------------------------------
# PARSING DE DATAS TNDM
# Formatos encontrados:
#   "28 MAR"              → dia + mês abrev (sem ano — inferir ano actual/próximo)
#   "27 mar - 18 abr 2026"→ período com mês abrev e ano no fim
#   "SET 2025 - JUL 2026" → só mês + ano (sem dia)
#   "27 de março de 2026" → completo
#   "5 mar - 12 abr 2026" → minúsculas
# ---------------------------------------------------------------------------

def _infer_year(month_num: str) -> int:
    """Se não há ano no texto, inferir: se o mês já passou usa próximo ano."""
    now = datetime.now()
    m = int(month_num)
    if m < now.month:
        return now.year + 1
    return now.year


def _parse_date_fragment(fragment: str) -> Optional[str]:
    """
    Tenta converter um fragmento de texto numa data ISO YYYY-MM-DD.
    Aceita: "28 MAR", "27 mar", "27 mar 2026", "27 de março de 2026"
    """
    f = fragment.strip().lower()
    f = re.sub(r"\bde\b", " ", f)
    f = re.sub(r"\s+", " ", f).strip()

    # Padrão: DD MMMM [YYYY]
    m = re.match(r"(\d{1,2})\s+([a-záéíóúâêôãõç]+)(?:\s+(\d{4}))?$", f)
    if m:
        day   = m.group(1).zfill(2)
        month_str = m.group(2)[:3]
        month = MONTH_PT.get(month_str)
        if not month:
            return None
        year  = m.group(3) or str(_infer_year(month))
        return f"{year}-{month}-{day}"

    # Padrão: MMMM YYYY (só mês + ano, sem dia — usar dia 1)
    m = re.match(r"([a-záéíóúâêôãõç]+)\s+(\d{4})$", f)
    if m:
        month_str = m.group(1)[:3]
        month = MONTH_PT.get(month_str)
        if month:
            return f"{m.group(2)}-{month}-01"

    return None


def _parse_tndm_dates(text: str) -> list[dict]:
    """
    Converte texto de data TNDM para lista de sessões.
    Trata períodos ("27 mar - 18 abr 2026") e datas únicas ("28 MAR").
    """
    if not text:
        return []

    t = text.strip()

    # Extrair hora se presente: "21h30", "19h", "16h00"
    time_m = re.search(r"(\d{1,2})h(\d{2})?", t)
    time_str = None
    if time_m:
        h  = time_m.group(1).zfill(2)
        mi = time_m.group(2) or "00"
        time_str = f"{h}:{mi}"
        t = t[:time_m.start()].strip()  # remover hora do texto de data

    # Tentar separadores de período: " - ", " a ", " até "
    sep = re.search(r"\s+[-–]\s+|\s+a\s+|\s+até\s+", t)
    if sep:
        part1 = t[:sep.start()].strip()
        part2 = t[sep.end():].strip()

        # Se part2 tem ano mas part1 não, inferir para part1
        year_in_2 = re.search(r"\d{4}", part2)
        if year_in_2 and not re.search(r"\d{4}", part1):
            year = year_in_2.group()
            if not re.search(r"\d{4}", part1):
                part1 = f"{part1} {year}"

        d1 = _parse_date_fragment(part1)
        d2 = _parse_date_fragment(part2)

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
    d = _parse_date_fragment(t)
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
# MÉTODO 1 — AGENDA GERAL
# A página /pt/programacao/agenda-geral/ lista TODOS os eventos
# com datas estruturadas em texto visível. É a fonte mais completa.
# ---------------------------------------------------------------------------

def _scrape_via_agenda(session: requests.Session) -> list[dict]:
    """
    Raspa a agenda geral do TNDM.
    Cada entrada tem: data, título, local, link para página individual.
    Agrupa múltiplas datas do mesmo espetáculo.
    """
    logger.info(f"TNDM: método 1 — Agenda geral ({AGENDA_URL})")
    resp = _get(session, AGENDA_URL)
    if not resp:
        logger.warning("TNDM Agenda: inacessível")
        return []

    soup = BeautifulSoup(resp.text, "lxml")

    # Estrutura esperada da agenda TNDM:
    # Cada item tem: data texto + título + link para espetáculo
    # Tentar vários selectores possíveis
    events_by_url: dict[str, dict] = {}

    # Procurar todos os links para espetáculos
    for a in soup.find_all("a", href=True):
        href = a["href"]
        full_url = href if href.startswith("http") else f"{WEBSITE}{href}"
        if not any(p.search(full_url) for p in EVENT_URL_PATTERNS):
            continue

        # Título — texto do link ou elemento próximo
        title = a.get_text(strip=True)
        if not title or len(title) < 3:
            continue

        # Data — procurar no elemento pai ou irmãos
        date_text = ""
        parent = a.parent
        for _ in range(4):  # subir até 4 níveis
            if not parent:
                break
            # Procurar texto com padrão de data
            text = parent.get_text(" ", strip=True)
            date_candidates = re.findall(
                r"\d{1,2}\s+[a-záéíóúâêôãõçA-Z]{3,}(?:\s+\d{4})?|"
                r"[a-záéíóúâêôãõçA-Z]{3,}\s+\d{4}",
                text
            )
            if date_candidates:
                date_text = date_candidates[0]
                break
            parent = parent.parent

        # Agrupar por URL do espetáculo
        if full_url not in events_by_url:
            events_by_url[full_url] = {
                "source_url": full_url,
                "title": title,
                "dates_raw": [],
            }
        if date_text and date_text not in events_by_url[full_url]["dates_raw"]:
            events_by_url[full_url]["dates_raw"].append(date_text)

    logger.info(f"TNDM Agenda: {len(events_by_url)} espetáculos encontrados")

    if not events_by_url:
        return []

    # Visitar cada página de espetáculo para dados completos
    events = []
    for i, (url, base_data) in enumerate(events_by_url.items()):
        event = _parse_event_page(url, session, base_data.get("dates_raw", []))
        if event:
            events.append(event)
        if (i + 1) % 5 == 0:
            logger.info(f"TNDM Agenda: {i+1}/{len(events_by_url)} processados")
        time.sleep(REQUEST_DELAY)

    logger.info(f"TNDM Agenda: {len(events)} eventos recolhidos")
    return events


# ---------------------------------------------------------------------------
# PARSE DE PÁGINA INDIVIDUAL
# ---------------------------------------------------------------------------

def _is_event_url(url: str) -> bool:
    return any(p.search(url) for p in EVENT_URL_PATTERNS)


def _parse_event_page(url: str, session: requests.Session,
                      dates_raw_hint: list[str] = None) -> Optional[dict]:
    """Faz parse de uma página individual de espetáculo do TNDM."""
    resp = _get(session, url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "lxml")
    full_text_lower = soup.get_text(" ", strip=True).lower()

    # ── Título ──
    title = ""
    for sel in ["h1.show-title", "h1.espetaculo-title", ".page-header h1", "main h1", "h1"]:
        el = soup.select_one(sel)
        if el:
            title = el.get_text(strip=True)
            break
    if not title:
        og = soup.find("meta", property="og:title")
        if og:
            title = og.get("content", "").strip()
    if not title:
        return None
    title = re.sub(r"\s*[-–]\s*Teatro Nacional D\. Maria II\s*$", "", title).strip()

    # ── Datas ──
    dates = []
    date_close = None

    # JSON-LD primeiro (mais fiável)
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            import json as _j
            data = _j.loads(script.string or "")
            if isinstance(data, list): data = data[0]
            if isinstance(data, dict) and data.get("@type") == "Event":
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
                    date_close = end[:10] if end else None
        except Exception:
            pass

    # Selectores HTML específicos do TNDM
    if not dates:
        for sel in [
            ".show-date", ".event-date", ".data-espetaculo",
            ".dates", ".date", "[class*='date']", "[class*='data']",
            ".show-info", ".event-info", ".schedule",
        ]:
            for el in soup.select(sel):
                text = el.get_text(strip=True)
                if text:
                    parsed = _parse_tndm_dates(text)
                    if parsed:
                        date_close = parsed[0].pop("_date_close", None)
                        dates = parsed
                        break
            if dates:
                break

    # Procurar no texto geral da página com regex
    if not dates:
        # Padrão: "27 mar - 18 abr 2026 •" ou "28 MAR •"
        date_patterns = [
            r"\b(\d{1,2}\s+[a-záéíóúA-Z]{3,}(?:\s*[-–]\s*\d{1,2}\s+[a-záéíóúA-Z]{3,})?\s+\d{4})\b",
            r"\b(\d{1,2}\s+[A-Z]{3})\b",
        ]
        page_text = soup.get_text(" ", strip=True)
        for pattern in date_patterns:
            m = re.search(pattern, page_text)
            if m:
                parsed = _parse_tndm_dates(m.group(1))
                if parsed:
                    date_close = parsed[0].pop("_date_close", None)
                    dates = parsed
                    break

    # Usar hint da agenda se ainda sem datas
    if not dates and dates_raw_hint:
        for raw in dates_raw_hint:
            parsed = _parse_tndm_dates(raw)
            if parsed:
                date_close = parsed[0].pop("_date_close", None)
                dates = parsed
                break

    # ── Descrição ──
    desc = ""
    for sel in [".show-description", ".event-description", ".entry-content",
                "main .content", ".synopsis", "article p"]:
        el = soup.select_one(sel)
        if el:
            desc = el.get_text(separator="\n", strip=True)
            if len(desc) > 50:
                break
    if not desc:
        og_desc = soup.find("meta", property="og:description")
        if og_desc:
            desc = og_desc.get("content", "").strip()

    # ── Imagem ──
    cover = None
    og_img = soup.find("meta", property="og:image")
    if og_img:
        cover = og_img.get("content")
    if not cover:
        for sel in ["main img", ".show-image img", ".event-image img"]:
            el = soup.select_one(sel)
            if el and el.get("src"):
                src = el["src"]
                cover = src if src.startswith("http") else f"{WEBSITE}{src}"
                break

    # ── Categorias ──
    cats = []
    if "/espetaculos/" in url:          cats.append("Espetáculos")
    elif "/participacao/" in url:       cats.append("Participação")
    elif "/livros-e-pensamento/" in url: cats.append("Livros e Pensamento")
    elif "/oficinas-e-formacao/" in url: cats.append("Oficinas e Formação")

    # ── Preço ──
    price_raw = ""
    for sel in [".price", ".ticket-price", "[class*='preco']", "[class*='price']"]:
        el = soup.select_one(sel)
        if el:
            price_raw = el.get_text(strip=True)
            break
    # Detectar "Entrada livre"
    if re.search(r"entrada\s+livre|gratuito|free", full_text_lower):
        price_raw = price_raw or "Entrada livre"

    # ── Bilheteira ──
    ticketing_url = None
    for a in soup.find_all("a", href=True):
        text_a = a.get_text(strip=True).lower()
        if any(kw in text_a for kw in ["bilhete", "comprar", "reservar", "ticket"]):
            h = a["href"]
            ticketing_url = h if h.startswith("http") else f"{WEBSITE}{h}"
            break

    # ── Créditos ──
    credits_raw = ""
    for sel in [".credits", ".ficha-tecnica", ".show-credits", "[class*='ficha']"]:
        el = soup.select_one(sel)
        if el:
            credits_raw = el.get_text(separator="\n", strip=True)
            break

    # ── Acessibilidade ──
    accessibility = {
        "has_sign_language":      "lgp" in full_text_lower or "língua gestual" in full_text_lower,
        "has_audio_description":  "audiodescri" in full_text_lower or "áudio descri" in full_text_lower,
        "has_subtitles":          bool(re.search(r"legenda[sd]?", full_text_lower)),
        "is_relaxed_performance": "sessão descontraída" in full_text_lower or "relaxed" in full_text_lower,
        "wheelchair_accessible":  True,
        "notes":                  None,
    }

    # ── Espaço ──
    space_id = None
    if "sala garrett"   in full_text_lower: space_id = "sala-garrett"
    elif "sala estúdio" in full_text_lower: space_id = "sala-estudio"
    elif "sala estudio" in full_text_lower: space_id = "sala-estudio"

    # ── Audience ──
    audience_raw = ""
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
        "_method":      "agenda",
    }


# ---------------------------------------------------------------------------
# MÉTODO 2 — LISTAGENS HTML
# ---------------------------------------------------------------------------

def _scrape_via_html(session: requests.Session) -> list[dict]:
    """Raspa as páginas de listagem e visita cada espetáculo."""
    logger.info("TNDM: método 2 — HTML das páginas de listagem")
    all_links: set[str] = set()

    for listing_url in LISTING_URLS:
        resp = _get(session, listing_url)
        if not resp:
            continue
        soup = BeautifulSoup(resp.text, "lxml")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            full = href if href.startswith("http") else f"{WEBSITE}{href}"
            if _is_event_url(full):
                all_links.add(full)
        logger.info(f"TNDM HTML: {len(all_links)} links após {listing_url}")
        time.sleep(REQUEST_DELAY)

    events = []
    for url in sorted(all_links):
        event = _parse_event_page(url, session)
        if event:
            event["_method"] = "html"
            events.append(event)
        time.sleep(REQUEST_DELAY)

    logger.info(f"TNDM HTML: {len(events)} eventos recolhidos")
    return events


# ---------------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------------

def run(start_date: Optional[str] = None) -> list[dict]:
    """Cascata: Agenda geral → Listagens HTML."""
    session = _make_session()

    events = _scrape_via_agenda(session)
    if events:
        return events

    events = _scrape_via_html(session)
    if events:
        return events

    logger.error("TNDM: todos os métodos falharam")
    return []


if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO)
    events = run()
    if events:
        print(json.dumps(events[0], indent=2, ensure_ascii=False))
    print(f"\nTotal: {len(events)} eventos")
