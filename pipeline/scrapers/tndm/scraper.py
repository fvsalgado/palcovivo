"""
Primeira Plateia — Scraper Teatro Nacional D. Maria II (Lisboa)
Venue: TNDM | tndm.pt  —  v6

Melhorias face à v5:
  - Extracção de TODAS as sessões da tabela .table_tickets (datas, horas, locais)
  - Horários parseados por sessão
  - Locais por sessão (não apenas space_id global)
  - Tags/categorias via .tag-list-item.programacao
  - Ficha técnica via .ficha_tecnica (selector corrigido)
  - Classificação etária: regex M/(\d+) sem exigir "anos"
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

TODA_BASE  = f"{WEBSITE}/pt/programacao/toda-a-programacao/"
MAX_PAGES  = 20

LISTING_URLS = [
    f"{WEBSITE}/pt/programacao/espetaculos/",
    f"{WEBSITE}/pt/programacao/participacao/",
    f"{WEBSITE}/pt/programacao/livros-e-pensamento/",
    f"{WEBSITE}/pt/programacao/oficinas-e-formacao/",
]

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
# PARSING DE DATAS E HORÁRIOS
# ---------------------------------------------------------------------------

def _parse_time(text: str) -> Optional[str]:
    """
    Extrai o primeiro horário de uma string como "11h30 e 16h" ou "11h . 15h30".
    Devolve "HH:MM" ou None.
    """
    m = re.search(r"(\d{1,2})h(\d{0,2})", text)
    if not m:
        return None
    h  = m.group(1).zfill(2)
    mi = m.group(2).zfill(2) if m.group(2) else "00"
    return f"{h}:{mi}"


def _parse_all_times(text: str) -> list[str]:
    """
    Extrai todos os horários de uma string. Ex: "11h30 e 16h" → ["11:30", "16:00"].
    """
    return [
        f"{m.group(1).zfill(2)}:{(m.group(2) or '00').zfill(2)}"
        for m in re.finditer(r"(\d{1,2})h(\d{0,2})", text)
    ]


def _parse_tndm_date(text: str) -> tuple[Optional[str], Optional[str]]:
    """
    Parseia texto de data TNDM. Retorna (date_first_iso, date_close_iso).
    Formatos suportados:
      "22 MAR"                    → data única sem ano
      "27 mar - 18 abr 2026"      → período com dois meses
      "9 - 10 MAI"                → período mesmo mês
      "4, 6, 7, 20 - 21 JUN"     → múltiplos → usar primeiro e último
      "SET 2025 - JUL 2026"       → só mês+ano
      "3 mai 2025"                → data completa minúsculas
    """
    if not text:
        return None, None

    t        = text.strip()
    t        = re.sub(r"\s+", " ", t).strip()
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

    # Data única: "22 MAR 2026" ou "28 ABR" ou "3 mai 2025"
    m = re.match(r"^(\d{1,2})\s+([a-záéíóúA-Z]+)(?:\s+(\d{4}))?$", t)
    if m:
        month = MONTH_PT.get(m.group(2).lower()[:3])
        if month:
            year = m.group(3) or now_year
            return f"{year}-{month}-{m.group(1).zfill(2)}", None

    return None, None


def _expand_date_text(date_text: str, now_year: str) -> list[str]:
    """
    Expande "4, 6, 7, 20 - 21 JUN" em datas ISO individuais.
    Retorna lista de ISO dates.
    Suporta:
      - listas com vírgulas: "3, 5, 17 - 19 JUN"
      - intervalos: "17 - 19 JUN"
      - misto: "4, 6, 7, 20 - 21 JUN"
    """
    t = date_text.strip()

    # Encontrar mês e ano
    month_m = re.search(r"\b([a-záéíóúA-Z]{3,})\b", t)
    if not month_m:
        return []
    month = MONTH_PT.get(month_m.group(1).lower()[:3])
    if not month:
        return []
    year_m = re.search(r"\d{4}", t)
    year   = year_m.group() if year_m else now_year

    # Extrair a parte numérica (antes do mês)
    num_part = t[:month_m.start()].strip().rstrip(",").strip()

    dates = []
    # Splittar por vírgulas, processar cada segmento
    for segment in re.split(r",", num_part):
        seg = segment.strip()
        # Intervalo: "17 - 19" ou "20 - 21"
        range_m = re.match(r"^(\d+)\s*[-–]\s*(\d+)$", seg)
        if range_m:
            start, end = int(range_m.group(1)), int(range_m.group(2))
            for d in range(start, end + 1):
                dates.append(f"{year}-{month}-{str(d).zfill(2)}")
        elif re.match(r"^\d+$", seg):
            dates.append(f"{year}-{month}-{seg.zfill(2)}")

    return dates


# ---------------------------------------------------------------------------
# EXTRACÇÃO DE SESSÕES DA TABELA .table_tickets
# ---------------------------------------------------------------------------

def _parse_sessions_from_table(soup: BeautifulSoup, now_year: str) -> list[dict]:
    """
    Lê a(s) tabela(s) .table_tickets e extrai uma lista de sessões.
    Cada sessão tem: date, time_start, time_end, venue_name, venue_url, notes.
    Ignora a secção "Espetáculo já apresentado em" (datas passadas).
    """
    sessions = []

    # Encontrar apenas blocos de datas futuras (excluir .datas_ultrapassadas)
    for container in soup.select(".datas_detalhe"):
        # Saltar datas ultrapassadas
        if "datas_ultrapassadas" in container.get("class", []):
            continue

        table = container.select_one("table.table_tickets")
        if not table:
            continue

        for row in table.select("tr"):
            wrapper = row.select_one(".evento_datas_wapper")
            if not wrapper:
                continue

            # Data e hora
            data_el = wrapper.select_one(".evento_data_container")
            if not data_el:
                continue

            divs = data_el.find_all("div", recursive=False)
            date_raw  = divs[0].get_text(strip=True) if len(divs) > 0 else ""
            time_raw  = divs[1].get_text(strip=True) if len(divs) > 1 else ""

            if not date_raw:
                continue

            # Local
            local_el  = wrapper.select_one(".evento_local_container")
            venue_name = ""
            venue_url  = None
            if local_el:
                link = local_el.find("a")
                if link:
                    venue_name = link.get_text(strip=True)
                    href = link.get("href", "")
                    if href and href.startswith("http"):
                        venue_url = href
                else:
                    venue_name = local_el.get_text(strip=True)

            # Expandir datas (pode ser "4, 6, 7, 20 - 21 JUN")
            expanded_dates = _expand_date_text(date_raw, now_year)
            if not expanded_dates:
                # Fallback: tentar parse simples
                d1, _ = _parse_tndm_date(date_raw)
                if d1:
                    expanded_dates = [d1]

            if not expanded_dates:
                continue

            # Horários
            times = _parse_all_times(time_raw)
            time_start = times[0] if times else None
            time_end   = times[1] if len(times) > 1 else None

            for iso_date in expanded_dates:
                sessions.append({
                    "date":        iso_date,
                    "time_start":  time_start,
                    "time_end":    time_end,
                    "venue_name":  venue_name or None,
                    "venue_url":   venue_url,
                    "is_cancelled": False,
                    "is_sold_out":  False,
                    "notes":       time_raw if time_raw and not times else None,
                })

    return sessions


# ---------------------------------------------------------------------------
# EXTRACTION DE LINKS (listagem)
# ---------------------------------------------------------------------------

def _extract_links_from_page(soup: BeautifulSoup) -> dict[str, dict]:
    entries: dict[str, dict] = {}

    for a in soup.find_all("a", href=True):
        href     = a["href"]
        full_url = href if href.startswith("http") else f"{WEBSITE}{href}"

        if not _is_event_url(full_url):
            continue

        title     = ""
        date_text = ""
        cats      = []

        node = a
        for _ in range(8):
            node = node.parent
            if not node:
                break
            text = node.get_text(" ", strip=True)

            date_m = re.search(
                r"\b(\d{1,2}\s*[-–]\s*\d{1,2}\s+[A-Za-záéíóú]{3,}(?:\s+\d{4})?|"
                r"\d{1,2}\s+[A-Za-záéíóú]{3,}(?:\s*[-–]\s*\d{1,2}\s+[A-Za-záéíóú]{3,})?(?:\s+\d{4})?|"
                r"[A-Z]{3}\s+\d{4}\s*[-–]\s*[A-Z]{3}\s+\d{4})\b",
                text
            )
            if date_m and not date_text:
                date_text = date_m.group(0).strip()

            if not title:
                for tag in node.find_all(["h2", "h3", "h4", "strong"]):
                    t = tag.get_text(strip=True)
                    if len(t) > 3 and "saiba" not in t.lower() and not re.match(r"^\d", t):
                        title = t
                        break

            for cat in ["Espetáculos", "Participação", "Livros e Pensamento", "Oficinas e Formação",
                        "Infância e Juventude", "Público em geral"]:
                if cat.lower() in text.lower() and cat not in cats:
                    cats.append(cat)

            if date_text and title:
                break

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
            if len(title) > len(entries[full_url]["title"]):
                entries[full_url]["title"] = title
            if date_text and not entries[full_url]["date_text"]:
                entries[full_url]["date_text"] = date_text

    return entries


# ---------------------------------------------------------------------------
# MÉTODO 1 — TODA A PROGRAMAÇÃO COM PAGINAÇÃO
# ---------------------------------------------------------------------------

def _scrape_toda_programacao(session: requests.Session) -> list[dict]:
    logger.info(f"TNDM: método 1 — Toda a Programação com paginação ({TODA_BASE})")
    all_entries: dict[str, dict] = {}

    resp = _get(session, TODA_BASE)
    if not resp:
        logger.warning("TNDM: toda-a-programacao inacessível")
        return []

    soup         = BeautifulSoup(resp.text, "lxml")
    page_entries = _extract_links_from_page(soup)
    all_entries.update(page_entries)
    logger.info(f"TNDM: página 1 — {len(page_entries)} itens ({len(all_entries)} total)")

    max_page = 1
    for a in soup.find_all("a", href=True):
        href = a["href"]
        pm = re.search(r"[?&]p=(\d+)", href)
        if pm:
            max_page = max(max_page, int(pm.group(1)))

    if max_page == 1:
        max_page = MAX_PAGES

    logger.info(f"TNDM: máximo de páginas detectado: {max_page}")

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

        if new_count == 0:
            logger.info(f"TNDM: sem itens novos na página {page} — a parar")
            break

    logger.info(f"TNDM: {len(all_entries)} espetáculos únicos encontrados")

    if not all_entries:
        return []

    events = []
    total  = len(all_entries)
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

    soup     = BeautifulSoup(resp.text, "lxml")
    ft       = soup.get_text(" ", strip=True)
    ftl      = ft.lower()
    now_year = str(datetime.now().year)

    # ── Título ──────────────────────────────────────────────────────────────
    title = ""
    for sel in ["h1.show-title", "h1.espetaculo-title", ".page-header h1", "main h1",
                "#hero-titulo", "h1"]:
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

    # ── Categorias/Tags ──────────────────────────────────────────────────────
    # v6: ler directamente de .tag-list-item.programacao
    cats = []
    for tag_el in soup.select(".tag-list-item.programacao"):
        t = tag_el.get_text(strip=True)
        if t and t not in cats:
            cats.append(t)

    # Fallback: hints da listagem ou inferência por URL
    if not cats and cats_hint:
        cats = cats_hint
    if not cats:
        ul = url.lower()
        if "/espetaculos/"           in ul: cats = ["Espetáculos"]
        elif "/participacao/"        in ul: cats = ["Participação"]
        elif "/livros-e-pensamento/" in ul: cats = ["Livros e Pensamento"]
        elif "/oficinas-e-formacao/" in ul: cats = ["Oficinas e Formação"]

    # ── Sessões da tabela .table_tickets ─────────────────────────────────────
    # v6: extracção completa de todas as sessões com data, hora e local
    sessions = _parse_sessions_from_table(soup, now_year)

    # Fallback para datas quando não há tabela estruturada
    if not sessions:
        sessions = _parse_dates_fallback(soup, ft, date_hint, now_year)

    # Construir dates[] compatível com schema
    dates = []
    for s in sessions:
        dates.append({
            "date":             s["date"],
            "time_start":       s.get("time_start"),
            "time_end":         s.get("time_end"),
            "duration_minutes": None,
            "is_cancelled":     s.get("is_cancelled", False),
            "is_sold_out":      s.get("is_sold_out", False),
            "notes":            s.get("notes"),
        })

    date_open  = dates[0]["date"]  if dates else None
    date_close = dates[-1]["date"] if len(dates) > 1 else None

    # ── Espaços/locais ───────────────────────────────────────────────────────
    # v6: recolher todos os locais únicos das sessões
    venue_names = []
    for s in sessions:
        vn = s.get("venue_name")
        if vn and vn not in venue_names:
            venue_names.append(vn)

    # space_id canónico (para a Sala principal TNDM)
    space_id = None
    if "sala garrett"          in ftl: space_id = "sala-garrett"
    elif "sala estúdio"        in ftl: space_id = "sala-estudio"
    elif "teatro variedades"   in ftl: space_id = "teatro-variedades"
    elif "jardins do bombarda" in ftl: space_id = "jardins-bombarda"

    # ── Descrição ────────────────────────────────────────────────────────────
    desc = ""
    for sel in [".show-description", ".event-description", ".synopsis",
                ".descricao_evento .htmleditor", ".entry-content",
                ".description", "main p"]:
        el = soup.select_one(sel)
        if el:
            d = el.get_text(separator="\n", strip=True)
            if len(d) > 40:
                desc = d; break
    if not desc:
        og = soup.find("meta", property="og:description")
        if og:
            desc = og.get("content", "").strip()

    # ── Imagem ───────────────────────────────────────────────────────────────
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

    # ── Preço ────────────────────────────────────────────────────────────────
    price_raw = ""
    for sel in [".price", "[class*='preco']", "[class*='price']"]:
        el = soup.select_one(sel)
        if el:
            price_raw = el.get_text(strip=True); break
    if not price_raw and re.search(r"entrada\s+livre|gratuito|acesso\s+livre", ftl):
        price_raw = "Entrada livre"

    # ── Bilheteira ───────────────────────────────────────────────────────────
    ticketing_url = None
    for a in soup.find_all("a", href=True):
        at = a.get_text(strip=True).lower()
        h  = a["href"]
        if any(k in at for k in ["bilhete", "comprar", "reservar", "ticket"]) or "bol.pt" in h:
            ticketing_url = h if h.startswith("http") else f"{WEBSITE}{h}"
            break

    # ── Ficha técnica ────────────────────────────────────────────────────────
    # v6: selector corrigido (.ficha_tecnica com underscore)
    credits_raw = ""
    for sel in [".ficha_tecnica .primeiro_bloco", ".ficha_tecnica", ".ficha-tecnica",
                ".credits", "[class*='ficha']", ".team"]:
        el = soup.select_one(sel)
        if el:
            credits_raw = el.get_text(separator="\n", strip=True); break

    # ── Classificação etária ─────────────────────────────────────────────────
    # v6: regex alargado — captura "M/3", "M/6 anos", "M/ 12 anos"
    audience_raw = ""
    age_m = re.search(r"M[/\s]\s*(\d+)(?:\s*anos?)?", resp.text, re.IGNORECASE)
    if age_m:
        audience_raw = f"M/{age_m.group(1)}"

    # ── Acessibilidade ───────────────────────────────────────────────────────
    accessibility = {
        "has_sign_language":      "lgp" in ftl or "língua gestual" in ftl,
        "has_audio_description":  "audiodescri" in ftl or "audiodescrição" in ftl,
        "has_subtitles":          bool(re.search(r"legenda[sd]?", ftl)),
        "is_relaxed_performance": "sessão descontraída" in ftl or "relaxed" in ftl,
        "wheelchair_accessible":  True,
        "notes":                  None,
    }

    return {
        "source_id":     url.rstrip("/").split("/")[-1],
        "source_url":    url,
        "title":         title,
        "subtitle":      None,
        "description":   desc,
        "categories":    cats,
        "tags":          [],
        "dates":         dates,
        "date_open":     date_open,
        "date_close":    date_close,
        "is_ongoing":    bool(date_close and date_open and date_close > date_open),
        "venue_names":   venue_names,      # v6: lista de locais das sessões
        "price_raw":     price_raw,
        "ticketing_url": ticketing_url,
        "audience":      audience_raw,
        "cover_image":   cover,
        "space_id":      space_id,
        "credits_raw":   credits_raw,
        "accessibility": accessibility,
        "scraped_at":    datetime.now(timezone.utc).isoformat(),
        "_method":       "tndm-v6",
    }


# ---------------------------------------------------------------------------
# FALLBACK DE DATAS (quando não há .table_tickets)
# ---------------------------------------------------------------------------

def _parse_dates_fallback(
    soup: BeautifulSoup,
    ft: str,
    date_hint: str,
    now_year: str,
) -> list[dict]:
    """
    Tenta extrair datas por JSON-LD, selectores HTML ou regex.
    Devolve lista de sessões mínimas [{date, time_start, ...}].
    """
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
                    sessions = [_msession(start[:10], start[11:16] if len(start) > 10 else None)]
                    if end and len(end) >= 10 and end[:10] != start[:10]:
                        sessions.append(_msession(end[:10]))
                    return sessions
        except Exception:
            pass

    # 2. Selectores HTML
    for sel in [".show-date", ".event-date", "[class*='date']", "[class*='data']", "time[datetime]"]:
        for el in soup.select(sel):
            raw = el.get("datetime") or el.get_text(strip=True)
            d1, d2 = _parse_tndm_date(raw)
            if d1:
                sessions = [_msession(d1)]
                if d2:
                    sessions.append(_msession(d2))
                return sessions

    # 3. Regex no texto
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
                sessions = [_msession(d1)]
                if d2:
                    sessions.append(_msession(d2))
                return sessions

    # 4. Hint da listagem
    if date_hint:
        d1, d2 = _parse_tndm_date(date_hint)
        if d1:
            sessions = [_msession(d1)]
            if d2:
                sessions.append(_msession(d2))
            return sessions

    return []


def _msession(date: str, time_start: str = None) -> dict:
    return {
        "date":        date,
        "time_start":  time_start,
        "time_end":    None,
        "venue_name":  None,
        "venue_url":   None,
        "is_cancelled": False,
        "is_sold_out":  False,
        "notes":       None,
    }


# ---------------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------------

def run(start_date: Optional[str] = None) -> list[dict]:
    session = _make_session()

    events = _scrape_toda_programacao(session)
    if events:
        return events

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
