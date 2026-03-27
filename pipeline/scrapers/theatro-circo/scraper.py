"""
Primeira Plateia — Scraper Theatro Circo (Braga)
Venue: Theatro Circo | theatrocirco.com

Estrutura real do site (sem JSON-LD de Event, sem <time datetime>):
  - Programação: theatrocirco.com/programa/
  - Cada evento listado com: "28 março (sáb) → Categoria"
  - Página individual: mesmo formato de data no topo
  - API The Events Calendar: dá 404 (não instalada)
  - Sitemap: tem URLs mas datas no JSON-LD não existem → parse HTML obrigatório

Estratégia:
  1. HTML da página /programa/ → links + datas directamente da listagem
  2. Sitemap → visitar páginas individuais com parser de data nativo
  3. API (fallback histórico, sempre falha com 404)

Melhorias v2:
  - Hora extraída do .info-box span (ex: "21h30")
  - Sala extraída do .info-box span (ex: "Sala Principal", "Sala Estúdio")
  - Preço melhorado: suporta "5€", "5,50€", "Gratuito"
  - Classificação etária extraída (ex: "M/14", "M/6", "Todos os públicos")
  - Tags do .info-box (ex: "Dia Mundial do Teatro", "Acessibilidade")
  - Subtítulo/autor extraído do h2.small
  - Duração extraída do texto de créditos
  - Sessões estruturadas via JSON em data-sessions do #popup-reserva
  - URL de bilhetes mais robusta (bol.pt prioritário)
  - Acessibilidade: LGP + Audiodescrição detectados em secção .access dedicada
  - Todos os campos novos integrados no schema de saída
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

import sys as _sys, os as _os
_sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))
try:
    from pipeline.core.http_cache import ConditionalSession as _ConditionalSession
    _HTTP_CACHE_AVAILABLE = True
except ImportError:
    _HTTP_CACHE_AVAILABLE = False

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
VENUE_ID   = "theatro-circo"
SCRAPER_ID = "theatro-circo"
WEBSITE    = "https://theatrocirco.com"

API_BASE  = f"{WEBSITE}/wp-json/tribe/events/v1/events"
PER_PAGE  = 50
MAX_PAGES = 20

SITEMAP_URLS = [
    f"{WEBSITE}/sitemap.xml",
    f"{WEBSITE}/event-sitemap.xml",
]

# Só URLs PT (excluir /en/event/)
EVENT_URL_PATTERN = re.compile(r"(?<!/en)/event/[^/?#]+/?$")

SITEMAP_MAX_AGE_DAYS = 365

# Página de programação — URL real confirmada
PROGRAMME_URLS = [
    f"{WEBSITE}/programa/",
    f"{WEBSITE}/programme/",
]

# Filtro de data: ignorar eventos passados há mais de N dias
# Usar 0 para não filtrar (recomendado — o dedup trata disso)
PAST_DAYS_CUTOFF = 0

REQUEST_DELAY = 0.4
TIMEOUT       = 20

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "pt-PT,pt;q=0.9,en;q=0.8",
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

# Salas conhecidas do Theatro Circo para validação
KNOWN_ROOMS = {
    "sala principal", "sala estúdio", "sala estudio",
    "grande auditório", "foyer", "claustro", "exterior",
}

# Classificações etárias portuguesas
AGE_RATING_RE = re.compile(
    r'\b(m/\d+|todos os públicos|todos os publicos|para todos|tp)\b',
    re.IGNORECASE
)


# ---------------------------------------------------------------------------
# SESSION
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


def _make_conditional_session():
    base = _make_session()
    if _HTTP_CACHE_AVAILABLE:
        return _ConditionalSession(venue_id=VENUE_ID, session=base)
    return None


def _get(session, url, timeout=TIMEOUT, params=None):
    try:
        resp = session.get(url, timeout=timeout, params=params)
        resp.raise_for_status()
        return resp
    except requests.exceptions.Timeout:
        logger.warning(f"TC: timeout — {url}")
    except requests.exceptions.HTTPError as e:
        logger.warning(f"TC: HTTP {e.response.status_code} — {url}")
    except requests.exceptions.RequestException as e:
        logger.debug(f"TC: erro — {url}: {e}")
    return None


# ---------------------------------------------------------------------------
# PARSING DE DATAS — formato nativo do TC
#
# Formatos encontrados na página real:
#   "28 março (sáb)"               → dia mês(extenso) (diasemana)
#   "28 março (sáb) →"             → com seta separadora
#   "12 janeiro a 18 abril"        → período
#   "3, 10, 17, 24 e 31 março"     → múltiplos dias — usar o primeiro
#   "18 abril"                     → simples sem dia semana
# ---------------------------------------------------------------------------

def _infer_year(month_num: str, month_end: str = None) -> str:
    """
    Infere o ano para uma data sem ano explícito.
    - Para períodos, usa o mês de fim para decidir
    - Só usa próximo ano se o mês de início é claramente no futuro distante
    - Por defeito usa o ano actual (cobre eventos recentes e próximos)
    """
    now = datetime.now()
    ref_month = int(month_end) if month_end else int(month_num)
    if ref_month >= now.month:
        return str(now.year)
    return str(now.year)


def _parse_tc_date(text: str) -> tuple[Optional[str], Optional[str]]:
    """
    Parseia texto de data do formato nativo do Theatro Circo.
    Retorna (date_first_iso, date_close_iso).
    """
    if not text:
        return None, None

    t = text.strip().lower()
    t = t.replace("→", "").strip()
    # Remover dia da semana entre parênteses: (sáb), (sex), (ter), (dom), (seg), (qua), (qui)
    t = re.sub(r'\([^)]+\)', '', t).strip()
    t = re.sub(r'\s+', ' ', t).strip()

    # Período: "12 janeiro a 18 abril" ou "12 jan - 18 abr"
    period = re.match(
        r'^(\d{1,2})\s+([a-záéíóúâêôãõç]+)\s+(?:a|até|-|–)\s+(\d{1,2})\s+([a-záéíóúâêôãõç]+)(?:\s+(\d{4}))?',
        t
    )
    if period:
        m1    = MONTH_PT.get(period.group(2)[:3])
        m2    = MONTH_PT.get(period.group(4)[:3])
        year  = period.group(5) or _infer_year(m1 or "01", m2)
        d1    = f"{year}-{m1}-{period.group(1).zfill(2)}" if m1 else None
        d2    = f"{year}-{m2}-{period.group(3).zfill(2)}" if m2 else None
        return d1, d2

    # Múltiplos dias: "3, 10, 17, 24 e 31 março"
    month_m = re.search(r'([a-záéíóúâêôãõç]{4,})', t)
    if month_m:
        month = MONTH_PT.get(month_m.group(1)[:3])
        if month:
            year_m = re.search(r'\d{4}', t)
            year   = year_m.group() if year_m else _infer_year(month)
            nums = re.findall(r'\d+', t[:month_m.start()])
            if nums:
                day = nums[0].zfill(2)
                return f"{year}-{month}-{day}", None

    # Mês abreviado (3 letras): "28 mar" ou "mar 28"
    month_short_m = re.search(r'\b(jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)\b', t)
    if month_short_m:
        month = MONTH_PT.get(month_short_m.group(1))
        if month:
            nums = re.findall(r'\d+', t)
            if nums:
                year = next((n for n in nums if len(n) == 4), _infer_year(month))
                day  = next((n for n in nums if len(n) <= 2), None)
                if day:
                    return f"{year}-{month}-{day.zfill(2)}", None

    return None, None


def _parse_time_tc(text: str) -> Optional[str]:
    """
    Converte hora no formato TC "21h30", "21H30", "21:30", "21h" para "HH:MM".
    """
    if not text:
        return None
    m = re.search(r'(\d{1,2})[hH:](\d{2})?', text.strip())
    if m:
        hh = m.group(1).zfill(2)
        mm = (m.group(2) or "00").zfill(2)
        return f"{hh}:{mm}"
    return None


# ---------------------------------------------------------------------------
# EXTRACÇÃO DE SESSÕES ESTRUTURADAS via #popup-reserva data-sessions
#
# O popup de reserva tem um atributo data-sessions com JSON:
#   [{"start":"2026-03-27 21:30:00","notes":"","show_site":true}]
# Esta é a fonte mais fiável de datas/horas.
# ---------------------------------------------------------------------------

def _extract_sessions_from_popup(soup: BeautifulSoup) -> list[dict]:
    """
    Extrai sessões estruturadas do atributo data-sessions do #popup-reserva.
    Retorna lista de dicts com date, time_start, notes.
    """
    popup = soup.select_one("#popup-reserva[data-sessions]")
    if not popup:
        return []

    raw = popup.get("data-sessions", "")
    if not raw:
        return []

    try:
        sessions_raw = _json.loads(raw)
    except (_json.JSONDecodeError, ValueError):
        logger.debug("TC: data-sessions JSON inválido")
        return []

    sessions = []
    for s in sessions_raw:
        if not isinstance(s, dict):
            continue
        start = s.get("start", "")
        if not start:
            continue
        # Formato: "2026-03-27 21:30:00"
        parts = str(start).strip().split(" ")
        date_part = parts[0] if parts else ""
        time_part = parts[1][:5] if len(parts) > 1 else None  # "21:30"

        # Validar data
        if not re.match(r'^\d{4}-\d{2}-\d{2}$', date_part):
            continue

        sessions.append({
            "date":             date_part,
            "time_start":       time_part,
            "time_end":         None,
            "duration_minutes": None,
            "is_cancelled":     False,
            "is_sold_out":      False,
            "notes":            s.get("notes") or None,
        })

    return sessions


# ---------------------------------------------------------------------------
# EXTRACÇÃO DO INFO-BOX
#
# Estrutura real do .info-box:
#   <div class="info-box">
#     <div>
#       <span>21h30</span>
#       <span>Sala Principal</span>
#     </div>
#     <div class="bottom desktop">
#       <span>5€ </span>
#       <span>&nbsp;&nbsp;M/14&nbsp;&nbsp;</span>
#       <div class="tag">Acessibilidade</div>
#       <div class="tag">Dia Mundial do Teatro</div>
#     </div>
#   </div>
# ---------------------------------------------------------------------------

def _extract_info_box(soup: BeautifulSoup) -> dict:
    """
    Extrai hora, sala, preço, classificação etária e tags do .info-box.
    Retorna dict com os campos encontrados.
    """
    result = {
        "time_start":  None,
        "room":        None,
        "price_raw":   "",
        "age_rating":  None,
        "event_tags":  [],
    }

    info_box = soup.select_one(".info-box")
    if not info_box:
        return result

    # Tags especiais (ex: "Dia Mundial do Teatro", "Acessibilidade")
    for tag_el in info_box.select(".tag"):
        tag_text = tag_el.get_text(strip=True)
        if tag_text:
            result["event_tags"].append(tag_text)

    # Primeiro bloco de spans: hora + sala
    first_div = info_box.find("div", class_=lambda c: not c or "bottom" not in (c or ""))
    if first_div and first_div.name == "div":
        spans = first_div.find_all("span", recursive=False)
        for span in spans:
            text = span.get_text(strip=True)
            if not text:
                continue
            # Hora: contém "h" ou ":"  e dígitos
            if result["time_start"] is None and re.search(r'\d{1,2}[hH:]\d{0,2}', text):
                result["time_start"] = _parse_time_tc(text)
            # Sala: texto sem dígitos e sem "h" que seja uma sala conhecida
            elif result["room"] is None and not re.search(r'\d', text):
                lower = text.lower()
                if any(k in lower for k in KNOWN_ROOMS) or re.search(r'\bsala\b|\bauditório\b|\bfoyer\b', lower, re.IGNORECASE):
                    result["room"] = text

    # Bloco .bottom: preço + classificação etária
    bottom = info_box.select_one(".bottom")
    if bottom:
        for span in bottom.find_all("span"):
            text = span.get_text(strip=True).replace("\xa0", " ").strip()
            if not text:
                continue

            # Preço: contém "€" ou "gratuito" ou "livre"
            if result["price_raw"] == "" and (
                "€" in text
                or re.search(r'gratuito|entrada\s+livre|livre\s+acesso', text, re.IGNORECASE)
            ):
                result["price_raw"] = text

            # Classificação etária: M/6, M/12, M/14, M/16, M/18, TP, Todos os públicos
            age_m = AGE_RATING_RE.search(text)
            if age_m and result["age_rating"] is None:
                result["age_rating"] = age_m.group(0).upper().replace(
                    "TODOS OS PÚBLICOS", "TP"
                ).replace("TODOS OS PUBLICOS", "TP").replace("PARA TODOS", "TP")

    return result


# ---------------------------------------------------------------------------
# EXTRACÇÃO DE DURAÇÃO dos créditos
# ---------------------------------------------------------------------------

def _extract_duration(soup: BeautifulSoup) -> Optional[int]:
    """
    Procura "Duração X minutos" ou "X min" no texto da página.
    Retorna duração em minutos ou None.
    """
    # Procurar na secção de créditos primeiro, depois em todo o texto
    for sel in [".credits", ".entry-content", "main"]:
        el = soup.select_one(sel)
        if not el:
            continue
        text = el.get_text(" ", strip=True)
        m = re.search(
            r'dura[çc][aã]o\s*[:\-]?\s*(\d+)\s*(?:minutos?|min\.?)',
            text, re.IGNORECASE
        )
        if m:
            return int(m.group(1))
        # Formato alternativo: "65 min" isolado
        m2 = re.search(r'\b(\d{2,3})\s*(?:minutos?|min\.?)\b', text, re.IGNORECASE)
        if m2:
            mins = int(m2.group(1))
            if 10 <= mins <= 300:  # sanidade: entre 10 min e 5 horas
                return mins
    return None


# ---------------------------------------------------------------------------
# EXTRACÇÃO DE SUBTÍTULO / AUTOR
# ---------------------------------------------------------------------------

def _extract_subtitle(soup: BeautifulSoup) -> Optional[str]:
    """
    Extrai o h2.small da secção .title-top como subtítulo/autor.
    Ex: "Sara Inês Gigante"
    """
    # Dentro de .title-top para evitar h2 do corpo
    title_top = soup.select_one("section.title-top")
    if title_top:
        h2 = title_top.select_one("h2.small")
        if h2:
            text = h2.get_text(strip=True)
            if text:
                return text

    # Fallback: qualquer h2 antes do conteúdo
    for h2 in soup.select("h2.small"):
        text = h2.get_text(strip=True)
        if text and len(text) < 200:
            return text

    return None


# ---------------------------------------------------------------------------
# EXTRACÇÃO DE ACESSIBILIDADE (melhorada)
# ---------------------------------------------------------------------------

def _extract_accessibility(soup: BeautifulSoup) -> dict:
    """
    Detecta features de acessibilidade, priorizando a secção .access dedicada.
    """
    # Texto completo da página
    ft = soup.get_text(" ", strip=True).lower()

    # Secção .access tem texto explícito sobre LGP, Audiodescrição, etc.
    access_text = ""
    access_el = soup.select_one(".access")
    if access_el:
        access_text = access_el.get_text(" ", strip=True).lower()

    # Combinar: secção dedicada tem prioridade mas texto geral também conta
    combined = access_text + " " + ft

    return {
        "has_sign_language": bool(
            re.search(r'lgp|língua gestual|lingua gestual', combined)
        ),
        "has_audio_description": bool(
            re.search(r'audiodescrição|audiodescri[çc]|audio\s*descri', combined)
        ),
        "has_subtitles": bool(
            re.search(r'legenda[sd]?', combined)
        ),
        "is_relaxed_performance": bool(
            re.search(r'relaxed|descontraída|descontraida', combined)
        ),
        "wheelchair_accessible": True,  # TC tem acessibilidade garantida
        "notes": access_el.get_text(strip=True) if access_el else None,
    }


# ---------------------------------------------------------------------------
# MÉTODO 1 — HTML da página /programa/
# Esta é a fonte principal — lista todos os eventos com data visível
# ---------------------------------------------------------------------------

def _scrape_via_programme_page(session: requests.Session) -> list[dict]:
    """
    Raspa a página de programação do TC.
    Estrutura: cada bloco de evento tem data em texto + link para página individual.
    Recolhe links + datas directamente da listagem para não visitar cada página.
    """
    for prog_url in PROGRAMME_URLS:
        logger.info(f"TC: método 1 — HTML da página de programação ({prog_url})")
        resp = _get(session, prog_url)
        if not resp:
            continue

        soup = BeautifulSoup(resp.text, "lxml")
        entries: list[dict] = []

        for a in soup.find_all("a", href=True):
            href = a["href"]
            full = href if href.startswith("http") else f"{WEBSITE}{href}"

            if not EVENT_URL_PATTERN.search(full):
                continue
            if "/en/" in full:
                continue

            # Extrair título do link
            title = ""
            for sel in ["h3", "h4", "h2", ".event-title"]:
                el = a.select_one(sel)
                if el:
                    title = el.get_text(strip=True)
                    break
            if not title:
                title = a.get_text(strip=True)
            if not title or title.lower() in ("bilhetes", "saiba mais", "ver mais"):
                title = ""

            # Data: procurar no elemento pai e irmãos anteriores
            date_text = ""
            node = a.parent
            for _ in range(5):
                if not node:
                    break
                t = node.get_text(" ", strip=True)
                dm = re.search(
                    r'\d{1,2}\s+(?:jan(?:eiro)?|fev(?:ereiro)?|mar(?:ço)?|abr(?:il)?|mai(?:o)?|'
                    r'jun(?:ho)?|jul(?:ho)?|ago(?:sto)?|set(?:embro)?|out(?:ubro)?|'
                    r'nov(?:embro)?|dez(?:embro)?)[^→\n]*',
                    t, re.IGNORECASE
                )
                if dm:
                    date_text = dm.group(0).strip()
                    break
                node = node.parent

            # Imagem de capa
            img = a.find("img")
            cover = img.get("src", "") if img else ""
            if cover and not cover.startswith("http"):
                cover = f"{WEBSITE}{cover}"
            cover = re.sub(r'-\d+x\d+(\.\w+)$', r'\1', cover)

            # Categoria — texto depois do "→" na linha de data
            cat = ""
            if "→" in date_text:
                parts = date_text.split("→")
                if len(parts) > 1:
                    cat = parts[-1].strip()
                    date_text = parts[0].strip()

            entries.append({
                "source_url": full,
                "title":      title,
                "date_text":  date_text,
                "cover":      cover or None,
                "category":   cat,
            })

        # Deduplicar por URL
        seen: set = set()
        deduped = []
        for e in entries:
            if e["source_url"] not in seen:
                seen.add(e["source_url"])
                deduped.append(e)

        logger.info(f"TC {prog_url}: {len(deduped)} eventos encontrados")

        if not deduped:
            continue

        cond_session = _make_conditional_session()
        events = []
        for i, entry in enumerate(deduped):
            ev = _parse_event_page(
                entry["source_url"], session,
                date_hint=entry["date_text"],
                title_hint=entry["title"],
                cover_hint=entry["cover"],
                cat_hint=entry["category"],
                cond_session=cond_session,
            )
            if ev:
                events.append(ev)
            if (i + 1) % 10 == 0:
                logger.info(f"TC programa: {i+1}/{len(deduped)} processados ({len(events)} válidos)")
            time.sleep(REQUEST_DELAY)

        logger.info(f"TC programa: {len(events)} eventos recolhidos")
        if events:
            return events

    return []


# ---------------------------------------------------------------------------
# MÉTODO 2 — Sitemap XML
# ---------------------------------------------------------------------------

def _fetch_sitemap_urls(session: requests.Session) -> list[str]:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=SITEMAP_MAX_AGE_DAYS)).strftime("%Y-%m-%d")

    loc_re   = re.compile(r"<loc[^>]*>(https?://[^<]+)</loc>")
    lmod_re  = re.compile(r"<lastmod[^>]*>([0-9]{4}-[0-9]{2}-[0-9]{2})")
    block_re = re.compile(r"<url[^>]*>(.*?)</url>", re.DOTALL)
    smap_re  = re.compile(r"<sitemap[^>]*>(.*?)</sitemap>", re.DOTALL)

    def extract(xml: str) -> list[tuple[str, str]]:
        pairs, seen = [], set()
        for block in block_re.findall(xml):
            loc_m  = loc_re.search(block)
            lmod_m = lmod_re.search(block)
            if loc_m:
                url     = loc_m.group(1).strip()
                lastmod = lmod_m.group(1) if lmod_m else ""
                if url not in seen and EVENT_URL_PATTERN.search(url) and "/en/" not in url:
                    pairs.append((url, lastmod))
                    seen.add(url)
        return pairs

    all_pairs: list[tuple[str, str]] = []

    for sitemap_url in SITEMAP_URLS:
        resp = _get(session, sitemap_url)
        if not resp:
            continue
        xml = resp.text
        logger.info(f"TC Sitemap: {sitemap_url}")

        # Sitemap index
        for block in smap_re.findall(xml):
            loc_m = loc_re.search(block)
            if not loc_m:
                continue
            sub_url = loc_m.group(1).strip()
            if "event" not in sub_url.lower():
                continue
            sub = _get(session, sub_url)
            if not sub:
                continue
            pairs = extract(sub.text)
            logger.info(f"TC Sitemap: {len(pairs)} URLs em {sub_url}")
            all_pairs.extend(pairs)
            time.sleep(0.2)
        if all_pairs:
            break

        # Sitemap simples
        pairs = extract(xml)
        if pairs:
            all_pairs.extend(pairs)
            break

    recent  = [(u, d) for u, d in all_pairs if d >= cutoff]
    nodate  = [u     for u, d in all_pairs if not d]
    old     = len(all_pairs) - len(recent) - len(nodate)
    recent.sort(key=lambda x: x[1], reverse=True)
    urls = [u for u, _ in recent] + nodate
    logger.info(f"TC Sitemap: {len(all_pairs)} totais → {len(recent)} recentes + {len(nodate)} sem data ({old} ignoradas)")
    return urls


def _scrape_via_sitemap(session: requests.Session) -> list[dict]:
    logger.info("TC: método 2 — Sitemap XML")
    urls = _fetch_sitemap_urls(session)
    if not urls:
        return []

    cond_session = _make_conditional_session()
    events, skipped = [], 0
    for i, url in enumerate(urls):
        ev = _parse_event_page(url, session, cond_session=cond_session)
        if ev:
            events.append(ev)
        else:
            skipped += 1
        if (i + 1) % 10 == 0:
            logger.info(f"TC Sitemap: {i+1}/{len(urls)} ({len(events)} eventos, {skipped} ignorados)")
        time.sleep(REQUEST_DELAY)

    logger.info(f"TC Sitemap: {len(events)} eventos de {len(urls)} URLs")
    return events


# ---------------------------------------------------------------------------
# PARSE DE PÁGINA INDIVIDUAL
# ---------------------------------------------------------------------------

def _parse_event_page(
    url: str,
    session: requests.Session,
    date_hint: str = None,
    title_hint: str = None,
    cover_hint: str = None,
    cat_hint: str = None,
    cond_session=None,
) -> Optional[dict]:
    # ETag/304: se página não mudou e temos hints suficientes, usar hints
    if cond_session is not None:
        resp = cond_session.get_conditional(url)
        if resp is None and title_hint and date_hint:
            d1, d2 = _parse_tc_date(date_hint)
            if d1:
                return {
                    "source_id":    url.rstrip("/").split("/")[-1],
                    "source_url":   url,
                    "title":        title_hint,
                    "subtitle":     None,
                    "description":  "",
                    "categories":   [cat_hint] if cat_hint else [],
                    "tags":         [],
                    "event_tags":   [],
                    "dates":        [_make_date(d1)],
                    "date_open":    d1,
                    "date_close":   d2,
                    "is_ongoing":   bool(d2),
                    "price_raw":    "",
                    "age_rating":   None,
                    "room":         None,
                    "duration_minutes": None,
                    "ticketing_url": None,
                    "audience":     None,
                    "cover_image":  cover_hint,
                    "space_id":     None,
                    "credits_raw":  None,
                    "accessibility": {
                        "has_sign_language":      False,
                        "has_audio_description":  False,
                        "has_subtitles":          False,
                        "is_relaxed_performance": False,
                        "wheelchair_accessible":  True,
                        "notes":                  None,
                    },
                    "scraped_at": datetime.now(timezone.utc).isoformat(),
                    "_method":    "tc-304",
                }
        if resp is None:
            return None
    else:
        resp = _get(session, url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "lxml")

    # ── Título ──
    title = ""
    for sel in ["h1.entry-title", ".event-title h1", "article h1", "main h1", "h1"]:
        el = soup.select_one(sel)
        if el:
            title = el.get_text(strip=True)
            break
    if not title:
        og = soup.find("meta", property="og:title")
        if og:
            title = og.get("content", "").replace(" - Theatro Circo", "").strip()
    if not title and title_hint:
        title = title_hint
    if not title:
        return None

    # ── Subtítulo / Autor ──
    subtitle = _extract_subtitle(soup)

    # ── Info Box: hora, sala, preço, classificação etária, tags ──
    info = _extract_info_box(soup)

    # ── Sessões estruturadas via data-sessions (fonte mais fiável) ──
    sessions = _extract_sessions_from_popup(soup)

    # ── Datas ──
    dates      = []
    date_close = None

    # 1. Sessões do popup (mais fiável — inclui hora exacta)
    if sessions:
        dates      = sessions
        date_close = None  # sessões múltiplas não têm date_close no sentido de período
        # Propagar duração para cada sessão se disponível
    else:
        # 2. Tentar JSON-LD (pouco provável no TC mas vale a pena)
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = _json.loads(script.string or "")
                if isinstance(data, list):
                    data = next((d for d in data if isinstance(d, dict) and d.get("@type") == "Event"), {})
                if isinstance(data, dict) and data.get("@type") == "Event":
                    start = data.get("startDate", "")
                    end   = data.get("endDate",   "")
                    if start and len(start) >= 10:
                        dates      = [_make_date(start[:10], start[11:16] if len(start) > 10 else info["time_start"])]
                        date_close = end[:10] if end and len(end) >= 10 else None
                        break
            except Exception:
                pass

        # 3. Tentar <time datetime>
        if not dates:
            for t in soup.select("time[datetime]"):
                dt = t.get("datetime", "")
                if dt and len(dt) >= 10:
                    dates = [_make_date(dt[:10], dt[11:16] if len(dt) > 10 else info["time_start"])]
                    break

        # 4. Parser nativo TC: procurar texto com formato "28 março (sáb)"
        if not dates:
            page_text = soup.get_text(" ", strip=True)
            tc_date_re = re.compile(
                r'\d{1,2}\s+(?:de\s+)?(?:janeiro|fevereiro|março|marco|abril|maio|junho|'
                r'julho|agosto|setembro|outubro|novembro|dezembro)'
                r'(?:\s+(?:a|até|-|–)\s+\d{1,2}\s+(?:de\s+)?(?:janeiro|fevereiro|março|'
                r'marco|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro))?'
                r'(?:\s+\d{4})?',
                re.IGNORECASE
            )
            m = tc_date_re.search(page_text)
            if m:
                d1, d2 = _parse_tc_date(m.group(0))
                if d1:
                    dates      = [_make_date(d1, info["time_start"])]
                    date_close = d2

        # 5. Usar hint da listagem
        if not dates and date_hint:
            d1, d2 = _parse_tc_date(date_hint)
            if d1:
                dates      = [_make_date(d1, info["time_start"])]
                date_close = d2

    # Se temos hora do info_box e sessões sem hora, propagar
    if info["time_start"] and dates:
        for d in dates:
            if d.get("time_start") is None:
                d["time_start"] = info["time_start"]

    # ── Duração ── propagar para todas as sessões
    duration = _extract_duration(soup)
    if duration and dates:
        for d in dates:
            d["duration_minutes"] = duration

    # ── Filtro de datas passadas ──
    if PAST_DAYS_CUTOFF > 0 and dates:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=PAST_DAYS_CUTOFF)).strftime("%Y-%m-%d")
        first_date = dates[0].get("date", "") if dates else ""
        if first_date and first_date < cutoff:
            if not date_close or date_close < cutoff:
                return None

    # ── Descrição ──
    # Estrutura real do TC: <section class="text module"><div class="col-lg-8"><p>...</p>
    desc = ""

    # 1. Selector primário nativo do TC
    text_section = soup.select_one("section.text .col-lg-8")
    if text_section:
        for unwanted in text_section.select(".credits, .credit-wrapper"):
            unwanted.decompose()
        d = text_section.get_text(separator="\n", strip=True)
        if len(d) > 30:
            desc = d

    # 2. Fallback: qualquer section.text
    if not desc:
        text_section2 = soup.select_one("section.text")
        if text_section2:
            for unwanted in text_section2.select(".credits, .credit-wrapper, footer"):
                unwanted.decompose()
            d = text_section2.get_text(separator="\n", strip=True)
            if len(d) > 30:
                desc = d

    # 3. Fallback histórico (outros temas WP)
    if not desc:
        for sel in [".entry-content", ".event-description", ".event-content", "article .content"]:
            el = soup.select_one(sel)
            if el:
                d = el.get_text(separator="\n", strip=True)
                if len(d) > 30:
                    desc = d
                    break

    # 4. og:description como último recurso (é sempre preenchido no TC)
    if not desc:
        og = soup.find("meta", property="og:description")
        if og:
            desc = og.get("content", "").strip()

    # ── Imagem ──
    cover = cover_hint
    if not cover:
        og_img = soup.find("meta", property="og:image")
        if og_img:
            cover = og_img.get("content", "")
    if not cover:
        for sel in [".wp-post-image", "article img[src]", "main img[src]"]:
            el = soup.select_one(sel)
            if el and el.get("src"):
                src   = el["src"]
                cover = src if src.startswith("http") else f"{WEBSITE}{src}"
                cover = re.sub(r'-\d+x\d+(\.\w+)$', r'\1', cover)
                break

    # ── Preço ──
    # Prioridade: info_box > regex no texto geral
    price_raw = info["price_raw"]
    if not price_raw:
        ft = soup.get_text(" ", strip=True)
        price_m = re.search(r'(\d+[,.]?\d*\s*€[^\n|.]{0,40})', ft)
        if price_m:
            price_raw = price_m.group(1).strip()
        if not price_raw and re.search(r'gratuito|entrada\s+livre|livre\s+acesso', ft, re.IGNORECASE):
            price_raw = "Entrada livre"

    # ── Bilheteira ──
    # Prioridade: link bol.pt > qualquer link com "bilhete"
    ticketing_url = None
    bol_link = soup.find("a", href=re.compile(r'bol\.pt'))
    if bol_link:
        ticketing_url = bol_link["href"]
    else:
        for a in soup.find_all("a", href=True):
            at = a.get_text(strip=True).lower()
            h  = a["href"]
            if any(k in at for k in ["bilhete", "comprar", "ticket"]):
                ticketing_url = h if h.startswith("http") else f"{WEBSITE}{h}"
                break

    # ── Acessibilidade ──
    accessibility = _extract_accessibility(soup)

    # ── Categoria ──
    cats = []
    if cat_hint:
        cats = [cat_hint.strip()]
    else:
        ft = soup.get_text(" ", strip=True).lower()
        for kw in ["Música", "Teatro", "Dança", "Cinema", "Mediação", "Multidisciplinar"]:
            if kw.lower() in ft:
                cats.append(kw)
                break

    # ── Tags de taxonomia WordPress ──
    wp_tags = []
    for a in soup.find_all("a", href=True):
        if "/event_tag/" in a["href"]:
            wp_tags.append(a.get_text(strip=True))

    # ── Créditos ──
    credits_raw = None
    credits_el = soup.select_one(".credits")
    if credits_el:
        credits_raw = credits_el.get_text(separator="\n", strip=True)

    return {
        "source_id":        url.rstrip("/").split("/")[-1],
        "source_url":       url,
        "title":            title,
        "subtitle":         subtitle,
        "description":      desc,
        "categories":       cats,
        "tags":             wp_tags,
        "event_tags":       info["event_tags"],  # Tags especiais do info-box (ex: "Dia Mundial do Teatro")
        "dates":            dates,
        "date_open":        dates[0]["date"] if dates else None,
        "date_close":       date_close,
        "is_ongoing":       bool(date_close),
        "price_raw":        price_raw,
        "age_rating":       info["age_rating"],      # Ex: "M/14", "TP"
        "room":             info["room"],             # Ex: "Sala Principal"
        "duration_minutes": duration,                 # Ex: 65
        "ticketing_url":    ticketing_url,
        "audience":         None,
        "cover_image":      cover or None,
        "space_id":         None,
        "credits_raw":      credits_raw,
        "accessibility":    accessibility,
        "scraped_at":       datetime.now(timezone.utc).isoformat(),
        "_method":          "tc-html",
    }


def _make_date(date: str, time_start: str = None) -> dict:
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
# MÉTODO 3 — API (histórico, sempre dá 404 no TC)
# ---------------------------------------------------------------------------

def _scrape_via_api(start_date: str, session: requests.Session) -> list[dict]:
    logger.info("TC: método 3 — API WP (The Events Calendar)")
    all_events = []
    page = 1
    while page <= MAX_PAGES:
        resp = _get(session, API_BASE, params={
            "page": page, "per_page": PER_PAGE,
            "status": "publish", "start_date": start_date,
        })
        if not resp:
            break
        data = resp.json()
        evts = data.get("events", [])
        if not evts:
            break
        all_events.extend(evts)
        if page >= data.get("total_pages", 1):
            break
        page += 1
        time.sleep(REQUEST_DELAY)

    if not all_events:
        logger.warning("TC API: sem eventos")
    return all_events


# ---------------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------------

def run(start_date: Optional[str] = None) -> list[dict]:
    session   = _make_session()
    start_str = start_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # 1. HTML da página de programação (fonte principal)
    events = _scrape_via_programme_page(session)
    if events:
        return events

    # 2. Sitemap + parse individual
    logger.warning("TC: página de programação sem resultados — a tentar sitemap")
    events = _scrape_via_sitemap(session)
    if events:
        return events

    # 3. API (raramente funciona)
    raw = _scrape_via_api(start_str, session)
    if raw:
        return raw

    logger.error("TC: todos os métodos falharam")
    return []


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    events = run()
    print(f"\nTotal: {len(events)} eventos")
    if events:
        print(_json.dumps(events[0], indent=2, ensure_ascii=False))
