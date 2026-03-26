"""
Culturgest Scraper — Versão 14 (26 Mar 2026)

DIAGNÓSTICO DO PROBLEMA v12/v13:
─────────────────────────────────
  O endpoint /pt/programacao/schedule/events/ devolve SEMPRE a página HTML
  completa (91 KB) independentemente dos headers enviados (incluindo
  X-Requested-With: XMLHttpRequest). O servidor Django do site não usa
  request.is_ajax() (deprecado desde Django 3.1) para activar o modo fragmento.

  O resultado: 0 ou 1 evento extraído (o link "Agenda" do nav de detalhe
  que passa erroneamente no filtro de URL porque /por-evento/?$ não faz
  match a /por-evento/? com query string vazia).

ESTRATÉGIA v14 — três níveis em cascata:
──────────────────────────────────────────
  1. filter_list_url  (/pt/programacao/filtrar/)
     O JS do site define window.filter_list_url. Este endpoint é chamado
     com os parâmetros de filtro e provavelmente devolve JSON com a lista
     de eventos. Testamos GET sem parâmetros e com typology/public.

  2. schedule/events/ com parâmetro 'from'
     O endpoint de listagem pode precisar do parâmetro 'from=YYYY-MM-DD'
     para activar o modo fragmento (eventos a partir de uma data).

  3. Sitemap XML
     Fallback robusto: /sitemap.xml ou robots.txt → descobrir sitemap
     de programação → extrair slugs de eventos.

  Após obter a lista de URLs, o parse de cada evento é idêntico ao v13
  (já validado com o HTML real do evento Diana Niepce).
"""

import re
import time
import json as _json
import logging
import warnings
from datetime import datetime, date, timezone
from typing import Optional, List
from urllib.parse import urlparse, urlunparse, urljoin

import requests
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", message="Unverified HTTPS request")

logger = logging.getLogger(__name__)

WEBSITE = "https://www.culturgest.pt"
LIST_URL = f"{WEBSITE}/pt/programacao/por-evento/"
SCHEDULE_URL = f"{WEBSITE}/pt/programacao/schedule/events/"
FILTER_URL = f"{WEBSITE}/pt/programacao/filtrar/"
SITEMAP_URLS = [
    f"{WEBSITE}/sitemap.xml",
    f"{WEBSITE}/pt/sitemap.xml",
    f"{WEBSITE}/robots.txt",
]

REQUEST_DELAY = 1.5
TIMEOUT = 30
MAX_PAGES = 30

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "pt-PT,pt;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": LIST_URL,
}

HEADERS_HTML = {**HEADERS, "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}
HEADERS_JSON = {**HEADERS, "Accept": "application/json, text/javascript, */*; q=0.01", "X-Requested-With": "XMLHttpRequest"}

MONTH_PT = {
    "jan": "01", "fev": "02", "mar": "03", "abr": "04", "mai": "05", "jun": "06",
    "jul": "07", "ago": "08", "set": "09", "out": "10", "nov": "11", "dez": "12",
    "janeiro": "01", "fevereiro": "02", "marco": "03", "abril": "04",
    "maio": "05", "junho": "06", "julho": "07", "agosto": "08", "setembro": "09",
    "outubro": "10", "novembro": "11", "dezembro": "12",
}


# ---------------------------------------------------------------------------
# Utilitários
# ---------------------------------------------------------------------------

def _normalize_url(url: str) -> str:
    p = urlparse(url)
    return urlunparse((p.scheme, p.netloc, p.path.rstrip("/"), "", "", ""))


def _is_event_url(url: str) -> bool:
    """
    URL de evento individual: /pt/programacao/<slug>/
    Exclui listagens, filtros, arquivo, nav links, e qualquer URL
    com query string que não seja de evento (typology=, public=, page=).
    """
    # Excluir query strings de listagem — eventos não têm query string
    parsed = urlparse(url)
    if parsed.query:
        return False
    path = parsed.path.rstrip("/")
    # Deve ser exactamente /pt/programacao/<slug>
    parts = [p for p in path.split("/") if p]
    if len(parts) != 3:
        return False
    if parts[0] not in ("pt", "en"):
        return False
    if parts[1] != "programacao":
        return False
    # Excluir slugs de páginas de sistema
    SYSTEM_SLUGS = {
        "por-evento", "agenda-pdf", "archive", "schedule",
        "filtrar", "sitemap", "por-semana", "por-mes",
    }
    if parts[2] in SYSTEM_SLUGS:
        return False
    return True


def _make_session() -> requests.Session:
    s = requests.Session()
    s.verify = False
    s.headers.update(HEADERS_HTML)
    try:
        r = s.get(LIST_URL, timeout=TIMEOUT)
        r.raise_for_status()
        logger.info(f"Sessão inicializada — cookies: {list(s.cookies.keys())}")
    except Exception as e:
        logger.warning(f"Sessão sem cookies iniciais: {e}")
    return s


def _get(session: requests.Session, url: str, params: dict = None, headers: dict = None):
    try:
        h = dict(session.headers)
        if headers:
            h.update(headers)
        r = session.get(url, params=params, headers=h, timeout=TIMEOUT)
        r.raise_for_status()
        return r
    except Exception as e:
        logger.warning(f"GET {url}: {e}")
        return None


# ---------------------------------------------------------------------------
# Parsers de campos
# ---------------------------------------------------------------------------

def _parse_date(text: str) -> Optional[str]:
    if not text:
        return None
    t = re.sub(r"\s+", " ", text.strip()).lower()
    t = re.sub(r"\b(seg|ter|qua|qui|sex|s[aá]b|dom)\b\.?", "", t).strip()
    t = t.replace(",", "").strip()
    m = re.match(r"(\d{1,2})\s+([a-záéíóúç]+)(?:\s+(\d{4}))?", t)
    if m:
        day = m.group(1).zfill(2)
        raw_month = m.group(2)
        month = MONTH_PT.get(raw_month[:3]) or MONTH_PT.get(raw_month)
        year = m.group(3) or str(datetime.now().year)
        if month:
            return f"{year}-{month}-{day}"
    return None


def _parse_time(text: str) -> Optional[str]:
    if not text:
        return None
    m = re.search(r"\b(\d{1,2})[h:](\d{2})\b", text.lower())
    if m:
        return f"{int(m.group(1)):02d}:{m.group(2)}"
    m = re.search(r"\b(\d{1,2})h\b", text.lower())
    if m:
        return f"{int(m.group(1)):02d}:00"
    return None


def _parse_price(text: str) -> Optional[dict]:
    if not text:
        return None
    if re.search(r"\bentrada\s+livre\b|\bgratuito\b|\bfree\b", text, re.I):
        return {"is_free": True, "price_min": 0.0, "price_display": "Entrada livre"}
    prices = re.findall(r"(\d+(?:[.,]\d+)?)\s*€", text)
    if prices:
        nums = [float(p.replace(",", ".")) for p in prices]
        result = {"is_free": False, "price_min": min(nums), "price_display": text.strip()}
        if len(nums) > 1:
            result["price_max"] = max(nums)
        return result
    return None


def _parse_duration(text: str) -> Optional[int]:
    if not text:
        return None
    m = re.search(r"(\d+)\s*h\s*(\d+)?", text.lower())
    if m:
        return int(m.group(1)) * 60 + (int(m.group(2)) if m.group(2) else 0)
    m = re.search(r"(\d+)\s*min", text.lower())
    if m:
        return int(m.group(1))
    return None


# ---------------------------------------------------------------------------
# ESTRATÉGIA 1: filter_list_url
# ---------------------------------------------------------------------------

def _fetch_via_filter(session: requests.Session) -> List[str]:
    """
    Tenta obter lista de eventos via /pt/programacao/filtrar/
    Este endpoint pode devolver JSON com URLs ou HTML de fragmento.
    Tenta também o schedule/events/ com parâmetro 'from'.
    """
    urls = []
    today = date.today().strftime("%Y-%m-%d")

    candidates = [
        # filter_list_url sem parâmetros
        (FILTER_URL, {}),
        # filter_list_url com data
        (FILTER_URL, {"from": today}),
        # schedule/events com data
        (SCHEDULE_URL, {"from": today}),
        # schedule/events com data e page
        (SCHEDULE_URL, {"from": today, "page": 1}),
        # schedule/events com typology (força modo fragmento)
        (SCHEDULE_URL, {"typology": ""}),
    ]

    for base_url, params in candidates:
        time.sleep(REQUEST_DELAY)
        resp = _get(session, base_url, params=params, headers=HEADERS_JSON)
        if not resp:
            continue

        ct = resp.headers.get("Content-Type", "")
        size = len(resp.text)
        logger.info(f"Filter probe {base_url} {params} → {size} chars | {ct[:40]}")

        # Se for JSON → extrair URLs
        if "application/json" in ct:
            try:
                data = resp.json()
                logger.info(f"  JSON keys: {list(data.keys())[:8]}")
                # Tentar extrair eventos de estruturas comuns
                for key in ("events", "results", "items", "data", "html"):
                    val = data.get(key)
                    if isinstance(val, list):
                        for item in val:
                            if isinstance(item, dict):
                                for url_key in ("url", "link", "href", "slug"):
                                    u = item.get(url_key, "")
                                    if u and "/programacao/" in u:
                                        full = u if u.startswith("http") else f"{WEBSITE}{u}"
                                        if _is_event_url(full):
                                            urls.append(full)
                    elif isinstance(val, str) and len(val) > 100:
                        # HTML embutido em JSON
                        found = _extract_event_links_from_html(val)
                        urls.extend(found)
                        logger.info(f"  HTML em JSON → {len(found)} links")
                if urls:
                    logger.info(f"  {len(urls)} URLs via JSON")
                    return list(dict.fromkeys(urls))
            except Exception as e:
                logger.debug(f"  Erro JSON: {e}")

        # Se for HTML pequeno (< 50 KB) → provavelmente fragmento de eventos
        elif size < 50_000:
            found = _extract_event_links_from_html(resp.text)
            if found:
                logger.info(f"  Fragmento HTML → {len(found)} links")
                urls.extend(found)
                if urls:
                    return list(dict.fromkeys(urls))

        # HTML grande (>= 50 KB) → página completa, ignorar para extracção de listing
        else:
            logger.debug(f"  HTML completo ({size} chars) — ignorar para listing")

    return list(dict.fromkeys(urls))


# ---------------------------------------------------------------------------
# ESTRATÉGIA 2: Sitemap XML
# ---------------------------------------------------------------------------

def _fetch_via_sitemap(session: requests.Session) -> List[str]:
    """
    Descobre e parseia sitemap.xml para obter URLs de eventos.
    Tenta robots.txt primeiro para descobrir o sitemap correcto.
    """
    urls = []

    # Tentar descobrir sitemap via robots.txt
    sitemap_locations = list(SITEMAP_URLS)
    robots_resp = _get(session, f"{WEBSITE}/robots.txt", headers=HEADERS_HTML)
    if robots_resp:
        for line in robots_resp.text.splitlines():
            if line.lower().startswith("sitemap:"):
                loc = line.split(":", 1)[1].strip()
                if loc not in sitemap_locations:
                    sitemap_locations.insert(0, loc)
                    logger.info(f"Sitemap descoberto via robots.txt: {loc}")

    for sitemap_url in sitemap_locations:
        if sitemap_url.endswith("robots.txt"):
            continue
        time.sleep(REQUEST_DELAY)
        resp = _get(session, sitemap_url, headers=HEADERS_HTML)
        if not resp:
            continue

        ct = resp.headers.get("Content-Type", "")
        logger.info(f"Sitemap {sitemap_url} → {len(resp.text)} chars | {ct[:40]}")

        soup = BeautifulSoup(resp.text, "lxml-xml")

        # Sitemapindex: contém links para outros sitemaps
        for sitemap_ref in soup.find_all("sitemap"):
            loc = sitemap_ref.find("loc")
            if loc:
                sub_url = loc.get_text(strip=True)
                if "programacao" in sub_url or "events" in sub_url or "programm" in sub_url:
                    logger.info(f"  Sub-sitemap: {sub_url}")
                    time.sleep(REQUEST_DELAY)
                    sub_resp = _get(session, sub_url, headers=HEADERS_HTML)
                    if sub_resp:
                        sub_soup = BeautifulSoup(sub_resp.text, "lxml-xml")
                        for url_el in sub_soup.find_all("url"):
                            loc2 = url_el.find("loc")
                            if loc2:
                                candidate = loc2.get_text(strip=True)
                                if _is_event_url(candidate):
                                    urls.append(candidate)

        # Sitemap normal: lista de URLs directamente
        for url_el in soup.find_all("url"):
            loc = url_el.find("loc")
            if loc:
                candidate = loc.get_text(strip=True)
                if _is_event_url(candidate):
                    urls.append(candidate)

        if urls:
            logger.info(f"Sitemap: {len(urls)} URLs de eventos encontrados")
            return list(dict.fromkeys(urls))

    return urls


# ---------------------------------------------------------------------------
# ESTRATÉGIA 3: schedule/events/ paginado com parâmetros correctos
# ---------------------------------------------------------------------------

def _fetch_via_schedule_paginated(session: requests.Session) -> List[str]:
    """
    Tenta o schedule endpoint com múltiplas combinações de parâmetros
    até encontrar uma que devolva fragmento HTML com eventos.
    Após encontrar o padrão correcto, pagina até não haver mais eventos.
    """
    today = date.today().strftime("%Y-%m-%d")
    all_urls: List[str] = []
    seen_sets: List[frozenset] = []

    # Detectar o padrão de parâmetros correcto
    working_params = None
    probe_variants = [
        {"from": today},
        {"from": today, "page": 1},
        {"start": today},
        {"date": today},
        {"offset": 0},
        {},  # sem parâmetros mas com headers JSON
    ]

    for variant in probe_variants:
        time.sleep(REQUEST_DELAY)
        resp = _get(session, SCHEDULE_URL, params=variant, headers=HEADERS_JSON)
        if not resp:
            continue
        size = len(resp.text)
        ct = resp.headers.get("Content-Type", "")
        logger.info(f"Schedule probe {variant} → {size} chars | {ct[:30]}")

        if size < 50_000:  # fragmento, não página completa
            found = _extract_event_links_from_html(resp.text)
            if found:
                logger.info(f"  ✓ Padrão encontrado: {variant} → {len(found)} links")
                working_params = variant
                all_urls.extend(found)
                seen_sets.append(frozenset(_normalize_url(u) for u in found))
                break

    if working_params is None:
        logger.warning("Schedule: nenhum parâmetro produziu fragmento com eventos")
        return all_urls

    # Paginar com o padrão encontrado
    page = 2
    page_key = next((k for k in ("page", "offset") if k in working_params), None)

    while page <= MAX_PAGES:
        time.sleep(REQUEST_DELAY)

        params = dict(working_params)
        if page_key == "page":
            params["page"] = page
        elif page_key == "offset":
            params["offset"] = (page - 1) * 20  # assumir 20 por página
        else:
            # Sem chave de paginação conhecida → tentar adicionar page=
            params["page"] = page

        resp = _get(session, SCHEDULE_URL, params=params, headers=HEADERS_JSON)
        if not resp or len(resp.text) < 100:
            break

        found = _extract_event_links_from_html(resp.text)
        page_set = frozenset(_normalize_url(u) for u in found)

        if not found or page_set in seen_sets:
            logger.info(f"Schedule pág {page}: fim da paginação")
            break

        seen_sets.append(page_set)
        all_urls.extend(found)
        logger.info(f"Schedule pág {page}: {len(found)} links ({len(all_urls)} total)")
        page += 1

    return list(dict.fromkeys(_normalize_url(u) for u in all_urls))


# ---------------------------------------------------------------------------
# Extracção de links de HTML (usado pelas 3 estratégias)
# ---------------------------------------------------------------------------

def _extract_event_links_from_html(html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    links = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        full = href if href.startswith("http") else f"{WEBSITE}{href}"
        if not _is_event_url(full):
            continue
        norm = _normalize_url(full)
        if norm not in seen:
            seen.add(norm)
            links.append(full)
    return links


# ---------------------------------------------------------------------------
# Orquestrador de listing
# ---------------------------------------------------------------------------

def _get_event_urls(session: requests.Session) -> List[str]:
    """
    Tenta as 3 estratégias por ordem e devolve a primeira que produz resultados.
    Regista diagnóstico detalhado para facilitar debugging futuro.
    """
    # Estratégia 1: filter_list_url / schedule com parâmetros
    logger.info("=== Estratégia 1: filter endpoint ===")
    urls = _fetch_via_filter(session)
    if urls:
        logger.info(f"Estratégia 1 bem-sucedida: {len(urls)} URLs")
        return urls

    # Estratégia 2: Sitemap
    logger.info("=== Estratégia 2: sitemap XML ===")
    urls = _fetch_via_sitemap(session)
    if urls:
        logger.info(f"Estratégia 2 bem-sucedida: {len(urls)} URLs")
        return urls

    # Estratégia 3: schedule paginado com probe de parâmetros
    logger.info("=== Estratégia 3: schedule paginado ===")
    urls = _fetch_via_schedule_paginated(session)
    if urls:
        logger.info(f"Estratégia 3 bem-sucedida: {len(urls)} URLs")
        return urls

    logger.error(
        "TODAS AS ESTRATÉGIAS FALHARAM. "
        "O site pode estar a usar SPA pura (client-side rendering). "
        "Considerar: Playwright/Selenium para render JS, ou inspecção "
        "manual do tráfego de rede para descobrir o endpoint real."
    )
    return []


# ---------------------------------------------------------------------------
# Parser de datas
# ---------------------------------------------------------------------------

def _parse_dates_block(soup: BeautifulSoup) -> List[dict]:
    sessions = []
    date_block = (
        soup.select_one(".description-aside .event-info-block.date")
        or soup.select_one(".event-info-block.date")
    )
    if not date_block:
        return sessions
    for br in date_block.find_all("br"):
        br.replace_with("\n")
    paragraphs = date_block.find_all("p")
    if paragraphs:
        for p in paragraphs:
            lines = [l.strip() for l in p.get_text().splitlines() if l.strip()]
            _extract_sessions_from_lines(lines, sessions)
    else:
        raw = date_block.get_text(separator="\n", strip=True)
        lines = [l.strip() for l in raw.splitlines() if l.strip()]
        _extract_sessions_from_lines(lines, sessions)
    return sessions


def _extract_sessions_from_lines(lines: List[str], sessions: List[dict]) -> None:
    current_date = None
    current_time = None
    for line in lines:
        d = _parse_date(line)
        if d:
            if current_date:
                sessions.append({"date": current_date, "time_start": current_time})
            current_date = d
            current_time = _parse_time(line)
        else:
            t = _parse_time(line)
            if t and current_date:
                current_time = t
    if current_date:
        sessions.append({"date": current_date, "time_start": current_time})


# ---------------------------------------------------------------------------
# Parser de ficha técnica
# ---------------------------------------------------------------------------

def _parse_technical_info(soup: BeautifulSoup) -> Optional[str]:
    tech = soup.select_one(".detail-extras-technical-info")
    if not tech:
        return None
    parts = []
    for col in tech.select(".column"):
        items = col.find_all("p")
        i = 0
        while i < len(items):
            p = items[i]
            is_label = (
                "subtitle-paragraph" in p.get("class", [])
                or "font-weight:bold" in p.get("style", "")
                or "font-weight: bold" in p.get("style", "")
            )
            if is_label and i + 1 < len(items):
                label = p.get_text(strip=True)
                value = items[i + 1].get_text(strip=True)
                if label and value and value.strip() not in ("", "\xa0", "&nbsp;"):
                    parts.append(f"{label}: {value}")
                i += 2
            else:
                i += 1
    return " | ".join(parts) if parts else None


# ---------------------------------------------------------------------------
# Parser de evento individual
# ---------------------------------------------------------------------------

def _parse_single_event(url: str, session: requests.Session) -> Optional[dict]:
    resp = _get(session, url, headers=HEADERS_HTML)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "lxml")

    # Título
    title = ""
    h1 = soup.select_one(".event-detail-header h1")
    if not h1:
        h1 = soup.find("h1")
    if h1:
        title = h1.get_text(strip=True)
    if not title:
        meta = soup.find("meta", property="og:title")
        if meta:
            title = meta.get("content", "").split("|")[0].strip()
    if len(title) < 2:
        return None

    # Subtítulo
    subtitle = None
    for sel in [".event-detail-header .subtitle", ".description > .subtitle"]:
        sub = soup.select_one(sel)
        if sub:
            candidate = sub.get_text(strip=True)
            if candidate and candidate != title:
                subtitle = candidate
                break

    # Categorias
    categories = [
        a.get_text(strip=True)
        for a in soup.select(".event-types .type")
        if a.get_text(strip=True)
    ]

    # Descrição
    desc = None
    desc_el = soup.select_one(".text-plugin")
    if desc_el:
        desc = desc_el.get_text(separator="\n", strip=True) or None

    # Datas
    sessions = _parse_dates_block(soup)

    # Highlight: preço, duração, sala, classificação
    price_raw = duration_minutes = space = age_rating = None
    highlight = soup.select_one(".description-aside .event-info-block.highlight")
    if highlight:
        for br in highlight.find_all("br"):
            br.replace_with("\n")
        for line in highlight.get_text().splitlines():
            line = line.strip()
            if not line:
                continue
            if "€" in line or re.search(r"entrada\s+livre|gratuito", line, re.I):
                price_raw = price_raw or line
            elif re.search(r"dura[çc][aã]o", line, re.I):
                duration_minutes = duration_minutes or _parse_duration(line)
            elif re.search(
                r"audit[oó]rio|grande\s+audit|pequeno\s+audit|est[uú]dio|"
                r"black\s*box|palco|sala\s*\d",
                line, re.I
            ):
                space = space or line
            elif re.search(r"\bm\s*/\s*\d+\b|\bm\s*\+\s*\d+\b", line, re.I):
                age_rating = age_rating or line

    price = _parse_price(price_raw) or {}

    # Bilheteira
    ticketing_url = None
    btn = soup.select_one("a.event-tickets-btn[href]")
    if btn:
        href = btn["href"]
        ticketing_url = href if href.startswith("http") else f"{WEBSITE}{href}"

    # Imagem
    cover_image = None
    og_img = soup.find("meta", property="og:image")
    if og_img:
        cover_image = og_img.get("content")

    # Ficha técnica
    credits_raw = _parse_technical_info(soup)

    # Acessibilidade
    full_text = soup.get_text(" ", strip=True).lower()
    accessibility = {
        "has_sign_language": bool(re.search(
            r"l[íi]ngua\s+gestual|interpreta[çc][aã]o\s+gestual|\blgp\b", full_text
        )),
        "has_audio_description": bool(re.search(
            r"audiodescrição|audiodescricao|[aá]udio.?descri", full_text
        )),
        "has_subtitles": bool(re.search(r"\blegendas?\b|\bsubtitle", full_text)),
        "is_relaxed_performance": bool(re.search(
            r"sess[aã]o\s+relaxada|relaxed\s+performance", full_text
        )),
        "wheelchair_accessible": True,
        "has_pre_show_access": bool(re.search(
            r"acesso\s+pré.espetáculo|pre.?show\s+access", full_text
        )),
    }

    n = len(sessions)
    logger.info(f"✓ {title[:65]} ({n} sess{'ão' if n == 1 else 'ões'})")

    return {
        "source_id": _normalize_url(url).rstrip("/").split("/")[-1],
        "source_url": url,
        "title": title,
        "subtitle": subtitle,
        "description": desc,
        "categories": categories,
        "dates": sessions,
        "date_open": sessions[0]["date"] if sessions else None,
        "price_raw": price_raw,
        "price": price,
        "duration_minutes": duration_minutes,
        "space": space,
        "age_rating": age_rating,
        "ticketing_url": ticketing_url,
        "cover_image": cover_image,
        "credits_raw": credits_raw,
        "location": "Culturgest",
        "accessibility": accessibility,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "_method": "culturgest-v14",
    }


# ---------------------------------------------------------------------------
# Entrada principal
# ---------------------------------------------------------------------------

def run() -> List[dict]:
    session = _make_session()
    logger.info("CULTURGEST v14 — estratégia em cascata")

    event_urls = _get_event_urls(session)

    if not event_urls:
        logger.error("Sem URLs de eventos. Verificar logs acima para diagnóstico.")
        return []

    logger.info(f"{len(event_urls)} URLs únicos a processar")

    all_events = []
    seen = set()
    for url in event_urls:
        norm = _normalize_url(url)
        if norm in seen:
            continue
        seen.add(norm)
        time.sleep(REQUEST_DELAY)
        ev = _parse_single_event(url, session)
        if ev:
            all_events.append(ev)

    logger.info(f"Total recolhido: {len(all_events)} eventos")
    return all_events


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    events = run()
    print(f"\n=== TOTAL FINAL: {len(events)} eventos ===\n")
    if events:
        print(_json.dumps(events[0], indent=2, ensure_ascii=False))
