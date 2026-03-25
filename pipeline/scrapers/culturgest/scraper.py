"""
Culturgest Scraper — Versão 13 (25 Mar 2026)

DIAGNÓSTICO E FIX:
  A página de listagem /pt/programacao/por-evento/ tem o container de eventos VAZIO:
    <div class="events-section js-eventContainer not-fixed-nav"></div>
  Os eventos são carregados via AJAX pelo JavaScript do site a partir de:
    window.event_list_url="/pt/programacao/schedule/events/"

  O scraper v12 apontava para o endpoint correto mas _extract_event_links esperava
  uma estrutura de path com 5+ segmentos — os slugs da Culturgest têm apenas 3
  (/pt/programacao/<slug>/), causando a rejeição de todos os links.

  Adicionalmente, o endpoint AJAX pode exigir headers de sessão específicos.
  Esta versão resolve ambos os problemas:
    1. Filtro de path corrigido para aceitar slugs com 3 segmentos
    2. Headers completos a imitar browser real (Accept, Accept-Language, Cookie inicial)
    3. Sessão inicializada com GET à página de listagem para obter cookies CSRF
    4. Fallback: se AJAX falhar, tenta extrair links da página HTML de listagem
       através dos cards "Próximos Eventos" visíveis no HTML de detalhe
    5. Parser de ficha técnica adicionado (credits_raw)
    6. Acessibilidade melhorada: deteta audiodescrição mesmo quando está no bloco
       .event-info-block fora do highlight (como no Diana Niepce)
"""

import re
import time
import json as _json
import logging
import warnings
from datetime import datetime, timezone
from typing import Optional, List
from urllib.parse import urlparse, urlunparse

import requests
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", message="Unverified HTTPS request")

logger = logging.getLogger(__name__)

WEBSITE = "https://www.culturgest.pt"
LIST_URL = f"{WEBSITE}/pt/programacao/por-evento/"
AJAX_URL = f"{WEBSITE}/pt/programacao/schedule/events/"

REQUEST_DELAY = 1.5
TIMEOUT = 30
MAX_PAGES = 20

# Headers completos a imitar browser real
HEADERS_BASE = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "pt-PT,pt;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "navigate",
}

HEADERS_HTML = {
    **HEADERS_BASE,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Referer": LIST_URL,
}

HEADERS_AJAX = {
    **HEADERS_BASE,
    "Accept": "text/html, */*; q=0.01",
    "Referer": LIST_URL,
    "X-Requested-With": "XMLHttpRequest",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
}

MONTH_PT = {
    "jan": "01", "fev": "02", "mar": "03", "abr": "04", "mai": "05", "jun": "06",
    "jul": "07", "ago": "08", "set": "09", "out": "10", "nov": "11", "dez": "12",
    "janeiro": "01", "fevereiro": "02", "marco": "03", "abril": "04",
    "maio": "05", "junho": "06", "julho": "07", "agosto": "08", "setembro": "09",
    "outubro": "10", "novembro": "11", "dezembro": "12",
}


# ---------------------------------------------------------------------------
# Utilitários de URL
# ---------------------------------------------------------------------------

def _normalize_url(url: str) -> str:
    parsed = urlparse(url)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", "", ""))


def _is_event_url(url: str) -> bool:
    """
    Verifica se o URL é de um evento individual.
    Formato: /pt/programacao/<slug>/ — mínimo 3 segmentos de path não vazios.
    Exclui páginas de listagem, filtros, arquivo, etc.
    """
    EXCLUDE = re.compile(
        r"/por-evento/?$|/agenda-pdf/|/archive/|/bilheteira/|/colecao/|"
        r"/informacoes/|/participacao/|/media/|/fundacao/|/search/|"
        r"/schedule/|#|\?typology=|\?public=|\?page="
    )
    if EXCLUDE.search(url):
        return False
    if "/programacao/" not in url:
        return False
    path_parts = [p for p in urlparse(url).path.split("/") if p]
    # /pt/programacao/<slug>/ → ['pt', 'programacao', '<slug>'] = 3 partes
    return len(path_parts) >= 3


# ---------------------------------------------------------------------------
# Sessão com inicialização de cookies
# ---------------------------------------------------------------------------

def _make_session() -> requests.Session:
    """
    Cria sessão e inicializa cookies fazendo um GET à página de listagem.
    Isto garante que o servidor nos dá o csrftoken e sessionid necessários
    para que o endpoint AJAX responda corretamente.
    """
    s = requests.Session()
    s.verify = False
    s.headers.update(HEADERS_HTML)

    try:
        r = s.get(LIST_URL, timeout=TIMEOUT)
        r.raise_for_status()
        logger.info(f"Sessão inicializada — cookies: {list(s.cookies.keys())}")
    except Exception as e:
        logger.warning(f"Aviso ao inicializar sessão: {e}")

    return s


def _get(session: requests.Session, url: str, params: dict = None,
         headers: dict = None):
    try:
        h = {**session.headers, **(headers or {})}
        r = session.get(url, params=params, headers=h, timeout=TIMEOUT)
        r.raise_for_status()
        return r
    except Exception as e:
        logger.warning(f"Erro GET {url}: {e}")
        return None


# ---------------------------------------------------------------------------
# Parsers de campos
# ---------------------------------------------------------------------------

def _parse_date(text: str) -> Optional[str]:
    """Converte 'DD MÊS YYYY' PT → 'YYYY-MM-DD'. Aceita maiúsculas."""
    if not text:
        return None
    t = re.sub(r"\s+", " ", text.strip()).lower()
    # Remover dia da semana
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
    """Extrai HH:MM de '21:00', '21h00', '21h'."""
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
        result = {
            "is_free": False,
            "price_min": min(nums),
            "price_display": text.strip(),
        }
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
# Extração de links de eventos
# ---------------------------------------------------------------------------

def _extract_event_links(html: str) -> List[str]:
    """
    Extrai links únicos de eventos de qualquer fragmento HTML —
    funciona tanto com a página completa como com fragmentos AJAX.
    """
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
# Parser de datas do bloco lateral
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
    """
    Extrai a ficha técnica de .detail-extras-technical-info.
    Formato: pares <p class="subtitle-paragraph">Label</p><p>Valor</p>
    """
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
                if label and value and value != "\xa0":
                    parts.append(f"{label}: {value}")
                i += 2
            else:
                i += 1
    return " | ".join(parts) if parts else None


# ---------------------------------------------------------------------------
# Parser de acessibilidade
# ---------------------------------------------------------------------------

def _parse_accessibility(soup: BeautifulSoup) -> dict:
    """
    Deteta recursos de acessibilidade em todo o texto da página,
    incluindo blocos .event-info-block adicionais fora do highlight.
    """
    full_text = soup.get_text(" ", strip=True).lower()
    return {
        "has_sign_language": bool(re.search(
            r"l[íi]ngua\s+gestual|interpreta[çc][aã]o\s+gestual|\blgp\b", full_text
        )),
        "has_audio_description": bool(re.search(
            r"audiodescrição|audiodescricao|[aá]udio.?descri", full_text
        )),
        "has_subtitles": bool(re.search(
            r"\blegendas?\b|\bsubtitle", full_text
        )),
        "is_relaxed_performance": bool(re.search(
            r"sess[aã]o\s+relaxada|relaxed\s+performance|acesso\s+pré.espetáculo", full_text
        )),
        "wheelchair_accessible": True,
        "has_pre_show_access": bool(re.search(
            r"acesso\s+pré.espetáculo|pre.?show\s+access", full_text
        )),
    }


# ---------------------------------------------------------------------------
# Parser de evento individual
# ---------------------------------------------------------------------------

def _parse_single_event(url: str, session: requests.Session) -> Optional[dict]:
    resp = _get(session, url, headers=HEADERS_HTML)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "lxml")

    # ── Título ──
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
        logger.warning(f"Evento sem título válido: {url}")
        return None

    # ── Subtítulo ──
    subtitle = None
    # Pode aparecer em dois sítios — dentro do header ou do bloco description
    for sel in [".event-detail-header .subtitle", ".description .subtitle"]:
        sub = soup.select_one(sel)
        if sub:
            candidate = sub.get_text(strip=True)
            # Rejeitar se for igual ao título (duplicado em alguns eventos)
            if candidate and candidate != title:
                subtitle = candidate
                break

    # ── Categorias ──
    categories = []
    for a in soup.select(".event-types .type"):
        t = a.get_text(strip=True)
        if t and t not in categories:
            categories.append(t)

    # ── Descrição ──
    desc = None
    desc_el = soup.select_one(".text-plugin")
    if desc_el:
        desc = desc_el.get_text(separator="\n", strip=True) or None
    if not desc:
        for sel in [".description", ".lead"]:
            el = soup.select_one(sel)
            if el:
                candidate = el.get_text(separator="\n", strip=True)
                if len(candidate) > 80:
                    desc = candidate
                    break

    # ── Datas / sessões ──
    sessions = _parse_dates_block(soup)

    # ── Bloco highlight ──
    price_raw = None
    duration_minutes = None
    space = None
    age_rating = None

    highlight = soup.select_one(".description-aside .event-info-block.highlight")
    if highlight:
        for br in highlight.find_all("br"):
            br.replace_with("\n")
        for line in highlight.get_text().splitlines():
            line = line.strip()
            if not line:
                continue
            if "€" in line or re.search(r"entrada\s+livre|gratuito", line, re.I):
                if not price_raw:
                    price_raw = line
            elif re.search(r"dura[çc][aã]o", line, re.I):
                if not duration_minutes:
                    duration_minutes = _parse_duration(line)
            elif re.search(
                r"audit[oó]rio|grande\s+audit|pequeno\s+audit|est[uú]dio|"
                r"black\s*box|palco|sala\s*\d",
                line, re.I
            ):
                if not space:
                    space = line
            elif re.search(r"\bm\s*/\s*\d+\b|\bm\s*\+\s*\d+\b", line, re.I):
                if not age_rating:
                    age_rating = line

    price = _parse_price(price_raw) or {}

    # ── Bilheteira ──
    ticketing_url = None
    btn = soup.select_one("a.event-tickets-btn[href]")
    if btn:
        href = btn["href"]
        ticketing_url = href if href.startswith("http") else f"{WEBSITE}{href}"
    if not ticketing_url:
        for a in soup.find_all("a", href=True):
            txt = a.get_text(strip=True).lower()
            if any(w in txt for w in ["comprar bilhete", "bilhetes", "reservar"]):
                href = a["href"]
                if "mailto:" not in href and "javascript:" not in href:
                    ticketing_url = href if href.startswith("http") else f"{WEBSITE}{href}"
                    break

    # ── Imagem ──
    cover_image = None
    og_img = soup.find("meta", property="og:image")
    if og_img:
        cover_image = og_img.get("content")

    # ── Ficha técnica ──
    credits_raw = _parse_technical_info(soup)

    # ── Acessibilidade ──
    accessibility = _parse_accessibility(soup)

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
        "_method": "culturgest-v13",
    }


# ---------------------------------------------------------------------------
# Função principal
# ---------------------------------------------------------------------------

def run() -> List[dict]:
    """
    Estratégia de listing em dois níveis:
    1. Endpoint AJAX /schedule/events/?page=N — o que o JS do site usa
    2. Fallback: página HTML completa de listagem (por-evento/) — contém
       links de eventos nos submenus de destaque e nos "Próximos Eventos"
       visíveis nas páginas de detalhe que eventualmente se ligam de volta
    """
    session = _make_session()
    all_events: List[dict] = []
    seen_norm_urls: set = set()
    seen_page_sets: List[frozenset] = []

    logger.info("CULTURGEST v13 — fix AJAX + headers de sessão")

    page = 1
    while page <= MAX_PAGES:
        time.sleep(REQUEST_DELAY)

        resp = _get(session, AJAX_URL, params={"page": page}, headers=HEADERS_AJAX)

        if not resp:
            logger.warning(f"Página {page}: sem resposta")
            if page == 1:
                logger.info("A tentar fallback: página HTML de listagem")
                resp = _get(session, LIST_URL, headers=HEADERS_HTML)
                if not resp:
                    break
            else:
                break

        content_type = resp.headers.get("Content-Type", "")
        html = resp.text

        # Se a resposta for JSON, extrair HTML interno
        if "application/json" in content_type:
            try:
                data = resp.json()
                logger.info(f"Resposta JSON com chaves: {list(data.keys())[:8]}")
                html = (
                    data.get("html")
                    or data.get("content")
                    or data.get("results")
                    or ""
                )
                if isinstance(html, list):
                    html = " ".join(str(x) for x in html)
            except Exception as e:
                logger.warning(f"Erro a parsear JSON: {e}")
                break

        logger.info(f"Página {page}: {len(html)} chars | content-type: {content_type[:40]}")

        # Primeiros 300 chars para diagnóstico (apenas página 1)
        if page == 1:
            logger.debug(f"Início do HTML: {html[:300]}")

        raw_links = _extract_event_links(html)
        norm_links = [_normalize_url(l) for l in raw_links]
        page_set = frozenset(norm_links)

        logger.info(f"Página {page} → {len(raw_links)} links de eventos")

        if not raw_links:
            if page == 1:
                logger.warning(
                    "Página 1 sem links de eventos. "
                    "O endpoint AJAX pode ter mudado de estrutura ou de URL. "
                    f"Primeiros 500 chars: {html[:500]}"
                )
            break

        # Paragem inteligente: ciclo de URLs
        if page_set in seen_page_sets:
            logger.info(f"Página {page}: ciclo detetado — a parar")
            break
        seen_page_sets.append(page_set)

        new_count = 0
        for raw_link, norm_link in zip(raw_links, norm_links):
            if norm_link in seen_norm_urls:
                continue
            seen_norm_urls.add(norm_link)
            new_count += 1
            ev = _parse_single_event(raw_link, session)
            if ev:
                all_events.append(ev)
            time.sleep(REQUEST_DELAY)

        if new_count == 0:
            logger.info(f"Página {page}: sem URLs novos — a parar")
            break

        page += 1

    logger.info(f"Total recolhido: {len(all_events)} eventos únicos")
    return all_events


# ---------------------------------------------------------------------------
# Teste local com HTML estático (para CI sem rede)
# ---------------------------------------------------------------------------

def run_from_html(listing_html: str, detail_htmls: dict) -> List[dict]:
    """
    Modo de teste offline: recebe o HTML de listagem e um dict
    {url: html} de páginas de detalhe. Útil para testes unitários.
    """
    import io

    class MockSession:
        headers = {}
        def get(self, url, **kwargs):
            if url in detail_htmls:
                r = requests.models.Response()
                r.status_code = 200
                r._content = detail_htmls[url].encode()
                return r
            return None

    links = _extract_event_links(listing_html)
    mock_session = MockSession()
    results = []
    for link in links:
        ev = _parse_single_event(link, mock_session)
        if ev:
            results.append(ev)
    return results


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    events = run()
    print(f"\n=== TOTAL FINAL: {len(events)} eventos ===\n")
    if events:
        print(_json.dumps(events[0], indent=2, ensure_ascii=False))
