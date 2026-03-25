"""
Culturgest Scraper — Versão 12 (25 Mar 2026)
Melhorias face à v11:
  - Normalização de URLs antes de deduplicar (remove query params e trailing slash)
  - Paragem inteligente corrigida: compara page_sets com URLs normalizados
  - MAX_PAGES reduzido de 50 para 5 (failsafe; paragem real é por ciclo)
  - _parse_date: aceita abreviações de mês em maiúsculas (ABR, MAI, etc.)
  - _parse_dates_block: robusto a formatos alternativos de datas
  - _parse_single_event: extração de space, age_rating, duration_minutes melhorada
  - Acessibilidade: detecta mais variantes textuais
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
EVENT_LIST_URL = f"{WEBSITE}/pt/programacao/schedule/events/"

REQUEST_DELAY = 1.0
TIMEOUT = 25
MAX_PAGES = 5  # failsafe reduzido — paragem real é por ciclo de URLs normalizados

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": f"{WEBSITE}/pt/programacao/por-evento/",
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
    """
    Remove query string, fragmento e trailing slash para comparação.
    Garante que o mesmo evento com URLs ligeiramente diferentes (ex: ?ref=schedule,
    ?page=2) seja tratado como duplicado.
    """
    parsed = urlparse(url)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", "", ""))


def _make_session() -> requests.Session:
    s = requests.Session()
    s.verify = False
    s.headers.update(HEADERS)
    return s


def _get(session: requests.Session, url: str, params: dict = None):
    try:
        r = session.get(url, params=params, timeout=TIMEOUT)
        r.raise_for_status()
        return r
    except Exception as e:
        logger.warning(f"Erro {url}: {e}")
        return None


# ---------------------------------------------------------------------------
# Parsers de campos
# ---------------------------------------------------------------------------

def _parse_date(text: str) -> Optional[str]:
    """Converte texto de data PT para YYYY-MM-DD. Aceita maiúsculas e minúsculas."""
    if not text:
        return None
    # Normalizar: lowercase, colapsar espaços
    t = re.sub(r"\s+", " ", text.strip()).lower()
    # Remover nome do dia da semana (seg, ter, qua, qui, sex, sab, dom)
    t = re.sub(r"\b(seg|ter|qua|qui|sex|s[aá]b|dom)\b\.?", "", t).strip()
    # Remover vírgulas e pontos soltos
    t = t.replace(",", "").strip()
    # Tentar match: DD MÊS [YYYY]
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
    """Extrai HH:MM de texto. Aceita '21:00', '21h00', '21h'."""
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
    """Extrai estrutura de preço de texto como '15€ (descontos)' ou 'Entrada livre'."""
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
    """Extrai duração em minutos de 'Duração 1h30', '1h50', '90 min'."""
    if not text:
        return None
    m = re.search(r"(\d+)\s*h\s*(\d+)?", text.lower())
    if m:
        hours = int(m.group(1))
        mins = int(m.group(2)) if m.group(2) else 0
        return hours * 60 + mins
    m = re.search(r"(\d+)\s*min", text.lower())
    if m:
        return int(m.group(1))
    return None


# ---------------------------------------------------------------------------
# Extração de links de eventos
# ---------------------------------------------------------------------------

def _extract_event_links(html: str) -> List[str]:
    """
    Extrai links de eventos do HTML da página de listagem.
    Filtra links de navegação (agenda, arquivo, por-evento, etc.).
    """
    soup = BeautifulSoup(html, "lxml")
    links = []
    seen_local = set()

    EXCLUDE_PATTERNS = re.compile(
        r"/por-evento/|/agenda-pdf/|/archive/|/bilheteira/|/colecao/|"
        r"/informacoes/|/participacao/|/media/|/fundacao/|/search/|#"
    )

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if EXCLUDE_PATTERNS.search(href):
            continue
        if "/programacao/" not in href:
            continue
        full = href if href.startswith("http") else f"{WEBSITE}{href}"
        # Garantir que é uma página de evento (pelo menos 6 segmentos de path)
        path_parts = urlparse(full).path.rstrip("/").split("/")
        if len(path_parts) < 5:
            continue
        norm = _normalize_url(full)
        if norm not in seen_local:
            seen_local.add(norm)
            links.append(full)

    return links


# ---------------------------------------------------------------------------
# Parser de datas do bloco lateral
# ---------------------------------------------------------------------------

def _parse_dates_block(soup: BeautifulSoup) -> List[dict]:
    """
    Extrai sessões do bloco lateral de datas.
    Suporta os formatos:
      <p>23 ABR 2026<br/>QUI 21:00</p>
      <p>23 ABR 2026 QUI 21:00</p>
      Texto corrido com múltiplas datas e horas
    """
    sessions = []

    date_block = (
        soup.select_one(".description-aside .event-info-block.date")
        or soup.select_one(".event-info-block.date")
    )
    if not date_block:
        return sessions

    # Substituir <br> por newline antes de extrair texto
    for br in date_block.find_all("br"):
        br.replace_with("\n")

    # Processar por <p> para preservar agrupamentos
    paragraphs = date_block.find_all("p")
    if paragraphs:
        for p in paragraphs:
            lines = [l.strip() for l in p.get_text().splitlines() if l.strip()]
            _extract_sessions_from_lines(lines, sessions)
    else:
        # Fallback: texto corrido
        raw = date_block.get_text(separator="\n", strip=True)
        lines = [l.strip() for l in raw.splitlines() if l.strip()]
        _extract_sessions_from_lines(lines, sessions)

    return sessions


def _extract_sessions_from_lines(lines: List[str], sessions: List[dict]) -> None:
    """
    Processa uma lista de linhas de texto e adiciona sessões (date + time) à lista.
    Cada data inicia uma nova sessão; a hora que se segue é associada a essa data.
    """
    current_date = None
    current_time = None

    for line in lines:
        d = _parse_date(line)
        if d:
            # Guardar sessão anterior se existia
            if current_date:
                sessions.append({"date": current_date, "time_start": current_time})
            current_date = d
            # Hora pode estar na mesma linha ("23 ABR 2026 21:00")
            current_time = _parse_time(line)
        else:
            t = _parse_time(line)
            if t and current_date:
                current_time = t

    if current_date:
        sessions.append({"date": current_date, "time_start": current_time})


# ---------------------------------------------------------------------------
# Parser de evento individual
# ---------------------------------------------------------------------------

def _parse_single_event(url: str, session: requests.Session) -> Optional[dict]:
    resp = _get(session, url)
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
            title = meta.get("content", "").strip()
    if len(title) < 2:
        return None

    # ── Subtítulo ──
    subtitle = None
    sub = soup.select_one(".event-detail-header .subtitle")
    if sub:
        subtitle = sub.get_text(strip=True) or None

    # ── Categorias (tipologia) ──
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
        for sel in [".description", ".lead", "article"]:
            el = soup.select_one(sel)
            if el:
                candidate = el.get_text(separator="\n", strip=True)
                if len(candidate) > 80:
                    desc = candidate
                    break

    # ── Datas / sessões ──
    sessions = _parse_dates_block(soup)

    # ── Bloco highlight: preço, duração, sala, classificação etária ──
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
                r"audit[oó]rio|sala\s*\d|grande\s+audit|est[uú]dio|black\s*box|palco",
                line, re.I
            ):
                if not space:
                    space = line
            elif re.search(r"\bm\s*/\s*\d+\b|\bm\s*\+\s*\d+\b", line, re.I):
                if not age_rating:
                    age_rating = line

    price = _parse_price(price_raw) or {}

    # ── URL de bilheteira ──
    ticketing_url = None
    btn = soup.select_one("a.event-tickets-btn[href]")
    if btn:
        href = btn["href"]
        ticketing_url = href if href.startswith("http") else f"{WEBSITE}{href}"
    if not ticketing_url:
        for a in soup.find_all("a", href=True):
            txt = a.get_text(strip=True).lower()
            if any(w in txt for w in ["comprar bilhete", "bilhetes", "reservar", "buy ticket"]):
                href = a["href"]
                ticketing_url = href if href.startswith("http") else f"{WEBSITE}{href}"
                break

    # ── Imagem de capa ──
    cover_image = None
    og_img = soup.find("meta", property="og:image")
    if og_img:
        cover_image = og_img.get("content")

    # ── Acessibilidade ──
    full_text = soup.get_text(" ", strip=True).lower()
    accessibility = {
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
            r"sess[aã]o\s+relaxada|relaxed\s+performance", full_text
        )),
        "wheelchair_accessible": True,
    }

    n_sessions = len(sessions)
    logger.info(
        f"✓ Extraído: {title[:80]} ({n_sessions} sess{'ão' if n_sessions == 1 else 'ões'})"
    )

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
        "location": "Culturgest",
        "accessibility": accessibility,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "_method": "culturgest-v12",
    }


# ---------------------------------------------------------------------------
# Função principal
# ---------------------------------------------------------------------------

def run() -> List[dict]:
    session = _make_session()
    all_events: List[dict] = []
    seen_norm_urls: set = set()      # URLs normalizados para deduplicação
    seen_page_sets: List[frozenset] = []  # frozensets de URLs normalizados por página

    logger.info("CULTURGEST v12 — extração completa + paragem inteligente (URLs normalizados)")

    page = 1
    while page <= MAX_PAGES:
        time.sleep(REQUEST_DELAY)
        resp = _get(session, EVENT_LIST_URL, {"page": page})
        if not resp:
            logger.warning(f"Página {page}: sem resposta — a parar")
            break

        raw_links = _extract_event_links(resp.text)
        norm_links = [_normalize_url(l) for l in raw_links]
        page_set = frozenset(norm_links)

        logger.info(f"Página {page} → {len(raw_links)} links de eventos")

        if not raw_links:
            logger.info(f"Página {page}: sem links — a parar")
            break

        # Paragem inteligente: este conjunto de URLs já foi visto (ciclo AJAX)
        if page_set in seen_page_sets:
            logger.info(
                f"Página {page}: conjunto de URLs já visto (ciclo AJAX) — a parar"
            )
            break
        seen_page_sets.append(page_set)

        # Processar apenas URLs novos
        new_count = 0
        for raw_link, norm_link in zip(raw_links, norm_links):
            if norm_link in seen_norm_urls:
                continue
            seen_norm_urls.add(norm_link)
            new_count += 1
            ev = _parse_single_event(raw_link, session)
            if ev:
                all_events.append(ev)

        if new_count == 0:
            logger.info(f"Página {page}: sem URLs novos — a parar")
            break

        page += 1

    logger.info(f"Total recolhido: {len(all_events)} eventos únicos")
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
