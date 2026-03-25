"""
Culturgest Scraper — Versão 11 (25 Mar 2026)
Melhorias face à v10:
  - Extração de datas, horas, preço, duração, subtítulo, categorias, acessibilidade
  - Paragem inteligente: para quando uma página não adiciona novos URLs (ciclo AJAX)
  - MAX_PAGES como failsafe apenas
"""

import re
import time
import json as _json
import logging
import warnings
from datetime import datetime, timezone
from typing import Optional, List

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
MAX_PAGES = 50  # failsafe — paragem real é por ciclo de URLs

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
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
def _parse_date(text: str) -> Optional[str]:
    """Converte texto de data PT para YYYY-MM-DD."""
    if not text:
        return None
    t = re.sub(r"\s+", " ", text.strip()).lower()
    # Remove nome do dia semana: "qui", "sex", "sab", etc.
    t = re.sub(r"\b(seg|ter|qua|qui|sex|s[aá]b|dom)\b\.?", "", t).strip()
    m = re.match(r"(\d{1,2})\s+([a-záéíóúç]+)(?:\s+(\d{4}))?", t)
    if m:
        day = m.group(1).zfill(2)
        month_key = m.group(2)[:3]
        month = MONTH_PT.get(month_key) or MONTH_PT.get(m.group(2))
        year = m.group(3) or str(datetime.now().year)
        if month:
            return f"{year}-{month}-{day}"
    return None

def _parse_time(text: str) -> Optional[str]:
    """Extrai HH:MM de texto."""
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
    """Extrai preço de texto como '16€ (descontos)'."""
    if not text:
        return None
    if re.search(r"\bentrada\s+livre\b|\bgratuito\b|\bfree\b", text, re.I):
        return {"is_free": True, "price_min": 0, "price_display": "Entrada livre"}
    prices = re.findall(r"(\d+(?:[.,]\d+)?)\s*€", text)
    if prices:
        nums = [float(p.replace(",", ".")) for p in prices]
        return {
            "is_free": False,
            "price_min": min(nums),
            "price_max": max(nums),
            "price_display": text.strip(),
        }
    return None

def _parse_duration(text: str) -> Optional[int]:
    """Extrai duração em minutos de 'Duração 1h30' ou '90min'."""
    if not text:
        return None
    m = re.search(r"(\d+)h(\d+)?", text.lower())
    if m:
        return int(m.group(1)) * 60 + (int(m.group(2)) if m.group(2) else 0)
    m = re.search(r"(\d+)\s*min", text.lower())
    if m:
        return int(m.group(1))
    return None

# ---------------------------------------------------------------------------
def _extract_event_links(html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    links = []
    seen_local = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if "/programacao/" in href and "/por-evento/" not in href:
            full = href if href.startswith("http") else f"{WEBSITE}{href}"
            if len(full.rstrip("/").split("/")) >= 6 and full not in seen_local:
                seen_local.add(full)
                links.append(full)
    return links

# ---------------------------------------------------------------------------
def _parse_dates_block(soup: BeautifulSoup) -> List[dict]:
    """
    Extrai sessões do bloco lateral de datas.
    Formato típico no HTML:
        <p>26 MAR 2026<br/>QUI 21:00</p>
        <p>27 MAR 2026<br/>SEX 21:00</p>
    """
    sessions = []

    date_block = (
        soup.select_one(".description-aside .event-info-block.date")
        or soup.select_one(".event-info-block.date")
    )
    if not date_block:
        return sessions

    # Cada <p> pode conter uma ou mais linhas (data + hora)
    for p in date_block.find_all("p"):
        # Substituir <br> por newline antes de extrair texto
        for br in p.find_all("br"):
            br.replace_with("\n")
        lines = [l.strip() for l in p.get_text().splitlines() if l.strip()]

        current_date = None
        current_time = None

        for line in lines:
            d = _parse_date(line)
            if d:
                # Se já tínhamos uma data pendente, guardá-la
                if current_date:
                    sessions.append({"date": current_date, "time_start": current_time})
                current_date = d
                current_time = _parse_time(line)  # hora pode estar na mesma linha
            else:
                t = _parse_time(line)
                if t and current_date:
                    current_time = t

        if current_date:
            sessions.append({"date": current_date, "time_start": current_time})

    # Fallback: ler todo o texto linha a linha
    if not sessions:
        raw = date_block.get_text(separator="\n", strip=True)
        lines = [l.strip() for l in raw.splitlines() if l.strip()]
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

    return sessions

# ---------------------------------------------------------------------------
def _parse_single_event(url: str, session: requests.Session) -> Optional[dict]:
    resp = _get(session, url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "lxml")

    # ── Título ──
    title = ""
    h1 = soup.select_one(".event-detail-header h1")
    if h1:
        title = h1.get_text(strip=True)
    if not title:
        h1 = soup.find("h1")
        title = h1.get_text(strip=True) if h1 else ""
    if not title:
        meta = soup.find("meta", property="og:title")
        title = meta["content"].strip() if meta else ""
    if len(title) < 2:
        return None

    # ── Subtítulo ──
    subtitle = ""
    sub = soup.select_one(".event-detail-header .subtitle")
    if sub:
        subtitle = sub.get_text(strip=True)

    # ── Categorias ──
    categories = [a.get_text(strip=True) for a in soup.select(".event-types .type") if a.get_text(strip=True)]

    # ── Descrição ──
    desc = ""
    desc_el = soup.select_one(".text-plugin")
    if desc_el:
        desc = desc_el.get_text(separator="\n", strip=True)
    if not desc:
        for sel in [".description", ".lead", "article", ".content"]:
            el = soup.select_one(sel)
            if el and len(el.get_text(strip=True)) > 80:
                desc = el.get_text(separator="\n", strip=True)
                break

    # ── Datas ──
    sessions = _parse_dates_block(soup)

    # ── Bloco de informações: preço, duração, sala, classificação ──
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
                price_raw = line
            elif re.search(r"dura[çc][aã]o", line, re.I):
                duration_minutes = _parse_duration(line)
            elif re.search(r"audit[oó]rio|sala\s*\d|grande\s+audit", line, re.I):
                space = line
            elif re.search(r"\bm/\d+\b|\bm\s*\+\s*\d+\b", line, re.I):
                age_rating = line

    price = _parse_price(price_raw) or {}

    # ── Bilheteira ──
    ticketing = None
    btn = soup.select_one("a.event-tickets-btn[href]")
    if btn:
        ticketing = btn["href"]
        if not ticketing.startswith("http"):
            ticketing = f"{WEBSITE}{ticketing}"
    if not ticketing:
        for a in soup.find_all("a", href=True):
            txt = a.get_text(strip=True).lower()
            if any(w in txt for w in ["bilhete", "comprar", "reservar", "ticket"]):
                ticketing = a["href"]
                if not ticketing.startswith("http"):
                    ticketing = f"{WEBSITE}{ticketing}"
                break

    # ── Imagem ──
    cover = None
    og_img = soup.find("meta", property="og:image")
    if og_img:
        cover = og_img.get("content")

    # ── Acessibilidade ──
    full_text = soup.get_text(" ", strip=True).lower()
    accessibility = {
        "has_sign_language": bool(re.search(r"língua gestual|lingua gestual|\blgp\b", full_text)),
        "has_audio_description": bool(re.search(r"audiodescrição|audiodescricao|áudio.?descri|audiodescrição", full_text)),
        "has_subtitles": bool(re.search(r"\blegenda[sd]?\b", full_text)),
        "is_relaxed_performance": bool(re.search(r"sessão relaxada|sessao relaxada|relaxed performance", full_text)),
        "wheelchair_accessible": True,
    }

    n_sessions = len(sessions)
    logger.info(f"✓ Extraído: {title[:80]} ({n_sessions} sess{'ão' if n_sessions == 1 else 'ões'})")

    return {
        "source_id": url.rstrip("/").split("/")[-1],
        "source_url": url,
        "title": title,
        "subtitle": subtitle or None,
        "description": desc or None,
        "categories": categories,
        "dates": sessions,
        "date_open": sessions[0]["date"] if sessions else None,
        "price_raw": price_raw,
        "price": price,
        "duration_minutes": duration_minutes,
        "space": space,
        "age_rating": age_rating,
        "ticketing_url": ticketing,
        "cover_image": cover,
        "location": "Culturgest",
        "accessibility": accessibility,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "_method": "culturgest-v11",
    }

# ---------------------------------------------------------------------------
def run() -> List[dict]:
    session = _make_session()
    all_events = []
    seen_urls: set = set()
    seen_page_sets: list = []  # frozensets de URLs por página

    logger.info("CULTURGEST v11 — extração completa + paragem inteligente")

    page = 1
    while page <= MAX_PAGES:
        time.sleep(REQUEST_DELAY)
        resp = _get(session, EVENT_LIST_URL, {"page": page})
        if not resp:
            logger.warning(f"Página {page}: sem resposta — a parar")
            break

        links = _extract_event_links(resp.text)
        page_set = frozenset(links)
        logger.info(f"Página {page} → {len(links)} links de eventos")

        # Paragem inteligente: ciclo AJAX detectado
        if page_set in seen_page_sets:
            logger.info(f"Página {page}: conjunto de URLs já visto — ciclo AJAX detectado, a parar")
            break
        seen_page_sets.append(page_set)

        new_count = 0
        for link in links:
            if link in seen_urls:
                continue
            seen_urls.add(link)
            new_count += 1
            ev = _parse_single_event(link, session)
            if ev:
                all_events.append(ev)

        if new_count == 0:
            logger.info(f"Página {page}: sem novos URLs — a parar")
            break

        page += 1

    logger.info(f"Total recolhido: {len(all_events)} eventos únicos")
    return all_events


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    events = run()
    print(f"\n=== TOTAL FINAL: {len(events)} eventos ===\n")
    if events:
        print(_json.dumps(events[0], indent=2, ensure_ascii=False))
