"""
Culturgest Scraper — Versão 17 (26 Mar 2026)

SOLUÇÃO DEFINITIVA:
  O sitemap.xml contém todos os URLs de /pt/programacao/<slug>/ directamente.
  Não é necessário o endpoint AJAX que se recusa a devolver fragmentos.

  Fluxo:
    1. GET /sitemap.xml
    2. Extrair todos os <loc> com /pt/programacao/<slug>/
    3. GET de cada página de evento
    4. Parsear campos (título, datas, preço, espaço, ficha técnica, etc.)
    5. Validator descarta os sem datas (open-calls, visitas, etc.)

  Ganhos vs v14:
    - Sem dependência do endpoint AJAX (eliminado)
    - Listing completo e estável — o sitemap é actualizado com cada publicação
    - Código muito mais simples: 1 estratégia robusta em vez de 3 em cascata
    - Inclui eventos históricos (útil para enriquecimento de dados)
    - Filtro opcional por data para correr só eventos futuros
"""

import re
import time
import json as _json
import logging
import warnings
from datetime import datetime, date, timezone
from typing import Optional, List
from urllib.parse import urlparse, urlunparse

import requests
import urllib3
from bs4 import BeautifulSoup

from pipeline.core.base_scraper import BaseScraper

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", message="Unverified HTTPS request")

logger = logging.getLogger(__name__)

WEBSITE = "https://www.culturgest.pt"
SITEMAP_URL = f"{WEBSITE}/sitemap.xml"
LIST_URL = f"{WEBSITE}/pt/programacao/por-evento/"

REQUEST_DELAY = 1.5
TIMEOUT = 30

# Filtrar apenas eventos com data >= FILTER_FROM_DATE (None = todos)
# Útil para runs incrementais: só eventos a partir de hoje
FILTER_FROM_DATE: Optional[str] = None  # ex: date.today().strftime("%Y-%m-%d")

# Modo incremental: set de source_ids já conhecidos — skip automático
# O pipeline pode passar este set via run(known_ids={...})
# Em modo FULL_RESCAN, ignorar o cache (re-processa tudo)
FULL_RESCAN: bool = False  # True: processa todos os 952 URLs mesmo se conhecidos

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "pt-PT,pt;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": LIST_URL,
}

MONTH_PT = {
    "jan": "01", "fev": "02", "mar": "03", "abr": "04", "mai": "05", "jun": "06",
    "jul": "07", "ago": "08", "set": "09", "out": "10", "nov": "11", "dez": "12",
    "janeiro": "01", "fevereiro": "02", "marco": "03", "abril": "04",
    "maio": "05", "junho": "06", "julho": "07", "agosto": "08", "setembro": "09",
    "outubro": "10", "novembro": "11", "dezembro": "12",
}

# Slugs que correspondem a páginas de sistema (não a eventos)
SYSTEM_SLUGS = {
    "por-evento", "agenda-pdf", "archive", "schedule", "filtrar",
    "sitemap", "por-semana", "por-mes",
}


# ---------------------------------------------------------------------------
# Utilitários
# ---------------------------------------------------------------------------

def _normalize_url(url: str) -> str:
    p = urlparse(url)
    return urlunparse((p.scheme, p.netloc, p.path.rstrip("/"), "", "", ""))


def _is_programacao_event(url: str) -> bool:
    """
    Aceita /pt/programacao/<slug>/ com exactamente 3 segmentos de path.
    Rejeita query strings, versões inglesas, coleccao, participacao, etc.
    """
    p = urlparse(url)
    if p.query:
        return False
    parts = [x for x in p.path.split("/") if x]
    if len(parts) != 3:
        return False
    if parts[0] != "pt" or parts[1] != "programacao":
        return False
    if parts[2] in SYSTEM_SLUGS:
        return False
    return True


def _make_session() -> requests.Session:
    """Module-level helper kept for backwards compatibility."""
    s = requests.Session()
    s.verify = False
    s.headers.update(HEADERS)
    return s


def _get(session: requests.Session, url: str) -> Optional[requests.Response]:
    """Module-level helper kept for backwards compatibility."""
    try:
        r = session.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        return r
    except Exception as e:
        logger.warning(f"GET {url}: {e}")
        return None


# ---------------------------------------------------------------------------
# STEP 1: Obter URLs do sitemap
# ---------------------------------------------------------------------------

def _get_event_urls_from_sitemap(session: requests.Session) -> List[str]:
    """
    Parseia o sitemap.xml e extrai todos os URLs de /pt/programacao/<slug>/.
    Sem duplicados (sitemap tem PT e EN — só queremos PT).
    """
    resp = _get(session, SITEMAP_URL)
    if not resp:
        logger.error("Não foi possível obter o sitemap.xml")
        return []

    logger.info(f"Sitemap: {len(resp.text)} chars")

    # Usar lxml-xml para parsear o XML correctamente
    # Se não estiver disponível, usar html.parser como fallback
    try:
        soup = BeautifulSoup(resp.text, "lxml-xml")
    except Exception:
        soup = BeautifulSoup(resp.text, "html.parser")

    urls = []
    seen = set()

    for loc in soup.find_all("loc"):
        url = loc.get_text(strip=True)
        if not _is_programacao_event(url):
            continue
        norm = _normalize_url(url)
        if norm not in seen:
            seen.add(norm)
            urls.append(url)

    logger.info(f"Sitemap: {len(urls)} URLs de /pt/programacao/ encontrados")
    return urls


# ---------------------------------------------------------------------------
# STEP 2: Parsers de campos
# ---------------------------------------------------------------------------

def _parse_date(text: str, ref_year: Optional[str] = None) -> Optional[str]:
    """
    Parseia datas como '26 MAR', '26 MAR 2022', '– 12 JUN 2022'.
    ref_year: ano de fallback quando a data não tem ano explícito.
    """
    if not text:
        return None
    t = re.sub(r"\s+", " ", text.strip()).lower()
    t = re.sub(r"^[–—\-]\s*", "", t).strip()  # remover dash inicial (date_end)
    t = re.sub(r"\b(seg|ter|qua|qui|sex|s[aá]b|dom)\b\.?", "", t).strip()
    t = t.replace(",", "").strip()
    m = re.match(r"(\d{1,2})\s+([a-záéíóúç]+)(?:\s+(\d{4}))?", t)
    if m:
        day = m.group(1).zfill(2)
        raw_month = m.group(2)
        month = MONTH_PT.get(raw_month[:3]) or MONTH_PT.get(raw_month)
        year = m.group(3) or ref_year or str(datetime.now().year)
        if month:
            return f"{year}-{month}-{day}"
    return None


def _parse_time_range(text: str) -> tuple:
    """Extrai (time_start, time_end) de '13:00 – 18:00' ou '22:00 - 00:00'."""
    m = re.search(r"(\d{1,2}[h:]\d{2})\s*[–—\-]\s*(\d{1,2}[h:]\d{2})", text)
    if m:
        return _parse_time(m.group(1)), _parse_time(m.group(2))
    return None, None


def _parse_weekday_schedule(text: str) -> Optional[dict]:
    """
    Parseia horários semanais como 'TER A DOM 13:00 – 18:00'.
    Devolve {weekdays, time_start, time_end} ou None.
    """
    t = text.lower()
    day_mentions = re.findall(r"\b(seg|ter|qua|qui|sex|s[aá]b|dom)\b", t)
    time_start, time_end = _parse_time_range(t)
    if not time_start:
        time_start = _parse_time(t)
    if day_mentions or time_start:
        return {"weekdays": day_mentions, "time_start": time_start, "time_end": time_end}
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


def _parse_dates_block(soup: BeautifulSoup) -> List[dict]:
    """
    Extrai sessões de um evento. Três padrões suportados:

    1. Sessões individuais no .date block: '26 MAR 21:00', '27 MAR 21:00'
    2. Range de datas com horário semanal: '26 MAR – 12 JUN 2022' + 'TER A DOM 13:00 – 18:00'
    3. Sessões especiais no .highlight: '25 MAR 22:00 - 00:00' (inaugurações, etc.)
    """
    sessions = []
    date_block = (
        soup.select_one(".description-aside .event-info-block.date")
        or soup.select_one(".event-info-block.date")
    )
    if not date_block:
        return sessions

    for br in date_block.find_all("br"):
        br.replace_with("\n")

    # Extrair linhas do bloco de datas
    paragraphs = date_block.find_all("p")
    raw_lines = []
    if paragraphs:
        for p in paragraphs:
            raw_lines += [l.strip() for l in p.get_text().splitlines() if l.strip()]
    else:
        raw = date_block.get_text(separator="\n", strip=True)
        raw_lines = [l.strip() for l in raw.splitlines() if l.strip()]

    # Detectar se é range (ex: "26 MAR" + "– 12 JUN 2022") ou lista de sessões
    # Um range tem exactamente uma linha de início e outra com dash "–"
    date_start = None
    date_end = None
    ref_year = None  # ano extraído do date_end para corrigir date_start sem ano

    # Primeira passagem: recolher datas para detectar range
    parsed_dates = []
    for line in raw_lines:
        clean = re.sub(r"^[–—\-]\s*", "", line).strip()
        d = _parse_date(clean)
        has_dash = bool(re.match(r"^[–—\-]", line))
        has_year = bool(re.search(r"\d{4}", clean))
        if d:
            parsed_dates.append((d, has_dash, has_year, clean))

    is_range = (
        len(parsed_dates) == 2
        and not parsed_dates[0][1]   # primeira sem dash
        and parsed_dates[1][1]       # segunda com dash
    ) or (
        len(parsed_dates) == 1
        and any(re.match(r"^[–—\-]", l) for l in raw_lines)
    )

    if is_range and parsed_dates:
        # Extrair ref_year do date_end (linha com dash), aplicar ao date_start
        for d, has_dash, has_year, clean in parsed_dates:
            if has_dash:
                date_end = d
                m_year = re.search(r"\d{4}", clean)
                if m_year:
                    ref_year = m_year.group()
            else:
                date_start = d

        # Se date_start não tem ano e temos ref_year, re-parsear
        if date_start and ref_year:
            for line in raw_lines:
                clean = re.sub(r"^[–—\-]\s*", "", line).strip()
                if not re.match(r"^[–—\-]", line):
                    d = _parse_date(clean, ref_year=ref_year)
                    if d:
                        date_start = d
                        break

        # Procurar horário semanal nos blocos seguintes (.event-info-block sem classe date)
        weekly_schedule = None
        for block in soup.select(".event-info-block"):
            if "date" in block.get("class", []):
                continue
            for p in block.find_all("p"):
                sched = _parse_weekday_schedule(p.get_text(strip=True))
                if sched and sched.get("time_start"):
                    weekly_schedule = sched
                    break
            if weekly_schedule:
                break

        # Produzir sessão de range (date_open / date_close)
        if date_start:
            sess = {
                "date": date_start,
                "date_end": date_end,
                "time_start": weekly_schedule["time_start"] if weekly_schedule else None,
                "time_end": weekly_schedule["time_end"] if weekly_schedule else None,
                "weekdays": weekly_schedule["weekdays"] if weekly_schedule else [],
                "is_ongoing": True,
            }
            sessions.append(sess)

        # Sessões especiais no .highlight (inaugurações, visitas, etc.)
        highlight = soup.select_one(".event-info-block.highlight")
        if highlight:
            for br in highlight.find_all("br"):
                br.replace_with("\n")
            for line in highlight.get_text().splitlines():
                line = line.strip()
                if not line or line == "\xa0":
                    continue
                d = _parse_date(line, ref_year=ref_year)
                if d:
                    t_start, t_end = _parse_time_range(line)
                    if not t_start:
                        t_start = _parse_time(line)
                    sessions.append({
                        "date": d,
                        "time_start": t_start,
                        "time_end": t_end,
                        "notes": line,
                    })

    else:
        # Modo normal: lista de sessões individuais
        _extract_sessions_from_lines(raw_lines, sessions)

    return sessions


def _extract_sessions_from_lines(lines: List[str], sessions: List[dict]) -> None:
    """Parseia lista de sessões individuais: cada data (com hora opcional) = 1 sessão."""
    # Detectar ref_year a partir das linhas com ano explícito
    ref_year = None
    for line in lines:
        m = re.search(r"\b(20\d{2})\b", line)
        if m:
            ref_year = m.group(1)
            break

    current_date = None
    current_time = None
    for line in lines:
        d = _parse_date(line, ref_year=ref_year)
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
                if label and value and value.strip() not in ("", "\xa0"):
                    parts.append(f"{label}: {value}")
                i += 2
            else:
                i += 1
    return " | ".join(parts) if parts else None


# ---------------------------------------------------------------------------
# STEP 3: Parser de evento individual
# ---------------------------------------------------------------------------

def _parse_single_event(url: str, session: requests.Session) -> Optional[dict]:
    resp = _get(session, url)
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

    # Filtro por data (opcional)
    if FILTER_FROM_DATE and sessions:
        sessions = [s for s in sessions if s["date"] >= FILTER_FROM_DATE]
        if not sessions:
            return None  # Evento passado — ignorar

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

    # Créditos de imagem (ex: "© Francisco Vidal")
    # Primeiro .event-info-block da aside que não tem classe adicional
    credits_image = None
    for block in soup.select(".description-aside .event-info-block"):
        classes = set(block.get("class", []))
        extra = classes - {"event-info-block"}
        if not extra:  # bloco sem classe adicional = créditos de imagem
            text = block.get_text(strip=True)
            if text and text != "\xa0":
                credits_image = text
            break

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
        "credits_image": credits_image,
        "location": "Culturgest",
        "accessibility": accessibility,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "_method": "culturgest-v17-sitemap",
    }


# ---------------------------------------------------------------------------
# CulturegestScraper — class-based wrapper
# ---------------------------------------------------------------------------

class CulturegestScraper(BaseScraper):
    """
    Culturgest scraper using sitemap.xml listing + per-event HTML parsing.

    Inherits session management and retry logic from BaseScraper.
    SSL verification is disabled (Culturgest returns a self-signed cert).
    """

    HEADERS = HEADERS
    VERIFY_SSL = False
    RATE_DELAY = REQUEST_DELAY
    TIMEOUT = TIMEOUT

    def fetch_event_list(self) -> list:
        """
        Returns the list of event URLs from the Culturgest sitemap,
        filtered against known_ids when in incremental mode.
        """
        known_ids = getattr(self, "_known_ids", None)

        if known_ids and not FULL_RESCAN:
            logger.info(f"CULTURGEST v17 — modo incremental ({len(known_ids)} já conhecidos)")
        else:
            logger.info("CULTURGEST v17 — listing via sitemap.xml (run completo)")

        event_urls = _get_event_urls_from_sitemap(self.session)

        if not event_urls:
            logger.error("Sem URLs de eventos. Verificar acesso ao sitemap.")
            return []

        if known_ids and not FULL_RESCAN:
            new_urls = []
            skipped = 0
            for url in event_urls:
                slug = _normalize_url(url).rstrip("/").split("/")[-1]
                if slug in known_ids:
                    skipped += 1
                else:
                    new_urls.append(url)
            logger.info(
                f"Sitemap: {len(event_urls)} total, {skipped} já conhecidos, "
                f"{len(new_urls)} novos"
            )
            event_urls = new_urls
        else:
            logger.info(f"{len(event_urls)} URLs a processar")

        return event_urls

    def parse_event(self, raw) -> dict:
        """
        raw is a URL string (str) for Culturgest — fetch and parse the event page.
        Returns the normalised event dict, or None if parsing fails.
        """
        return _parse_single_event(raw, self.session)

    def run(
        self,
        known_ids=None,
        rate_delay=None,
        scraper_flags=None,
    ) -> list:
        """
        Entry point. Accepts known_ids for incremental mode.
        """
        self._known_ids = known_ids

        if rate_delay is not None:
            self._rate_delay = rate_delay

        event_urls = self.fetch_event_list()

        all_events = []
        seen = set()

        for url in event_urls:
            norm = _normalize_url(url)
            if norm in seen:
                continue
            seen.add(norm)
            time.sleep(self._rate_delay)
            ev = self.parse_event(url)
            if ev:
                all_events.append(ev)

        logger.info(f"Total recolhido: {len(all_events)} eventos")
        return all_events


# ---------------------------------------------------------------------------
# Entrada principal — kept for backwards compatibility
# ---------------------------------------------------------------------------

def run(known_ids: Optional[set] = None) -> List[dict]:
    """
    Backwards-compatible module-level entry point. Delegates to CulturegestScraper.

    known_ids: set de source_ids já em cache — skip automático em modo incremental.
               None ou conjunto vazio = processar tudo (primeiro run ou FULL_RESCAN).
    """
    return CulturegestScraper().run(known_ids=known_ids)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    events = run()
    print(f"\n=== TOTAL FINAL: {len(events)} eventos ===\n")
    if events:
        print(_json.dumps(events[0], indent=2, ensure_ascii=False))
