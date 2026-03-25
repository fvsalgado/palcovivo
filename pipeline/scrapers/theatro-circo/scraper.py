"""
Primeira Plateia — Scraper Theatro Circo (Braga)
Venue: Theatro Circo | theatrocirco.com

Estrutura real do site (sem JSON-LD, sem <time datetime>):
  - Programação: theatrocirco.com/programa/
  - Cada evento listado com: "28 março (sáb) → Categoria"
  - Página individual: mesmo formato de data no topo
  - API The Events Calendar: dá 404 (não instalada)
  - Sitemap: tem URLs mas datas no JSON-LD não existem → parse HTML obrigatório

Estratégia:
  1. HTML da página /programa/ → links + datas directamente da listagem
  2. Sitemap → visitar páginas individuais com parser de data nativo
  3. API (fallback histórico, sempre falha com 404)
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
    # Para períodos: se o mês de fim ainda não passou, é este ano
    ref_month = int(month_end) if month_end else int(month_num)
    if ref_month >= now.month:
        return str(now.year)
    # Mês já passou: se foi há pouco (< 6 meses), ainda é este ano
    # Se foi há muito (> 6 meses), pode ser próximo ano mas é improvável
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
    # Encontrar o mês e usar o primeiro número antes dele
    month_m = re.search(r'([a-záéíóúâêôãõç]{4,})', t)
    if month_m:
        month = MONTH_PT.get(month_m.group(1)[:3])
        if month:
            year_m = re.search(r'\d{4}', t)
            year   = year_m.group() if year_m else _infer_year(month)
            # Todos os números antes do mês
            nums = re.findall(r'\d+', t[:month_m.start()])
            if nums:
                day = nums[0].zfill(2)
                return f"{year}-{month}-{day}", None

    # Mês abreviado (3 letras): "28 mar" ou "mar 28"
    # Tentativa genérica com meses de 3 letras
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

        # Cada evento na listagem tem uma estrutura de bloco:
        # [data + categoria em texto] + [<a href> para evento] + [<h3> título]
        # Estratégia: encontrar todos os links para /event/ e, para cada um,
        # subir na DOM para encontrar a data no elemento irmão anterior.

        for a in soup.find_all("a", href=True):
            href = a["href"]
            full = href if href.startswith("http") else f"{WEBSITE}{href}"

            if not EVENT_URL_PATTERN.search(full):
                continue
            if "/en/" in full:
                continue

            # Extrair título do link (pode ser <h3>/<h4> dentro do <a>)
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
                # Padrão TC: "28 março (sáb)" ou "12 janeiro a 18 abril"
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
            # Normalizar URLs de thumbnail WordPress → versão grande
            cover = re.sub(r'-\d+x\d+(\.\w+)$', r'\1', cover)

            # Categoria — texto antes do "→" na linha de data
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

        # Visitar cada página individual para dados completos
        events = []
        for i, entry in enumerate(deduped):
            ev = _parse_event_page(
                entry["source_url"], session,
                date_hint=entry["date_text"],
                title_hint=entry["title"],
                cover_hint=entry["cover"],
                cat_hint=entry["category"],
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

    # Filtrar por cutoff e ordenar mais recentes primeiro
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

    events, skipped = [], 0
    for i, url in enumerate(urls):
        ev = _parse_event_page(url, session)
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
) -> Optional[dict]:
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

    # ── Datas ──
    dates      = []
    date_close = None

    # Tentar JSON-LD primeiro (pouco provável no TC mas vale a pena)
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = _json.loads(script.string or "")
            if isinstance(data, list):
                data = next((d for d in data if isinstance(d, dict) and d.get("@type") == "Event"), {})
            if isinstance(data, dict) and data.get("@type") == "Event":
                start = data.get("startDate", "")
                end   = data.get("endDate",   "")
                if start and len(start) >= 10:
                    dates      = [_make_date(start[:10], start[11:16] if len(start) > 10 else None)]
                    date_close = end[:10] if end and len(end) >= 10 else None
                    break
        except Exception:
            pass

    # Tentar <time datetime>
    if not dates:
        for t in soup.select("time[datetime]"):
            dt = t.get("datetime", "")
            if dt and len(dt) >= 10:
                dates = [_make_date(dt[:10], dt[11:16] if len(dt) > 10 else None)]
                break

    # Parser nativo TC: procurar texto com formato "28 março (sáb)"
    if not dates:
        # Procurar em elementos de topo da página — data aparece antes do h1
        page_text = soup.get_text(" ", strip=True)
        # Padrão TC: dígito + espaço + mês por extenso
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
                dates      = [_make_date(d1)]
                date_close = d2

    # Usar hint da listagem
    if not dates and date_hint:
        d1, d2 = _parse_tc_date(date_hint)
        if d1:
            dates      = [_make_date(d1)]
            date_close = d2

    # ── Filtro de datas passadas ──
    if PAST_DAYS_CUTOFF > 0 and dates:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=PAST_DAYS_CUTOFF)).strftime("%Y-%m-%d")
        if dates[0]["date"] and dates[0]["date"] < cutoff:
            if not date_close or date_close < cutoff:
                return None

    # ── Descrição ──
    desc = ""
    for sel in [".entry-content", ".event-description", ".event-content", "article .content", "main p"]:
        el = soup.select_one(sel)
        if el:
            d = el.get_text(separator="\n", strip=True)
            if len(d) > 30:
                desc = d
                break
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
                # Remover sufixo de thumbnail WordPress
                cover = re.sub(r'-\d+x\d+(\.\w+)$', r'\1', cover)
                break

    # ── Preço ──
    price_raw = ""
    ft = soup.get_text(" ", strip=True)
    # Padrão TC: "3,5€ adultos | Gratuito crianças..."
    price_m = re.search(r'(\d+[,.]?\d*\s*€[^\n|.]*)', ft)
    if price_m:
        price_raw = price_m.group(1).strip()
    if not price_raw and re.search(r'gratuito|entrada\s+livre|livre\s+acesso', ft, re.IGNORECASE):
        price_raw = "Entrada livre"

    # ── Bilheteira ──
    ticketing_url = None
    for a in soup.find_all("a", href=True):
        at = a.get_text(strip=True).lower()
        h  = a["href"]
        if any(k in at for k in ["bilhete", "comprar", "ticket"]) or "bol.pt" in h:
            ticketing_url = h if h.startswith("http") else f"{WEBSITE}{h}"
            break

    # ── Acessibilidade ──
    ftl = ft.lower()
    accessibility = {
        "has_sign_language":      "lgp" in ftl or "língua gestual" in ftl,
        "has_audio_description":  "audiodescrição" in ftl or "audiodescri" in ftl,
        "has_subtitles":          bool(re.search(r"legenda[sd]?", ftl)),
        "is_relaxed_performance": "relaxed" in ftl or "descontraída" in ftl,
        "wheelchair_accessible":  True,
        "notes":                  None,
    }

    # ── Categoria ──
    cats = []
    if cat_hint:
        cats = [cat_hint.strip()]
    else:
        # Tentar extrair da página
        for kw in ["Música", "Teatro", "Dança", "Cinema", "Mediação", "Multidisciplinar"]:
            if kw.lower() in ftl:
                cats.append(kw)
                break

    # ── Tags ──
    tags = []
    for a in soup.find_all("a", href=True):
        if "/event_tag/" in a["href"]:
            tags.append(a.get_text(strip=True))

    return {
        "source_id":     url.rstrip("/").split("/")[-1],
        "source_url":    url,
        "title":         title,
        "subtitle":      None,
        "description":   desc,
        "categories":    cats,
        "tags":          tags,
        "dates":         dates,
        "date_open":     dates[0]["date"] if dates else None,
        "date_close":    date_close,
        "is_ongoing":    bool(date_close),
        "price_raw":     price_raw,
        "ticketing_url": ticketing_url,
        "audience":      None,
        "cover_image":   cover or None,
        "space_id":      None,
        "credits_raw":   None,
        "accessibility": accessibility,
        "scraped_at":    datetime.now(timezone.utc).isoformat(),
        "_method":       "tc-html",
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
