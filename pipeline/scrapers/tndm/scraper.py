"""
Primeira Plateia — Scraper Teatro Nacional D. Maria II (Lisboa)
Venue: TNDM | tndm.pt  —  v4

Problemas corrigidos vs versão anterior:
  1. O link "Saiba mais" tem href para o espetáculo mas o TÍTULO está
     num elemento irmão ("Projeto: Filodemo") — scraper anterior usava
     a.get_text() que retornava "Saiba mais" em vez do título real.
  2. O padrão de data tem prefixo "Data:" que o regex anterior não capturava.
  3. Fallback insuficiente: agora raspa TODAS as secções individualmente.

Estratégia:
  1. Agenda geral — parseia o bloco COMPLETO de cada item
     (Data + Programa + Projeto + Local + href) de uma vez.
  2. Listagens por secção — visita /espetaculos/, /participacao/, etc.
  3. Parse da página individual de cada espetáculo.
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

AGENDA_URL = f"{WEBSITE}/pt/programacao/agenda-geral/"

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
    re.compile(r"/pt/programacao/toda-a-programacao/[^/?#]+/?$"),
]

# URLs a ignorar (navegação, sobre, etc.)
SKIP_URLS = re.compile(
    r"/(sobre|historia|equipa|parceiros|contactos?|bilheteira|livraria"
    r"|acessibilidade|newsletter|recrutamento|arquivo|politica|cookies"
    r"|podcast|escola|projetos?-de-continuidade|atos|boca-aberta"
    r"|bolsa|panos|premio|editorial|internacional|proxima-cena"
    r"|odisseia|requalificacao|pt/?$|en/)(/|$)"
)

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


def _is_event_url(url: str) -> bool:
    if not url.startswith(WEBSITE):
        return False
    if SKIP_URLS.search(url):
        return False
    return any(p.search(url) for p in EVENT_URL_PATTERNS)


# ---------------------------------------------------------------------------
# PARSING DE DATAS
# ---------------------------------------------------------------------------

def _infer_year(month_num: str) -> str:
    now = datetime.now()
    m   = int(month_num)
    return str(now.year + 1 if m < now.month else now.year)


def _parse_date_fragment(fragment: str) -> Optional[str]:
    """Converte texto de data num ISO YYYY-MM-DD."""
    f = fragment.strip().lower()
    f = re.sub(r"\bde\b", " ", f)
    f = re.sub(r"\s+", " ", f).strip()

    # DD MMMM [YYYY]
    m = re.match(r"^(\d{1,2})\s+([a-záéíóúâêôãõç]+)(?:\s+(\d{4}))?$", f)
    if m:
        month = MONTH_PT.get(m.group(2)[:3])
        if month:
            year = m.group(3) or _infer_year(month)
            return f"{year}-{month}-{m.group(1).zfill(2)}"

    # MMMM YYYY
    m = re.match(r"^([a-záéíóúâêôãõç]+)\s+(\d{4})$", f)
    if m:
        month = MONTH_PT.get(m.group(1)[:3])
        if month:
            return f"{m.group(2)}-{month}-01"

    return None


def _parse_date_text(text: str) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Parseia texto de data TNDM.
    Retorna (date_first, date_close, time_str).
    Formatos:
      "28 MAR"
      "27 mar - 18 abr 2026"
      "SET 2025 - JUL 2026"
      "4, 6, 7, 20 - 21 JUN"
      "27 mar - 18 abr 2026 · 21h30"
    """
    if not text:
        return None, None, None

    t = text.strip()

    # Extrair hora: "21h30", "19h", "16h00"
    time_m  = re.search(r"(\d{1,2})h(\d{2})?", t)
    time_str = None
    if time_m:
        h  = time_m.group(1).zfill(2)
        mi = time_m.group(2) or "00"
        time_str = f"{h}:{mi}"
        t = t[:time_m.start()].strip()

    # Limpar separadores de campos
    t = re.split(r"[·•]", t)[0].strip()
    t = re.sub(r"\s+", " ", t).strip()

    # Período: "27 mar - 18 abr 2026" ou "SET 2025 - JUL 2026"
    sep = re.search(r"\s+[-–]\s+", t)
    if sep:
        p1 = t[:sep.start()].strip()
        p2 = t[sep.end():].strip()
        # Propagar ano de p2 para p1 se necessário
        year_m = re.search(r"\d{4}", p2)
        if year_m and not re.search(r"\d{4}", p1):
            p1 = f"{p1} {year_m.group()}"
        d1 = _parse_date_fragment(p1)
        d2 = _parse_date_fragment(p2)
        if d1:
            return d1, d2, time_str

    # Múltiplos dias: "4, 6, 7, 20 - 21 JUN" → usar o primeiro
    t_first = re.split(r"[,;]", t)[0].strip()
    d = _parse_date_fragment(t_first)
    if d:
        return d, None, time_str

    return None, None, time_str


# ---------------------------------------------------------------------------
# MÉTODO 1 — AGENDA GERAL
#
# Estrutura real da página (confirmada por Google):
#   <item>
#     "Data: SET 2025 - JUL 2026 · Programa: Oficinas e Formação · Projeto: Oficinas de Teatro · Local: ... "
#     <a href="/pt/programacao/oficinas-e-formacao/oficinas-de-teatro/">Saiba mais</a>
#   </item>
#
# O problema anterior: a.get_text() == "Saiba mais" — o título estava
# no texto circundante, não no elemento <a>.
#
# Solução: para cada link "Saiba mais", subir na DOM e parsear o bloco
# completo com regex para extrair Data:, Programa:, Projeto:, Local:.
# ---------------------------------------------------------------------------

def _extract_block_fields(text: str) -> dict:
    """
    Extrai campos label:valor de um bloco de texto da agenda.
    Padrão: "Data: X · Programa: Y · Projeto: Z · Local: W"
    """
    fields = {}
    # Separar por · e extrair pares label: valor
    parts = re.split(r"\s*[·•]\s*", text)
    for part in parts:
        m = re.match(r"^(Data|Programa|Projeto|Local)\s*:\s*(.+)$", part.strip(), re.IGNORECASE)
        if m:
            fields[m.group(1).lower()] = m.group(2).strip()
    return fields


def _scrape_via_agenda(session: requests.Session) -> list[dict]:
    logger.info(f"TNDM: método 1 — Agenda geral ({AGENDA_URL})")
    resp = _get(session, AGENDA_URL)
    if not resp:
        logger.warning("TNDM Agenda: inacessível")
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    entries_by_url: dict[str, dict] = {}

    # Encontrar todos os links "Saiba mais" com href para espetáculo
    for a in soup.find_all("a", href=True):
        href = a["href"]
        full_url = href if href.startswith("http") else f"{WEBSITE}{href}"

        if not _is_event_url(full_url):
            continue

        # Subir na DOM para encontrar o bloco com os campos
        block_text = ""
        title      = ""
        date_text  = ""

        node = a
        for depth in range(6):
            node = node.parent
            if not node:
                break

            text = node.get_text(" ", strip=True)

            # Tentar extrair campos estruturados
            fields = _extract_block_fields(text)
            if fields.get("projeto"):
                title     = fields["projeto"]
                date_text = fields.get("data", "")
                break

            # Alternativa: procurar padrão de data no texto
            date_m = re.search(
                r"(?:Data\s*:\s*)?"
                r"(\d{1,2}\s+[a-záéíóúA-Z]{3,}(?:\s*[-–]\s*\d{1,2}\s+[a-záéíóúA-Z]{3,})?\s*(?:\d{4})?)",
                text
            )
            if date_m and not date_text:
                date_text = date_m.group(1).strip()

        # Se não encontrou título via campos estruturados, tentar og:title
        # ou inferir do URL
        if not title:
            slug = full_url.rstrip("/").split("/")[-1]
            title = slug.replace("-", " ").title()

        if not title or len(title) < 3:
            continue

        if full_url not in entries_by_url:
            entries_by_url[full_url] = {
                "source_url":  full_url,
                "title":       title,
                "dates_raw":   [],
            }
        else:
            # Actualizar título se este for mais longo (mais descritivo)
            if len(title) > len(entries_by_url[full_url]["title"]):
                entries_by_url[full_url]["title"] = title

        if date_text and date_text not in entries_by_url[full_url]["dates_raw"]:
            entries_by_url[full_url]["dates_raw"].append(date_text)

    logger.info(f"TNDM Agenda: {len(entries_by_url)} espetáculos encontrados")

    if not entries_by_url:
        # Diagnóstico: mostrar quantos links existem na página
        all_links = soup.find_all("a", href=True)
        logger.warning(f"TNDM Agenda: {len(all_links)} links totais na página — nenhum corresponde a EVENT_URL_PATTERNS")
        # Mostrar alguns links para diagnóstico
        sample = [a["href"] for a in all_links if "/programacao/" in a.get("href","")][:10]
        if sample:
            logger.info(f"TNDM Agenda: links /programacao/ encontrados: {sample}")
        return []

    # Visitar cada página individual para dados completos
    events = []
    total  = len(entries_by_url)
    for i, (url, meta) in enumerate(entries_by_url.items()):
        ev = _parse_event_page(
            url, session,
            title_hint=meta["title"],
            dates_raw_hint=meta["dates_raw"],
        )
        if ev:
            events.append(ev)
        if (i + 1) % 5 == 0 or (i + 1) == total:
            logger.info(f"TNDM Agenda: {i+1}/{total} processados ({len(events)} válidos)")
        time.sleep(REQUEST_DELAY)

    logger.info(f"TNDM Agenda: {len(events)} eventos recolhidos")
    return events


# ---------------------------------------------------------------------------
# MÉTODO 2 — LISTAGENS POR SECÇÃO
# Visita cada página de listagem (/espetaculos/, /participacao/, etc.)
# e recolhe todos os links para espetáculos individuais.
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
        logger.info(f"TNDM listagens: {len(all_urls)} links únicos após {listing_url}")
        time.sleep(REQUEST_DELAY)

    logger.info(f"TNDM listagens: {len(all_urls)} URLs de espetáculos a processar")

    events = []
    for url in sorted(all_urls):
        ev = _parse_event_page(url, session)
        if ev:
            ev["_method"] = "listing"
            events.append(ev)
        time.sleep(REQUEST_DELAY)

    logger.info(f"TNDM listagens: {len(events)} eventos recolhidos")
    return events


# ---------------------------------------------------------------------------
# PARSE DE PÁGINA INDIVIDUAL
# ---------------------------------------------------------------------------

def _parse_event_page(
    url: str,
    session: requests.Session,
    title_hint: str = None,
    dates_raw_hint: list = None,
) -> Optional[dict]:
    """Faz parse de uma página de espetáculo do TNDM."""
    resp = _get(session, url)
    if not resp:
        return None

    soup    = BeautifulSoup(resp.text, "lxml")
    ftl     = soup.get_text(" ", strip=True).lower()
    ft      = soup.get_text(" ", strip=True)

    # ── Título ──
    title = ""
    for sel in [
        "h1.show-title", "h1.espetaculo-title", "h1.event-title",
        ".page-header h1", ".content-header h1", "main h1", "h1",
    ]:
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
    # Limpar sufixo do site
    title = re.sub(r"\s*[-–|]\s*Teatro Nacional.*$", "", title, flags=re.IGNORECASE).strip()
    title = re.sub(r"\s*[-–|]\s*TNDM.*$",           "", title, flags=re.IGNORECASE).strip()

    # ── Datas ──
    dates      = []
    date_close = None

    # 1. JSON-LD schema.org
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = _json.loads(script.string or "")
            if isinstance(data, list):
                data = next((d for d in data if isinstance(d, dict) and d.get("@type") == "Event"), {})
            if isinstance(data, dict) and data.get("@type") == "Event":
                start = data.get("startDate", "")
                end   = data.get("endDate",   "")
                if start and len(start) >= 10:
                    dates      = [_make_date_entry(start[:10], start[11:16] if len(start) > 10 else None)]
                    date_close = end[:10] if end and len(end) >= 10 else None
                    break
        except Exception:
            pass

    # 2. Selectores HTML
    if not dates:
        for sel in [
            ".show-date", ".event-date", ".data", ".dates",
            "[class*='date']", "[class*='data']",
            ".show-info", ".event-info", "time[datetime]",
        ]:
            for el in soup.select(sel):
                raw = el.get("datetime") or el.get_text(strip=True)
                d1, d2, ts = _parse_date_text(raw)
                if d1:
                    dates      = [_make_date_entry(d1, ts)]
                    date_close = d2
                    break
            if dates:
                break

    # 3. Regex no texto da página — padrões de data TNDM
    if not dates:
        candidates = [
            # período com ano: "27 mar - 18 abr 2026"
            r"(\d{1,2}\s+[a-záéíóúA-Z]{3,}\s*[-–]\s*\d{1,2}\s+[a-záéíóúA-Z]{3,}\s+\d{4})",
            # mês + ano: "SET 2025 - JUL 2026"
            r"([A-Z]{3}\s+\d{4}\s*[-–]\s*[A-Z]{3}\s+\d{4})",
            # dia + mês + ano: "27 mar 2026"
            r"(\d{1,2}\s+[a-záéíóúA-Z]{3,}\s+\d{4})\b",
            # dia + mês sem ano: "28 MAR"
            r"\b(\d{1,2}\s+[A-ZÁÉÍÓÚ]{3,4})\b",
        ]
        for pat in candidates:
            m = re.search(pat, ft)
            if m:
                d1, d2, ts = _parse_date_text(m.group(1))
                if d1:
                    dates      = [_make_date_entry(d1, ts)]
                    date_close = d2
                    break

    # 4. Hints da agenda
    if not dates and dates_raw_hint:
        for raw in dates_raw_hint:
            d1, d2, ts = _parse_date_text(raw)
            if d1:
                dates      = [_make_date_entry(d1, ts)]
                date_close = d2
                break

    # ── Descrição ──
    desc = ""
    for sel in [
        ".show-description", ".event-description", ".synopsis",
        ".entry-content", ".description", "main .content",
        "article .text", ".text-content", "main p",
    ]:
        el = soup.select_one(sel)
        if el:
            d = el.get_text(separator="\n", strip=True)
            if len(d) > 40:
                desc = d
                break
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
        for sel in [".show-image img", ".event-image img", ".poster img", "main img[src]"]:
            el = soup.select_one(sel)
            if el and el.get("src") and not el["src"].endswith((".svg", ".gif")):
                src   = el["src"]
                cover = src if src.startswith("http") else f"{WEBSITE}{src}"
                break

    # ── Categorias ──
    cats = []
    ul = url.lower()
    if "/espetaculos/"          in ul: cats.append("Espetáculos")
    elif "/participacao/"       in ul: cats.append("Participação")
    elif "/livros-e-pensamento/" in ul: cats.append("Livros e Pensamento")
    elif "/oficinas-e-formacao/" in ul: cats.append("Oficinas e Formação")

    # ── Preço ──
    price_raw = ""
    for sel in [".price", ".ticket-price", "[class*='preco']", "[class*='price']"]:
        el = soup.select_one(sel)
        if el:
            price_raw = el.get_text(strip=True)
            break
    if not price_raw and re.search(r"entrada\s+livre|gratuito|acesso\s+livre", ftl):
        price_raw = "Entrada livre"

    # ── Bilheteira ──
    ticketing_url = None
    for a in soup.find_all("a", href=True):
        at = a.get_text(strip=True).lower()
        if any(k in at for k in ["bilhete", "comprar", "reservar", "ticket", "bol.pt"]):
            h = a["href"]
            ticketing_url = h if h.startswith("http") else f"{WEBSITE}{h}"
            break

    # ── Créditos ──
    credits_raw = ""
    for sel in [".credits", ".ficha-tecnica", ".show-credits", "[class*='ficha']", ".team"]:
        el = soup.select_one(sel)
        if el:
            credits_raw = el.get_text(separator="\n", strip=True)
            break

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
    if "sala garrett"           in ftl: space_id = "sala-garrett"
    elif "sala estúdio"         in ftl: space_id = "sala-estudio"
    elif "sala estudio"         in ftl: space_id = "sala-estudio"
    elif "teatro variedades"    in ftl: space_id = "teatro-variedades"
    elif "jardins do bombarda"  in ftl: space_id = "jardins-bombarda"
    elif "convento"             in ftl: space_id = "convento"

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
        "is_ongoing":    bool(date_close and date_close > dates[0]["date"]) if dates else False,
        "price_raw":     price_raw,
        "ticketing_url": ticketing_url,
        "audience":      audience_raw,
        "cover_image":   cover,
        "space_id":      space_id,
        "credits_raw":   credits_raw,
        "accessibility": accessibility,
        "scraped_at":    datetime.now(timezone.utc).isoformat(),
        "_method":       "agenda",
    }


def _make_date_entry(date: str, time_start: Optional[str] = None) -> dict:
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
    """
    Cascata:
    1. Agenda geral — fonte mais completa, todas as secções
    2. Listagens por secção — fallback se agenda falhar
    """
    session = _make_session()

    events = _scrape_via_agenda(session)
    if events:
        return events

    logger.warning("TNDM: agenda geral sem resultados — a tentar listagens por secção")
    events = _scrape_via_listings(session)
    if events:
        return events

    logger.error("TNDM: todos os métodos falharam")
    return []


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    events = run()
    print(f"\nTotal: {len(events)} eventos")
    if events:
        print(_json.dumps(events[0], indent=2, ensure_ascii=False))
