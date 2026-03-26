"""
Primeira Plateia — Scraper Teatro Nacional D. Maria II (Lisboa)
Venue: TNDM | tndm.pt  —  v7

Melhorias face à v6:
  - Sessões expandidas por horário: "qua–qui, 20h · sex, 21h · sáb, 19h · dom, 16h"
    gera uma sessão por data+hora correcta (cada dia da semana com o seu time_start)
  - Ficha técnica estruturada: credits.cast[], credits.creative_team[], credits.director,
    work.playwright, work.composer, work.original_title, duration_minutes
  - Media: gallery[] com URLs das fotos + autoria, trailer_url (YouTube/Vimeo)
  - Preço estruturado: price.is_free, price_min, price_max, price_display
  - Audiência estruturada: audience.age_min, audience.label, audience.is_family
  - Produção: production_origin, is_premiere (deteta "estreia")
  - Acessibilidade: notas de data para sessões LGP, audiodescrição, etc.
  - Multi-venue: is_multi_venue, multi_venue_ids quando há >1 local
  - is_multi_venue por sessão (venue_name por sessão já existia em v6)
  - Categoria default "Teatro" quando não há tags (antes ficava em branco)
  - Campo event_status inferido
  - Output alinhado ao event.schema.json
"""

import re
import time
import json as _json
import logging
import warnings
from datetime import datetime, date, timedelta, timezone
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
SCRAPER_ID = "tndm-v7"
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

# Mapeamento abreviatura de dia da semana PT → número ISO (0=segunda, 6=domingo)
WEEKDAY_PT = {
    "seg": 0, "ter": 1, "qua": 2, "qui": 3,
    "sex": 4, "sáb": 5, "sab": 5, "dom": 6,
}

# Mapeamento URL section → categoria/domain
SECTION_META = {
    "/espetaculos/":           {"category": "Teatro",               "domain": "artes-palco"},
    "/participacao/":          {"category": "Participação",          "domain": "artes-palco"},
    "/livros-e-pensamento/":   {"category": "Livros e Pensamento",   "domain": "pensamento"},
    "/oficinas-e-formacao/":   {"category": "Formação",              "domain": "formacao"},
}

# Mapeamento de roles PT → chaves estruturadas
ROLE_MAP = {
    "encenação":              "director",
    "encenação e":            "director",
    "direção":                "director",
    "direcção":               "director",
    "direção musical":        "conductor",
    "direcção musical":       "conductor",
    "coreografia":            "choreographer",
    "de":                     "playwright",
    "texto":                  "playwright",
    "composição":             "composer",
    "música":                 "composer",
    "interpretação":          "cast",
    "elenco":                 "cast",
    "com":                    "cast",
    "actores":                "cast",
    "atores":                 "cast",
}

# ---------------------------------------------------------------------------
# SESSION HTTP
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
# PARSING DE DATAS
# ---------------------------------------------------------------------------

def _parse_time(text: str) -> Optional[str]:
    m = re.search(r"(\d{1,2})h(\d{0,2})", text)
    if not m:
        return None
    return f"{m.group(1).zfill(2)}:{(m.group(2) or '00').zfill(2)}"


def _parse_all_times(text: str) -> list[str]:
    return [
        f"{m.group(1).zfill(2)}:{(m.group(2) or '00').zfill(2)}"
        for m in re.finditer(r"(\d{1,2})h(\d{0,2})", text)
    ]


def _parse_tndm_date(text: str) -> tuple[Optional[str], Optional[str]]:
    if not text:
        return None, None
    t        = re.sub(r"\s+", " ", text.strip())
    now_year = str(datetime.now().year)

    # Período dois meses: "27 mar - 18 abr 2026" ou "SET 2025 - JUL 2026"
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

    # Data única
    m = re.match(r"^(\d{1,2})\s+([a-záéíóúA-Z]+)(?:\s+(\d{4}))?$", t)
    if m:
        month = MONTH_PT.get(m.group(2).lower()[:3])
        if month:
            year = m.group(3) or now_year
            return f"{year}-{month}-{m.group(1).zfill(2)}", None

    return None, None


def _expand_date_text(date_text: str, now_year: str) -> list[str]:
    """Expande "4, 6, 7, 20 - 21 JUN" em lista de ISO dates."""
    t = date_text.strip()
    month_m = re.search(r"\b([a-záéíóúA-Z]{3,})\b", t)
    if not month_m:
        return []
    month = MONTH_PT.get(month_m.group(1).lower()[:3])
    if not month:
        return []
    year_m = re.search(r"\d{4}", t)
    year   = year_m.group() if year_m else now_year
    num_part = t[:month_m.start()].strip().rstrip(",").strip()
    dates = []
    for segment in re.split(r",", num_part):
        seg = segment.strip()
        range_m = re.match(r"^(\d+)\s*[-–]\s*(\d+)$", seg)
        if range_m:
            for d in range(int(range_m.group(1)), int(range_m.group(2)) + 1):
                dates.append(f"{year}-{month}-{str(d).zfill(2)}")
        elif re.match(r"^\d+$", seg):
            dates.append(f"{year}-{month}-{seg.zfill(2)}")
    return dates


# ---------------------------------------------------------------------------
# EXPANSÃO DE SESSÕES COM HORÁRIOS POR DIA DA SEMANA
# ---------------------------------------------------------------------------

def _parse_weekday_schedule(time_text: str) -> dict[int, str]:
    """
    Parseia strings como "qua – qui, 20h ∙ sex, 21h ∙ sáb, 19h ∙ dom, 16h"
    e devolve {weekday_iso: "HH:MM"} (0=segunda...6=domingo).
    Também suporta "21h" simples (aplica a todos os dias).
    """
    schedule: dict[int, str] = {}
    # Normalizar separadores
    text = time_text.replace("∙", "|").replace("·", "|").replace(",", "|")
    # Separar em segmentos por "|"
    segments = [s.strip() for s in text.split("|") if s.strip()]

    # Verificar se há dias da semana de todo
    has_days = any(
        re.search(r"\b(" + "|".join(WEEKDAY_PT.keys()) + r")\b", seg, re.IGNORECASE)
        for seg in segments
    )

    if not has_days:
        # Formato simples: apenas horas (ex: "21h30")
        times = _parse_all_times(time_text)
        if times:
            # Sem informação de dia → retornar dict vazio especial
            return {"__all__": times[0]}
        return {}

    # Parsear grupos "dia[-dia], HH:MM"
    # Cada segmento pode conter dias e uma hora. A hora pertence aos dias do segmento.
    # Estratégia: varrer os segmentos e associar hora ao(s) dia(s) encontrados.
    # Segmentos separados por "|" podem ser:
    #   "qua – qui, 20h"  →  qua+qui → 20h
    #   "sex, 21h"         →  sex → 21h
    #   "sáb, 19h"         →  sáb → 19h
    #   "dom, 16h"         →  dom → 16h
    # Mas também pode vir de: "qua – qui | 20h | sex | 21h ..." depois de split por "|"
    # Vamos re-juntar e usar um parsing mais robusto.

    # Re-juntar e fazer split por hora — cada grupo de "dias + hora"
    full = time_text.replace("∙", "·")
    # Split por "·" que separa grupos dia/hora
    groups = re.split(r"[·∙]", full)
    if len(groups) <= 1:
        # Tentar split por vírgula entre hora e próximo dia
        # Ex: "qua – qui, 20h ∙ sex, 21h" → grupos pelo ·
        groups = [full]

    for group in groups:
        group = group.strip()
        if not group:
            continue
        # Extrair hora do grupo
        times = _parse_all_times(group)
        time_str = times[0] if times else None
        if not time_str:
            continue
        # Extrair dias do grupo (antes da hora)
        # Remover a parte numérica da hora para não confundir
        day_part = re.sub(r"\d{1,2}h\d*", "", group)
        # Encontrar todos os dias mencionados
        day_matches = re.findall(
            r"\b(seg|ter|qua|qui|sex|s[aá]b|dom)\b",
            day_part, re.IGNORECASE
        )
        if not day_matches:
            # Pode ser "todos" → skip (sem dia específico)
            continue
        # Expandir intervalos de dias: "qua – qui" → qua, qui
        # Verificar se há "–" ou "-" entre dias
        day_range = re.search(
            r"\b(seg|ter|qua|qui|sex|s[aá]b|dom)\s*[-–]\s*(seg|ter|qua|qui|sex|s[aá]b|dom)\b",
            day_part, re.IGNORECASE
        )
        if day_range:
            d_start = WEEKDAY_PT.get(day_range.group(1).lower()[:3].replace("á", "a"))
            d_end   = WEEKDAY_PT.get(day_range.group(2).lower()[:3].replace("á", "a"))
            if d_start is not None and d_end is not None:
                for wd in range(d_start, d_end + 1):
                    schedule[wd] = time_str
        else:
            for dm in day_matches:
                key = dm.lower()[:3].replace("á", "a")
                wd = WEEKDAY_PT.get(key)
                if wd is not None:
                    schedule[wd] = time_str

    return schedule


def _expand_date_range_with_schedule(
    date_start: str,
    date_end: str,
    weekday_schedule: dict,
    now_year: str,
) -> list[tuple[str, str]]:
    """
    Dado um intervalo de datas e um mapa {weekday: time}, devolve lista de (date_iso, time_str)
    para cada data no intervalo que corresponda a um dia da semana agendado.
    """
    try:
        d_start = date.fromisoformat(date_start)
        d_end   = date.fromisoformat(date_end)
    except ValueError:
        return []

    results = []
    current = d_start
    while current <= d_end:
        wd = current.weekday()  # 0=segunda, 6=domingo
        if "__all__" in weekday_schedule:
            results.append((current.isoformat(), weekday_schedule["__all__"]))
        elif wd in weekday_schedule:
            results.append((current.isoformat(), weekday_schedule[wd]))
        current += timedelta(days=1)

    return results


# ---------------------------------------------------------------------------
# EXTRACÇÃO DE SESSÕES DA TABELA .table_tickets (v7: com horários por dia)
# ---------------------------------------------------------------------------

def _parse_sessions_from_table(soup: BeautifulSoup, now_year: str) -> list[dict]:
    """
    Lê a(s) tabela(s) .table_tickets e extrai sessões completas.
    Para períodos com horários por dia da semana, gera uma sessão por data+hora.
    """
    sessions = []

    for container in soup.select(".datas_detalhe"):
        if "datas_ultrapassadas" in container.get("class", []):
            continue
        table = container.select_one("table.table_tickets")
        if not table:
            continue

        for row in table.select("tr"):
            wrapper = row.select_one(".evento_datas_wapper")
            if not wrapper:
                continue

            data_el = wrapper.select_one(".evento_data_container")
            if not data_el:
                continue

            divs = data_el.find_all("div", recursive=False)
            date_raw = divs[0].get_text(strip=True) if len(divs) > 0 else ""
            time_raw = divs[1].get_text(strip=True) if len(divs) > 1 else ""

            if not date_raw:
                continue

            # Local
            local_el   = wrapper.select_one(".evento_local_container")
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

            # Determinar se é período ou data(s) avulsas
            is_period = bool(re.search(r"[-–]", date_raw)) and bool(
                re.search(r"[a-záéíóúA-Z]{3,}", date_raw)
            ) and bool(re.search(r"\d{1,2}\s+[a-záéíóúA-Z]", date_raw) or
                       re.search(r"[a-záéíóúA-Z]\s+\d{4}", date_raw))

            # Parsear horário
            weekday_schedule = _parse_weekday_schedule(time_raw) if time_raw else {}
            simple_times     = _parse_all_times(time_raw)

            if is_period:
                d1, d2 = _parse_tndm_date(date_raw)
                if d1 and d2 and weekday_schedule and "__all__" not in weekday_schedule:
                    # Expandir por dia da semana
                    pairs = _expand_date_range_with_schedule(d1, d2, weekday_schedule, now_year)
                    for iso_date, t_start in pairs:
                        sessions.append(_build_session(iso_date, t_start, None, venue_name, venue_url))
                elif d1 and d2:
                    # Período simples sem dias da semana detalhados
                    t_start = simple_times[0] if simple_times else None
                    t_end   = simple_times[1] if len(simple_times) > 1 else None
                    sessions.append(_build_session(d1, t_start, t_end, venue_name, venue_url,
                                                   notes=time_raw if time_raw else None))
                elif d1:
                    t_start = (weekday_schedule.get("__all__") or
                               (simple_times[0] if simple_times else None))
                    sessions.append(_build_session(d1, t_start, None, venue_name, venue_url))
            else:
                # Datas avulsas / lista
                expanded = _expand_date_text(date_raw, now_year)
                if not expanded:
                    d1, _ = _parse_tndm_date(date_raw)
                    if d1:
                        expanded = [d1]
                t_start = (weekday_schedule.get("__all__") or
                           (simple_times[0] if simple_times else None))
                t_end   = simple_times[1] if len(simple_times) > 1 else None
                for iso_date in expanded:
                    sessions.append(_build_session(iso_date, t_start, t_end, venue_name, venue_url))

    return sessions


def _build_session(
    date_iso: str,
    time_start: Optional[str],
    time_end: Optional[str],
    venue_name: str,
    venue_url: Optional[str],
    notes: Optional[str] = None,
) -> dict:
    return {
        "date":        date_iso,
        "time_start":  time_start,
        "time_end":    time_end,
        "venue_name":  venue_name or None,
        "venue_url":   venue_url,
        "is_cancelled": False,
        "is_sold_out":  False,
        "notes":       notes,
    }


# ---------------------------------------------------------------------------
# PARSE DA FICHA TÉCNICA (v7: estruturada)
# ---------------------------------------------------------------------------

def _parse_credits(soup: BeautifulSoup, ft: str) -> dict:
    """
    Extrai ficha técnica estruturada: director, playwright, cast, creative_team, etc.
    Também extrai duration_minutes.
    """
    credits = {
        "company":       None,
        "director":      None,
        "conductor":     None,
        "choreographer": None,
        "cast":          [],
        "creative_team": [],
        "musicians":     [],
        "credits_raw":   None,
    }
    duration_minutes = None

    ficha_el = None
    for sel in [".ficha_tecnica .primeiro_bloco", ".ficha_tecnica", ".ficha-tecnica",
                "[class*='ficha']"]:
        ficha_el = soup.select_one(sel)
        if ficha_el:
            break

    if not ficha_el:
        return credits, duration_minutes

    raw_text = ficha_el.get_text(separator="\n", strip=True)
    credits["credits_raw"] = raw_text

    # Extrair duração
    dur_m = re.search(r"dura[çc][aã]o\s*[:\s]\s*(\d+)h?(\d+)?", raw_text, re.IGNORECASE)
    if dur_m:
        h  = int(dur_m.group(1))
        mn = int(dur_m.group(2)) if dur_m.group(2) else 0
        duration_minutes = h * 60 + mn if h < 10 else h  # "1h45" → 105, "105" → 105 min
        if h < 10:
            duration_minutes = h * 60 + mn
        else:
            duration_minutes = h  # já em minutos

    # Extrair linha a linha
    lines = [l.strip() for l in raw_text.split("\n") if l.strip()]
    for line in lines:
        # Detectar "role nome" ou "role: nome"
        # Padrões: linha com underline/itálico no HTML → role é o primeiro token até espaço
        m = re.match(r"^([^:]+?)\s{2,}(.+)$", line)  # dois espaços separam role de nome
        if not m:
            # Tentar "role nome" onde role é uma palavra conhecida
            for role_pt, role_key in ROLE_MAP.items():
                pat = re.compile(
                    r"^" + re.escape(role_pt) + r"\s+(.+)$",
                    re.IGNORECASE
                )
                rm = pat.match(line)
                if rm:
                    names_raw = rm.group(1).strip()
                    _assign_credit(credits, role_key, names_raw)
                    break
        else:
            role_raw = m.group(1).strip().lower()
            names_raw = m.group(2).strip()
            role_key = ROLE_MAP.get(role_raw)
            if role_key:
                _assign_credit(credits, role_key, names_raw)

    # Fallback: parsear a partir dos elementos <span> com underline (estrutura TNDM)
    for span in ficha_el.find_all("span"):
        style = span.get("style", "")
        if "underline" not in style:
            continue
        role_raw  = span.get_text(strip=True).lower().rstrip(":")
        # Texto que se segue ao span (até próximo span ou fim de div)
        next_text = ""
        for sibling in span.next_siblings:
            if hasattr(sibling, "get_text"):
                t = sibling.get_text(strip=True)
            else:
                t = str(sibling).strip()
            if t:
                next_text = t
                break

        if not next_text:
            # Texto pode estar no pai, após o span
            parent_text = span.parent.get_text(strip=True) if span.parent else ""
            role_text   = span.get_text(strip=True)
            next_text   = parent_text.replace(role_text, "").strip().lstrip(":").strip()

        if not next_text:
            continue

        role_key = ROLE_MAP.get(role_raw)
        if role_key:
            _assign_credit(credits, role_key, next_text)

    # Duração via regex na ficha completa (fallback)
    if duration_minutes is None:
        dur_m2 = re.search(r"(\d+)h(\d+)", raw_text)
        if dur_m2:
            duration_minutes = int(dur_m2.group(1)) * 60 + int(dur_m2.group(2))
        else:
            dur_m3 = re.search(r"dura[çc][aã]o\s*[:\s]\s*(\d+)\s*min", raw_text, re.IGNORECASE)
            if dur_m3:
                duration_minutes = int(dur_m3.group(1))

    return credits, duration_minutes


def _assign_credit(credits: dict, role_key: str, names_raw: str):
    """Atribui nome(s) ao campo correcto de credits."""
    if role_key in ("cast", "musicians"):
        # Lista de nomes separados por vírgula
        names = [n.strip() for n in re.split(r",\s*", names_raw) if n.strip()]
        for name in names:
            entry = {"role": "actor" if role_key == "cast" else "musician", "name": name}
            if entry not in credits[role_key]:
                credits[role_key].append(entry)
    elif role_key == "creative_team":
        entry = {"role": role_key, "name": names_raw}
        if entry not in credits["creative_team"]:
            credits["creative_team"].append(entry)
    elif role_key in credits and isinstance(credits[role_key], (str, type(None))):
        if not credits[role_key]:  # não sobrescrever se já preenchido
            credits[role_key] = names_raw


def _parse_credits_from_hero(soup: BeautifulSoup, credits: dict):
    """Complementa credits com dados do hero (ex: 'de Luís de Camões', 'encenação Pedro Penim')."""
    hero = soup.select_one(".detalhes_espectaculo")
    if not hero:
        return
    for div in hero.find_all("div"):
        span = div.find("span")
        if not span:
            continue
        role_raw  = span.get_text(strip=True).lower().rstrip(":")
        names_raw = div.get_text(strip=True).replace(span.get_text(strip=True), "").strip()
        if not names_raw:
            continue
        role_key = ROLE_MAP.get(role_raw)
        if role_key:
            _assign_credit(credits, role_key, names_raw)


# ---------------------------------------------------------------------------
# PARSE DE PREÇO
# ---------------------------------------------------------------------------

def _parse_price(soup: BeautifulSoup, ft: str, ftl: str) -> dict:
    price = {
        "is_free":        False,
        "price_min":      None,
        "price_max":      None,
        "price_display":  None,
        "price_raw":      None,
        "has_discounts":  False,
        "discount_notes": None,
        "ticketing_url":  None,
    }

    # Bilheteira URL
    for a in soup.find_all("a", href=True):
        at = a.get_text(strip=True).lower()
        h  = a["href"]
        if any(k in at for k in ["bilhete", "comprar", "reservar", "ticket"]) or "bol.pt" in h:
            price["ticketing_url"] = h if h.startswith("http") else f"{WEBSITE}{h}"
            break

    # Entrada livre
    if re.search(r"entrada\s+livre|gratuito|acesso\s+livre", ftl):
        price["is_free"]       = True
        price["price_display"] = "Entrada livre"
        price["price_raw"]     = "Entrada livre"
        return price

    # Extrair valores €
    price_vals = [float(m.replace(",", ".")) for m in
                  re.findall(r"(\d+(?:[,\.]\d{1,2})?)\s*€", ft)]
    if price_vals:
        price["price_min"]     = min(price_vals)
        price["price_max"]     = max(price_vals)
        price["price_raw"]     = ft[ft.find(str(int(price_vals[0]))):].split("\n")[0][:60]
        price["price_display"] = (
            f"{price['price_min']:.0f}€" if price["price_min"] == price["price_max"]
            else f"{price['price_min']:.0f}€ – {price['price_max']:.0f}€"
        )

    # Descontos
    if re.search(r"desconto|reforma|estudante|jovem|cartão", ftl):
        price["has_discounts"]  = True
        disc_m = re.search(r"(desconto[^.;]+[.;])", ft, re.IGNORECASE)
        if disc_m:
            price["discount_notes"] = disc_m.group(1).strip()

    return price


# ---------------------------------------------------------------------------
# PARSE DE AUDIÊNCIA
# ---------------------------------------------------------------------------

def _parse_audience(soup: BeautifulSoup, resp_text: str, ftl: str) -> dict:
    audience = {
        "label":          None,
        "label_raw":      None,
        "age_min":        None,
        "age_max":        None,
        "is_family":      False,
        "is_educational": False,
        "school_level":   None,
        "notes":          None,
    }

    # Classificação etária M/N
    age_m = re.search(r"M[/\s]\s*(\d+)(?:\s*anos?)?", resp_text, re.IGNORECASE)
    if age_m:
        age              = int(age_m.group(1))
        audience["age_min"]   = age
        audience["label_raw"] = f"M/{age}"
        audience["label"]     = f"Maiores de {age} anos"

    # Família / infância
    if re.search(r"infância|juventude|família|criança|children", ftl):
        audience["is_family"] = True

    # Educativo / escolar
    if re.search(r"escola|escolar|educati|formação", ftl):
        audience["is_educational"] = True

    return audience


# ---------------------------------------------------------------------------
# PARSE DE MEDIA (v7: gallery + trailer)
# ---------------------------------------------------------------------------

def _parse_media(soup: BeautifulSoup) -> dict:
    media = {
        "cover_image":       None,
        "cover_image_local": None,
        "thumbnail":         None,
        "trailer_url":       None,
        "gallery":           [],
    }

    # Cover via og:image
    og_img = soup.find("meta", property="og:image")
    if og_img:
        media["cover_image"] = og_img.get("content")

    # Fallback cover
    if not media["cover_image"]:
        for sel in [".show-image img", ".event-image img", "main img[src]"]:
            el = soup.select_one(sel)
            if el and el.get("src") and not el["src"].endswith((".svg", ".gif")):
                src = el["src"]
                media["cover_image"] = src if src.startswith("http") else f"{WEBSITE}{src}"
                break

    # Galeria de imagens (.galeria_imagem)
    for a in soup.select(".galeria_imagem .galeria a[href]"):
        href = a["href"]
        if href and not href.endswith((".svg", ".gif")):
            full = href if href.startswith("http") else f"{WEBSITE}{href}"
            if full not in media["gallery"]:
                media["gallery"].append(full)

    # Trailer YouTube/Vimeo
    for iframe in soup.find_all("iframe", src=True):
        src = iframe["src"]
        if "youtube.com/embed" in src or "vimeo.com" in src:
            if not media["trailer_url"]:
                media["trailer_url"] = src
            break

    return media


# ---------------------------------------------------------------------------
# INFERIR DOMAIN E CATEGORY A PARTIR DA URL (com default Teatro)
# ---------------------------------------------------------------------------

def _infer_section_meta(url: str) -> tuple[str, str]:
    """Devolve (category, domain) baseado na secção da URL."""
    url_lower = url.lower()
    for path, meta in SECTION_META.items():
        if path in url_lower:
            return meta["category"], meta["domain"]
    return "Teatro", "artes-palco"


# ---------------------------------------------------------------------------
# EXTRACÇÃO DE LINKS (listagem)
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
    logger.info(f"TNDM: método 1 — Toda a Programação ({TODA_BASE})")
    all_entries: dict[str, dict] = {}

    resp = _get(session, TODA_BASE)
    if not resp:
        logger.warning("TNDM: toda-a-programacao inacessível")
        return []

    soup         = BeautifulSoup(resp.text, "lxml")
    page_entries = _extract_links_from_page(soup)
    all_entries.update(page_entries)
    logger.info(f"TNDM: página 1 — {len(page_entries)} itens")

    max_page = 1
    for a in soup.find_all("a", href=True):
        pm = re.search(r"[?&]p=(\d+)", a["href"])
        if pm:
            max_page = max(max_page, int(pm.group(1)))
    if max_page == 1:
        max_page = MAX_PAGES

    for page in range(2, min(max_page + 1, MAX_PAGES + 1)):
        time.sleep(REQUEST_DELAY)
        resp = _get(session, TODA_BASE, params={"tipo": "1", "cat": "1", "p": str(page)})
        if not resp:
            break
        soup         = BeautifulSoup(resp.text, "lxml")
        page_entries = _extract_links_from_page(soup)
        if not page_entries:
            break
        new_count = sum(1 for k in page_entries if k not in all_entries)
        all_entries.update(page_entries)
        logger.info(f"TNDM: página {page} — {len(page_entries)} itens ({new_count} novos)")
        if new_count == 0:
            break

    logger.info(f"TNDM: {len(all_entries)} espetáculos encontrados")
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
            logger.info(f"TNDM: {i+1}/{total} páginas ({len(events)} válidos)")
        time.sleep(REQUEST_DELAY)

    return events


# ---------------------------------------------------------------------------
# MÉTODO 2 — FALLBACK POR SECÇÕES
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
        time.sleep(REQUEST_DELAY)

    events = []
    for url in sorted(all_urls):
        ev = _parse_event_page(url, session)
        if ev:
            events.append(ev)
        time.sleep(REQUEST_DELAY)

    return events


# ---------------------------------------------------------------------------
# PARSE DE PÁGINA INDIVIDUAL (v7)
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

    # ── Título ───────────────────────────────────────────────────────────────
    title = ""
    for sel in ["h1.nome_espetaculo", "#hero-titulo", "h1.show-title", "h1.espetaculo-title",
                ".page-header h1", "main h1", "h1"]:
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
    title_raw = title

    # ── Subtítulo ─────────────────────────────────────────────────────────────
    subtitle = None
    for sel in [".subtitulo", ".subtitle", "h2.subtitulo"]:
        el = soup.select_one(sel)
        if el:
            t = el.get_text(strip=True)
            if len(t) > 2 and t.lower() != title.lower():
                subtitle = t
                break

    # ── Category / Domain ────────────────────────────────────────────────────
    # Ler tags .tag-list-item.programacao
    cats_raw = []
    for tag_el in soup.select(".tag-list-item.programacao"):
        t = tag_el.get_text(strip=True)
        if t and t not in cats_raw:
            cats_raw.append(t)

    if not cats_raw and cats_hint:
        cats_raw = cats_hint

    # Inferir category e domain pela secção da URL
    category_url, domain_url = _infer_section_meta(url)

    # Category final: usar tag se existir, senão usar a da URL (que default para "Teatro")
    if cats_raw:
        category = cats_raw[0]
    else:
        category = category_url  # inclui default "Teatro"
        # Injectar na lista para que o harmonizer a receba
        cats_raw = [category_url]

    domain = domain_url

    # ── Sessões da tabela .table_tickets ──────────────────────────────────────
    sessions = _parse_sessions_from_table(soup, now_year)

    if not sessions:
        sessions = _parse_dates_fallback(soup, ft, date_hint, now_year)

    # Construir dates[] para o schema
    dates = []
    for s in sessions:
        dates.append({
            "date":             s["date"],
            "time_start":       s.get("time_start"),
            "time_end":         s.get("time_end"),
            "duration_minutes": None,  # preenchido abaixo após parse de ficha
            "is_cancelled":     s.get("is_cancelled", False),
            "is_sold_out":      s.get("is_sold_out", False),
            "notes":            s.get("notes"),
        })

    date_first = dates[0]["date"]  if dates else None
    date_last  = dates[-1]["date"] if len(dates) > 1 else date_first

    # ── Venue / Espaços ──────────────────────────────────────────────────────
    venue_names = []
    venue_urls  = []
    for s in sessions:
        vn = s.get("venue_name")
        vu = s.get("venue_url")
        if vn and vn not in venue_names:
            venue_names.append(vn)
        if vu and vu not in venue_urls:
            venue_urls.append(vu)

    is_multi_venue = len(venue_names) > 1

    # space_id canónico (sala principal TNDM)
    space_id = None
    if "sala garrett"          in ftl: space_id = "sala-garrett"
    elif "sala estúdio"        in ftl: space_id = "sala-estudio"
    elif "teatro variedades"   in ftl: space_id = "teatro-variedades"
    elif "jardins do bombarda" in ftl: space_id = "jardins-bombarda"

    # ── Descrição ────────────────────────────────────────────────────────────
    desc = ""
    for sel in [".descricao_evento .htmleditor", ".show-description", ".event-description",
                ".synopsis", ".entry-content", ".description", "main p"]:
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

    desc_short = None
    if desc:
        desc_short = re.sub(r"\s+", " ", desc)[:297].strip()
        if len(desc) > 297:
            desc_short += "..."

    # ── Ficha Técnica Estruturada ─────────────────────────────────────────────
    credits, duration_minutes = _parse_credits(soup, ft)
    _parse_credits_from_hero(soup, credits)

    # Preencher duration_minutes nas datas
    if duration_minutes:
        for d in dates:
            d["duration_minutes"] = duration_minutes

    # ── Work (obra) ───────────────────────────────────────────────────────────
    work = {
        "original_title": None,
        "playwright":     credits.get("playwright") if isinstance(credits.get("playwright"), str) else None,
        "composer":       credits.get("composer")   if isinstance(credits.get("composer"), str) else None,
        "choreographer":  credits.get("choreographer") if isinstance(credits.get("choreographer"), str) else None,
        "year_created":   None,
        "country_of_origin": None,
        "genre_original": None,
    }

    # ── Media ─────────────────────────────────────────────────────────────────
    media = _parse_media(soup)

    # ── Preço ─────────────────────────────────────────────────────────────────
    price = _parse_price(soup, ft, ftl)

    # ── Audiência ─────────────────────────────────────────────────────────────
    audience = _parse_audience(soup, resp.text, ftl)

    # ── Acessibilidade ────────────────────────────────────────────────────────
    # Notas detalhadas de acessibilidade (ex: "Sessão com LGP no dia 12 de abril")
    access_notes = None
    access_el = soup.select_one(".acessibilidade_descricao .htmleditor")
    if access_el:
        access_notes = access_el.get_text(separator=" ", strip=True)

    accessibility = {
        "has_sign_language":      "lgp" in ftl or "língua gestual" in ftl,
        "has_audio_description":  "audiodescri" in ftl or "audiodescrição" in ftl,
        "has_subtitles":          bool(re.search(r"legenda[sd]?", ftl)),
        "subtitle_language":      None,
        "is_relaxed_performance": "sessão descontraída" in ftl or "relaxed" in ftl,
        "wheelchair_accessible":  True,
        "notes":                  access_notes,
    }

    # ── Inferir event_status ─────────────────────────────────────────────────
    event_status = "em-cartaz"
    if re.search(r"\bestreia\b", ftl):
        event_status = "estreia"
    elif re.search(r"\breposiç[aã]o\b|\breprise\b", ftl):
        event_status = "reposicao"
    elif dates and len(dates) == 1:
        event_status = "unica-sessao"

    is_premiere        = event_status == "estreia"
    is_reprise         = event_status == "reposicao"
    is_national_premiere = bool(re.search(r"estreia\s+nacional", ftl))

    # ── Produção ─────────────────────────────────────────────────────────────
    production_origin = None
    if re.search(r"co.produ[çc][aã]o\s+internacional|coprodução\s+internacional", ftl, re.IGNORECASE):
        production_origin = "co-producao-internacional"
    elif re.search(r"co.produ[çc][aã]o", ftl, re.IGNORECASE):
        production_origin = "co-producao-nacional"
    elif re.search(r"produ[çc][aã]o\s+nacional|teatro nacional d\. maria", ftl, re.IGNORECASE):
        production_origin = "nacional"

    # ── Língua do espetáculo ──────────────────────────────────────────────────
    language_of_performance = "pt"
    language_notes = None
    if re.search(r"\bem ingl[eê]s\b|\bin english\b", ftl):
        language_of_performance = "en"
    elif re.search(r"sobretitulado|sobretítulo|surtitulado", ftl):
        language_notes = "com sobretítulos"

    # ── Total de sessões ──────────────────────────────────────────────────────
    total_sessions = len(dates) if dates else None

    # ── Campos no topo para compatibilidade com o harmonizer ──────────────────
    # O harmonizer lê estes campos directamente do raw (não dentro de sub-dicts):
    #   audience    → string "M/12"
    #   price_raw   → string de preço
    #   ticketing_url → string URL
    #   cover_image, thumbnail, trailer_url, gallery → strings/lista no topo
    #   scraped_at  → string ISO no topo
    #   credits_raw → string no topo (fallback se credits{} não existir)

    audience_raw_str = audience.get("label_raw") or ""  # ex: "M/12"
    price_raw_str    = price.get("price_raw") or ""

    return {
        # ── Campos raw que o harmonizer lê directamente ──────────────────────
        "source_id":      url.rstrip("/").split("/")[-1],
        "source_url":     url,
        "space_id":       space_id,
        "title":          title,
        "subtitle":       subtitle,
        "description":    desc,
        "categories":     cats_raw,          # harmonizer → harmonize_category()
        "tags":           [],
        "dates":          dates,
        "audience":       audience_raw_str,  # harmonizer espera string
        "price_raw":      price_raw_str,     # harmonizer → parse_price()
        "ticketing_url":  price.get("ticketing_url"),
        "cover_image":    media.get("cover_image"),
        "thumbnail":      None,
        "trailer_url":    media.get("trailer_url"),
        "gallery":        media.get("gallery", []),
        "accessibility":  accessibility,
        "scraped_at":     datetime.now(timezone.utc).isoformat(),
        "production_origin": production_origin,
        "language_of_performance": language_of_performance,
        "language_notes": language_notes,

        # ── Campos estruturados extra (harmonizer passa-os através) ──────────
        # credits: o harmonizer usa raw_event.get("credits", {...}) directamente
        "credits": {
            "company":       credits.get("company"),
            "director":      credits.get("director"),
            "conductor":     credits.get("conductor"),
            "choreographer": credits.get("choreographer"),
            "cast":          credits.get("cast", []),
            "creative_team": credits.get("creative_team", []),
            "musicians":     credits.get("musicians", []),
            "credits_raw":   credits.get("credits_raw"),
        },
        "credits_raw": credits.get("credits_raw"),  # fallback harmonizer v4

        # work: o harmonizer usa raw_event.get("work", {...}) directamente
        "work": work,

        # ── Campos informativos extras (não lidos pelo harmonizer, mas ficam no raw) ──
        "date_open":        date_first,
        "date_close":       date_last,
        "is_ongoing":       bool(date_last and date_first and date_last > date_first),
        "is_multi_venue":   is_multi_venue,
        "venue_names":      venue_names,
        "description_short": desc_short,
        "_method":          SCRAPER_ID,
    }


# ---------------------------------------------------------------------------
# FALLBACK DE DATAS
# ---------------------------------------------------------------------------

def _parse_dates_fallback(
    soup: BeautifulSoup,
    ft: str,
    date_hint: str,
    now_year: str,
) -> list[dict]:
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

    # 4. Hint
    if date_hint:
        d1, d2 = _parse_tndm_date(date_hint)
        if d1:
            sessions = [_msession(d1)]
            if d2:
                sessions.append(_msession(d2))
            return sessions

    return []


def _msession(date_iso: str, time_start: str = None) -> dict:
    return {
        "date":        date_iso,
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
