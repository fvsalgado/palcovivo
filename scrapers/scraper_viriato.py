"""
Scraper: Teatro Viriato
Fonte: https://www.teatroviriato.com/pt/programacao
Cidade: Viseu

Estrutura do site (HTML estático):
  - Listagem: página única com todos os eventos do ano agrupados por mês.
    Cada evento é um <a> com data, título, subtítulo, imagem e categoria visível.
  - Página de evento: /pt/programacao/espetaculo/<slug>
    Contém: data, hora, duração, preço, sala, categoria, sinopse, ficha técnica,
    link de bilhetes (caeviseu.bol.pt).

Estratégia de filtragem (por ordem):
  1. Na listagem: aceitar imediatamente categorias TEATRO.
  2. Na listagem: rejeitar imediatamente categorias claramente fora de âmbito
     (Dança, Música, Cinema, Exposição, Residência, OFICINA, DOCUMENTÁRIO,
      MASTERCLASS, SEMINÁRIO, FORMAÇÃO, OPEN CALL, VISITA GUIADA, CINE-CONCERTO,
      CMV, Conferência, ENCONTRO, Pensamento, Circo contemporâneo).
  3. Na listagem: "Cruzamento Disciplinar" → ir à página do evento e verificar
     se a ficha técnica contém "Encenação" ou "Texto" — se sim, é teatro.
  4. Eventos com "CANCELADO" no texto da listagem → ignorar.
"""

import re
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from scrapers.utils import (
    make_id, parse_date, parse_date_range, log,
    HEADERS, can_scrape, truncate_synopsis, build_image_object,
)

BASE         = "https://www.teatroviriato.com"
AGENDA       = f"{BASE}/pt/programacao"
THEATER_NAME = "Teatro Viriato"
SOURCE_SLUG  = "viriato"

# Categorias aceites directamente da listagem
_ACCEPT_CATEGORIES = {"teatro"}

# Categorias rejeitadas directamente (nunca entram)
_REJECT_CATEGORIES = {
    "dança", "música", "cinema", "exposição", "exposicao",
    "residência", "residencia", "oficina", "documentário", "documentario",
    "masterclass", "seminário", "seminario", "formação", "formacao",
    "open call", "visita guiada", "cine-concerto", "cmv",
    "conferência", "conferencia", "encontro", "pensamento",
    "circo contemporâneo", "circo contemporaneo",
}

# Categorias que requerem verificação na página do evento
_VERIFY_CATEGORIES = {"cruzamento disciplinar"}

# Palavras-chave na ficha técnica que confirmam que é teatro
_THEATER_FICHA_KEYS = re.compile(
    r"\b(encena[çc][aã]o|texto\s+e\s+encena[çc][aã]o|dramaturgia|direção\s+artística)\b",
    re.IGNORECASE,
)

_PT_MONTHS = {
    "jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6,
    "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12,
}


# ─────────────────────────────────────────────────────────────
# Ponto de entrada
# ─────────────────────────────────────────────────────────────

def scrape() -> list[dict]:
    if not can_scrape(BASE):
        log(f"robots.txt: scraping bloqueado para {BASE}")
        return []

    candidates = _collect_candidates()
    log(f"[{THEATER_NAME}] {len(candidates)} candidatos após filtragem da listagem")

    events:   list[dict] = []
    seen_ids: set[str]   = set()

    for item in candidates:
        ev = _scrape_event(item["url"], item["category_raw"], item.get("stub"))
        if ev:
            eid = ev["id"]
            if eid not in seen_ids:
                seen_ids.add(eid)
                events.append(ev)
        time.sleep(0.4)

    log(f"[{THEATER_NAME}] {len(events)} eventos de teatro")
    return events


# ─────────────────────────────────────────────────────────────
# Recolha de candidatos da listagem
# ─────────────────────────────────────────────────────────────

def _collect_candidates() -> list[dict]:
    """
    Percorre a listagem e devolve candidatos a eventos de teatro.
    Cada candidato é um dict com url, category_raw, e stub (dados parciais da listagem).
    """
    try:
        r = requests.get(AGENDA, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as e:
        log(f"[{THEATER_NAME}] Erro na listagem: {e}")
        return []

    soup       = BeautifulSoup(r.text, "lxml")
    candidates = []
    seen_urls  = set()

    # Cada evento na listagem é um <a href="/pt/programacao/espetaculo/slug">
    for a in soup.find_all("a", href=re.compile(r"/pt/programacao/espetaculo/")):
        href = a.get("href", "")
        url  = href if href.startswith("http") else urljoin(BASE, href)
        if url in seen_urls:
            continue

        # Texto completo do bloco do evento
        block_text = a.get_text(" ", strip=True)

        # Ignorar cancelados
        if "CANCELADO" in block_text.upper():
            continue

        # Ignorar CMV — têm "CMV" como categoria ou "Câmara Municipal" no texto
        if re.search(r"\bCMV\b|Câmara Municipal", block_text, re.IGNORECASE):
            continue

        # Extrair categoria — último elemento de texto não-vazio do bloco
        # O site coloca a categoria como texto independente no final do <a>
        category_raw = _extract_category(a)
        category_key = category_raw.lower().strip()

        # Filtrar por categoria
        if category_key in _REJECT_CATEGORIES:
            continue
        if category_key not in _ACCEPT_CATEGORIES and category_key not in _VERIFY_CATEGORIES:
            # Categoria desconhecida — ignorar por defeito (conservador)
            continue

        seen_urls.add(url)

        # Stub com dados já disponíveis na listagem (evita pedido extra se não necessário)
        stub = _extract_stub(a, url)

        candidates.append({
            "url":          url,
            "category_raw": category_raw,
            "stub":         stub,
        })

    return candidates


def _extract_category(a_tag) -> str:
    """
    Extrai a categoria do bloco de evento na listagem.
    O site coloca a categoria como último span/div de texto no <a>.
    """
    # Tentar encontrar elemento com classe que contenha "category" ou similar
    for cls in ["category", "tag", "tipo", "label"]:
        el = a_tag.find(class_=re.compile(cls, re.IGNORECASE))
        if el:
            return el.get_text(strip=True)

    # Fallback: o último texto não-vazio do bloco costuma ser a categoria
    texts = [t.strip() for t in a_tag.stripped_strings if t.strip()]
    if texts:
        # A categoria está tipicamente no final, após título e subtítulo
        # Ignorar textos que são claramente datas ou horários
        for t in reversed(texts):
            if not re.match(r"^\d", t) and len(t) < 40:
                return t
    return ""


def _extract_stub(a_tag, url: str) -> dict:
    """Extrai dados parciais disponíveis na listagem."""
    texts = [t.strip() for t in a_tag.stripped_strings if t.strip()]

    # Título: texto mais longo que não seja data nem categoria curta
    title = ""
    for t in texts:
        if len(t) > len(title) and not re.match(r"^\d", t) and len(t) > 3:
            title = t

    # Imagem
    img_tag = a_tag.find("img")
    img_url = ""
    if img_tag:
        img_url = img_tag.get("src") or img_tag.get("data-src") or ""
        if img_url and not img_url.startswith("http"):
            img_url = urljoin(BASE, img_url)

    # Data da listagem (ex: "13 - 31" ou "13" ou "13 jan - 25 jul")
    date_text = ""
    for t in texts:
        if re.match(r"^\d{1,2}", t):
            date_text = t
            break

    return {"title": title, "img_url": img_url, "date_text": date_text, "url": url}


# ─────────────────────────────────────────────────────────────
# Scraping de página de evento individual
# ─────────────────────────────────────────────────────────────

def _scrape_event(
    url: str,
    category_raw: str,
    stub: dict | None,
) -> dict | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as e:
        log(f"[{THEATER_NAME}] Erro em {url}: {e}")
        return None

    soup      = BeautifulSoup(r.text, "lxml")
    full_text = soup.get_text(" ", strip=True)

    # ── Verificação para "Cruzamento Disciplinar" ─────────────
    # Só aceitar se a ficha técnica indicar encenação/dramaturgia
    if category_raw.lower().strip() == "cruzamento disciplinar":
        if not _THEATER_FICHA_KEYS.search(full_text):
            log(f"[{THEATER_NAME}] '{_get_title(soup)}' ignorado (Cruzamento Disciplinar sem encenação)")
            return None

    # ── Título ────────────────────────────────────────────────
    title = _get_title(soup)
    if not title or len(title) < 3:
        return None

    # ── Categoria normalizada ─────────────────────────────────
    # Para o schema do Palco Vivo, mapeamos tudo para "Teatro"
    category = "Teatro"

    # ── Datas ────────────────────────────────────────────────
    dates_label, date_start, date_end = _parse_dates(soup, full_text)
    if not date_start:
        # Fallback para dados do stub da listagem
        if stub and stub.get("date_text"):
            dates_label, date_start, date_end = _parse_date_text(stub["date_text"])
    if not date_start:
        return None

    # ── Horário ───────────────────────────────────────────────
    schedule = ""
    m_s = re.search(r"\b([a-záéíóúàãõç]+)\s+(\d{1,2}[h:]\d{2})\b", full_text, re.IGNORECASE)
    if m_s:
        schedule = f"{m_s.group(1).capitalize()} {m_s.group(2)}"

    # ── Duração ───────────────────────────────────────────────
    duration = ""
    m_d = re.search(r"(\d+)\s*min\.?", full_text, re.IGNORECASE)
    if m_d:
        duration = f"{m_d.group(1)} min."

    # ── Sala ──────────────────────────────────────────────────
    sala = ""
    m_sala = re.search(r"LOCAL\s+([^\n]{3,60})", full_text, re.IGNORECASE)
    if m_sala:
        sala = m_sala.group(1).strip()

    # ── Preço ─────────────────────────────────────────────────
    price_info = ""
    m_p = re.search(
        r"(Entrada\s+livre|gratuito|\d+(?:[,\.]\d+)?\s*€(?:\s*[-–]\s*\d+(?:[,\.]\d+)?\s*€)?)",
        full_text, re.IGNORECASE,
    )
    if m_p:
        price_info = m_p.group(1).strip()

    # ── Classificação etária ───────────────────────────────────
    age_rating = ""
    m_a = re.search(r"\+(\d+)\s*(?:Maiores de)?", full_text)
    if not m_a:
        m_a = re.search(r"M\s*/\s*(\d+)", full_text)
    if m_a:
        age_rating = f"+{m_a.group(1)}"

    # ── Imagem ────────────────────────────────────────────────
    image = None
    # og:image é o mais fiável
    og = soup.find("meta", property="og:image")
    raw_img = og.get("content", "") if og else ""
    if not raw_img or not raw_img.startswith("http"):
        # Tentar primeira imagem da galeria
        for img in soup.find_all("img", src=re.compile(r"/contents/galleryimage/")):
            src = img.get("src", "")
            if src:
                raw_img = src if src.startswith("http") else urljoin(BASE, src)
                break
    if not raw_img and stub and stub.get("img_url"):
        raw_img = stub["img_url"]
    if raw_img:
        image = build_image_object(raw_img, soup, THEATER_NAME, url)

    # ── Bilhetes ──────────────────────────────────────────────
    ticket_url = ""
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "bol.pt" in href or "bilhete" in href.lower() or "ticket" in href.lower():
            ticket_url = href if href.startswith("http") else urljoin(BASE, href)
            break

    # ── Sinopse ───────────────────────────────────────────────
    synopsis = _extract_synopsis(soup)

    # ── Ficha técnica ─────────────────────────────────────────
    technical_sheet = _parse_ficha(soup, full_text)

    return {
        "id":              make_id(SOURCE_SLUG, title),
        "title":           title,
        "theater":         THEATER_NAME,
        "category":        category,
        "dates_label":     dates_label,
        "date_start":      date_start,
        "date_end":        date_end,
        "schedule":        schedule,
        "synopsis":        truncate_synopsis(synopsis),
        "image":           image,
        "source_url":      url,
        "ticket_url":      ticket_url,
        "price_info":      price_info,
        "duration":        duration,
        "age_rating":      age_rating,
        "sala":            sala,
        "technical_sheet": technical_sheet,
    }


# ─────────────────────────────────────────────────────────────
# Helpers de parsing
# ─────────────────────────────────────────────────────────────

def _get_title(soup) -> str:
    h1 = soup.find("h1")
    return h1.get_text(strip=True) if h1 else ""


def _parse_dates(soup, text: str) -> tuple[str, str, str]:
    """
    Extrai datas da página de evento.
    Formatos encontrados no Viriato:
      "13 fev 2026"       — data única
      "13 - 31 jan 2026"  — intervalo mesmo mês
      "13 jan - 25 jul"   — intervalo meses distintos (ano inferido)
      "02 - 06 jun"       — intervalo sem ano
    """
    # Tentar encontrar o bloco de data no <h2> ou elemento de data dedicado
    date_el = soup.find("h2")
    date_src = date_el.get_text(" ", strip=True) if date_el else ""

    for src in [date_src, text]:
        result = _parse_date_text(src)
        if result[1]:  # date_start preenchido
            return result

    return "", "", ""


def _parse_date_text(text: str) -> tuple[str, str, str]:
    """Converte texto de data para (dates_label, date_start, date_end)."""
    if not text:
        return "", "", ""

    text = text.strip()

    # DD - DD MMM [YYYY] — intervalo mesmo mês
    m = re.search(
        r"(\d{1,2})\s*[-–]\s*(\d{1,2})\s+([a-záéíóú]{3,})\s*(\d{4})?",
        text, re.IGNORECASE,
    )
    if m:
        d1, d2, mon_s, yr_s = m.groups()
        n = _mon(mon_s)
        if n:
            y = int(yr_s) if yr_s else _infer_year(n, int(d1))
            dates_label = f"{d1} - {d2} {mon_s} {y}"
            date_start  = f"{y}-{n:02d}-{int(d1):02d}"
            date_end    = f"{y}-{n:02d}-{int(d2):02d}"
            return dates_label, date_start, date_end

    # DD MMM [YYYY] – DD MMM [YYYY] — intervalo meses distintos
    m = re.search(
        r"(\d{1,2})\s+([a-záéíóú]{3,})(?:\s+(\d{4}))?\s*[-–]\s*(\d{1,2})\s+([a-záéíóú]{3,})\s*(\d{4})?",
        text, re.IGNORECASE,
    )
    if m:
        d1, mo1, y1, d2, mo2, y2 = m.groups()
        n1, n2 = _mon(mo1), _mon(mo2)
        if n1 and n2:
            yr2 = int(y2) if y2 else _infer_year(n2, int(d2))
            yr1 = int(y1) if y1 else yr2
            dates_label = f"{d1} {mo1} – {d2} {mo2} {yr2}"
            date_start  = f"{yr1}-{n1:02d}-{int(d1):02d}"
            date_end    = f"{yr2}-{n2:02d}-{int(d2):02d}"
            return dates_label, date_start, date_end

    # DD MMM [YYYY] — data única
    m = re.search(r"(\d{1,2})\s+([a-záéíóú]{3,})\s*(\d{4})?", text, re.IGNORECASE)
    if m:
        d, mon_s, yr_s = m.groups()
        n = _mon(mon_s)
        if n:
            y = int(yr_s) if yr_s else _infer_year(n, int(d))
            dates_label = f"{d} {mon_s} {y}"
            date_start  = f"{y}-{n:02d}-{int(d):02d}"
            return dates_label, date_start, date_start

    return "", "", ""


def _mon(s: str) -> int | None:
    return _PT_MONTHS.get(s.lower()[:3])


def _infer_year(month: int, day: int) -> int:
    """Infere o ano: se o mês já passou, usa o próximo ano."""
    from datetime import datetime
    now = datetime.now()
    if month > now.month or (month == now.month and day >= now.day):
        return now.year
    return now.year + 1


def _extract_synopsis(soup) -> str:
    """
    Extrai sinopse da página do evento.
    No Viriato, a sinopse é um bloco de parágrafos após a ficha de detalhes
    (data, hora, preço, sala) e antes da ficha técnica.
    """
    # og:description como fallback rápido
    og_desc = soup.find("meta", property="og:description")
    og_text = og_desc.get("content", "").strip() if og_desc else ""

    # Parágrafos substantivos do conteúdo principal
    synopsis = ""
    for p in soup.find_all("p"):
        t = p.get_text(strip=True)
        # Ignorar parágrafos curtos, listas de descontos, informação técnica
        if len(t) < 80:
            continue
        if re.match(
            r"^(\d+%|Mecenas|Sócios|Famílias|Profissionais|Funcionários|m/\s*\d+|Os descontos|Este site)",
            t, re.IGNORECASE,
        ):
            continue
        synopsis += (" " if synopsis else "") + t
        if len(synopsis) > 800:
            break

    return synopsis.strip() or og_text


def _parse_ficha(soup, text: str) -> dict:
    """
    Extrai ficha técnica da página do evento.
    No Viriato, a ficha é texto corrido com padrão:
      "Encenação Pedro Penim\nTexto Tiago Rodrigues\n..."
    """
    ficha      = {}
    known_keys = [
        ("texto",           r"[Tt]exto\s+(?:e\s+[Ee]ncena[çc][aã]o\s+)?"),
        ("encenação",       r"[Ee]ncena[çc][aã]o\s+"),
        ("dramaturgia",     r"[Dd]ramaturgia\s+"),
        ("direção",         r"[Dd]ire[çc][aã]o\s+(?:artística\s+)?"),
        ("tradução",        r"[Tt]radu[çc][aã]o\s+"),
        ("adaptação",       r"[Aa]dapta[çc][aã]o\s+"),
        ("cenografia",      r"[Cc]enografia\s+"),
        ("figurinos",       r"[Ff]igurinos?\s+"),
        ("luz",             r"[Dd]esenho\s+de\s+[Ll]uz\s+|[Ii]lumina[çc][aã]o\s+"),
        ("som",             r"[Dd]esenho\s+de\s+[Ss]om\s+|[Ss]onoplastia\s+"),
        ("música",          r"[Mm][úu]sica\s+(?:original\s+)?"),
        ("interpretação",   r"[Ii]nterpreta[çc][aã]o\s+"),
        ("produção",        r"[Pp]rodu[çc][aã]o\s+(?:[Ee]xecutiva\s+)?"),
        ("coprodução",      r"[Cc]oprodu[çc][aã]o\s+"),
        ("fotografia",      r"[Ff]otografia(?:\s+e\s+identidade\s+gráfica)?\s+"),
    ]

    positions = []
    for key, pattern in known_keys:
        for match in re.finditer(pattern, text):
            positions.append((match.start(), match.end(), key))
    positions.sort()

    for i, (start, end, key) in enumerate(positions):
        next_start = positions[i + 1][0] if i + 1 < len(positions) else end + 300
        value      = re.sub(r"\s+", " ", text[end:next_start].strip())
        # Parar antes de campos de apoio ou copyright
        value = re.split(r"\s+(?:Apoio|Agradecimentos|©|Coprodução\b)", value)[0]
        value = value[:200].strip()
        if value and key not in ficha:
            ficha[key] = value

    return ficha
