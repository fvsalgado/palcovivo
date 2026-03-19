"""
Scraper: Teatro Maria Matos
Listagem teatro: https://teatromariamatos.pt/tipo/teatro/
URLs eventos: /espetaculos/slug/

Estrutura da página de evento (HTML estático, WordPress):
  - Categoria:   1ª palavra do bloco de texto no topo ("Teatro", "Dança", etc.)
  - Datas:       "18 MARÇO – 17 MAIO"  /  sessões avulsas "terça 3 março • 21:00"
  - Horários:    linhas com "domingos · 17:00", "quintas · 21:00", etc.
  - Preço:       "20-22€" ou "20€" — linha isolada
  - Ficha:       "Autor: X Encenação: Y ..."  (texto corrido)
  - Elenco:      "Interpretação: X, Y, Z"
  - Idade:       "M/12", "M/14", "M/6", "M/18"
  - Duração:     "90 min."
  - Sinopse:     parágrafos após a ficha técnica
  - Imagem:      og:image
  - Bilhetes:    primeiro link ticketline.pt na página
"""
import re
import time
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from scrapers.utils import make_id, parse_date, log, HEADERS, can_scrape, truncate_synopsis, build_image_object

BASE    = "https://teatromariamatos.pt"
AGENDA  = f"{BASE}/tipo/teatro/"
THEATER = "Teatro Maria Matos"

# Dias da semana em português (para detectar sessões avulsas)
WEEKDAYS_PT = r"(?:segunda|terça|quarta|quinta|sexta|sábado|domingo)"

# Padrão de horário recorrente: "domingos · 17:00"
SCHEDULE_LINE = re.compile(
    r"(domingos?|segundas?|terças?|quartas?|quintas?|sextas?|sábados?)"
    r"\s*[·•]\s*(\d{1,2}:\d{2})",
    re.IGNORECASE,
)

# Campos da ficha técnica — chaves a extrair
FICHA_KEYS = [
    "texto", "autor", "autora", "dramaturgia",
    "encenação", "encenadore?s?",
    "tradução", "adaptação",
    "cenário", "cenografia",
    "figurinos?",
    "desenho de luz", "iluminação",
    "desenho de som", "sonoplastia",
    "música", "composição",
    "coreografia",
    "produção",
    "coprodução",
    "direção",
    "ass\\. encenação", "assistente de encenação",
    "fotografias(?: de cena)?",
    "vídeo",
]
FICHA_RE = re.compile(
    r"(" + "|".join(FICHA_KEYS) + r")\s*[:\s]\s*([^A-ZÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕÇÑ\n]{3,}?)(?=\s+(?:"
    + "|".join(FICHA_KEYS) + r"|Interpretação|M/\d|$))",
    re.IGNORECASE,
)


def scrape():
    if not can_scrape(BASE):
        log(f"robots.txt: scraping bloqueado para {BASE}")
        return []
    try:
        r = requests.get(AGENDA, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as e:
        log(f"[Maria Matos] Erro na listagem: {e}")
        return []

    soup = BeautifulSoup(r.text, "lxml")
    seen, events = set(), []

    for a in soup.find_all("a", href=re.compile(r"/espetaculos/")):
        href = a["href"]
        full = href if href.startswith("http") else BASE + href
        if full in seen:
            continue
        seen.add(full)
        ev = _scrape_event(full)
        if ev:
            events.append(ev)
        time.sleep(0.3)

    log(f"[Maria Matos] {len(events)} eventos")
    return events


def _scrape_event(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as e:
        log(f"[Maria Matos] Erro em {url}: {e}")
        return None

    soup = BeautifulSoup(r.text, "lxml")
    raw  = r.text

    # --- Título ---
    title_el = soup.select_one("h1")
    if not title_el:
        return None
    title = title_el.get_text(strip=True)
    if not title or len(title) < 3:
        return None

    # Texto limpo do <main> para extracção de campos
    text = soup.get_text(" ")

    # --- Imagem (og:image é sempre o mais fiável) ---
    image = ""
    og = soup.find("meta", property="og:image")
    if og and og.get("content", "").startswith("http"):
        image = og["content"]

    # --- Bilhetes ---
    ticket_url = ""
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if "ticketline" in href or "sapo.pt" in href:
            ticket_url = href
            break

    # --- Categoria ---
    # A primeira linha do conteúdo é geralmente a categoria WP ("Teatro", "Dança", etc.)
    category = "Teatro"
    cat_links = soup.select(
        ".tipo a, [class*='tipo'] a, [rel='category tag'], "
        ".entry-meta a[href*='/tipo/']"
    )
    if cat_links:
        category = cat_links[0].get_text(strip=True).capitalize()
    else:
        # fallback: primeira tag de categoria no texto
        cm = re.search(r"^(Teatro|Dança|Música|Performance|Ópera)", text.strip(), re.IGNORECASE)
        if cm:
            category = cm.group(1).capitalize()

    # --- Datas e horários ---
    dates_label, date_start, date_end, schedule = _parse_dates_and_schedule(text, soup)

    # --- Preço ---
    # Formato: "20€", "20-22€", "20€ - 22€", "Entrada livre"
    price = ""
    pm = re.search(
        r"(Entrada\s+livre"
        r"|\d+(?:[,\.]\d+)?\s*€\s*[-–]\s*\d+(?:[,\.]\d+)?\s*€"
        r"|\d+(?:[,\.]\d+)?[-–]\d+(?:[,\.]\d+)?\s*€"
        r"|\d+(?:[,\.]\d+)?\s*€)",
        text, re.IGNORECASE,
    )
    if pm:
        price = pm.group(1).strip()

    # --- Duração ---
    duration = ""
    dm = re.search(r"(\d+\s*min\.?)", text, re.IGNORECASE)
    if dm:
        duration = dm.group(1).strip()

    # --- Indicação de idade ---
    age_rating = ""
    am = re.search(r"\b(M\s*/\s*\d+|Livre)\b", text)
    if am:
        age_rating = am.group(1).replace(" ", "")

    # --- Ficha técnica ---
    technical_sheet = _parse_ficha(text)

    # --- Sinopse ---
    # og:description é a sinopse curta; os <p> do main têm a versão completa
    synopsis_short = ""
    og_desc = soup.find("meta", property="og:description")
    if og_desc:
        synopsis_short = og_desc.get("content", "").strip()

    synopsis_full = ""
    for p in soup.select("main p, article p, .entry-content p"):
        t = p.get_text(strip=True)
        # Evitar parágrafos da ficha técnica (começam com "Autor:", etc.)
        if len(t) > 60 and not re.match(r"^(Autor|Texto|Encenação|Tradução|Cenário|Figurinos|Com |Interpretação)", t, re.IGNORECASE):
            if not synopsis_full:
                synopsis_full = t
            else:
                synopsis_full += " " + t
            if len(synopsis_full) > 800:
                break

    description = synopsis_full.strip() or synopsis_short

    return {
        "id":              make_id("mariamatos", title),
        "title":          title,
        "theater":        THEATER,
        "category":       category,
        "dates_label":    dates_label,
        "date_start":     date_start,
        "date_end":       date_end,
        "schedule":       schedule,
        "description":     truncate_synopsis(description),
        "synopsis_short":  truncate_synopsis(synopsis_short),
        "image":           build_image_object(image, None, "Teatro Maria Matos", url),
        "url":            url,
            "source_url":      url,
        "ticket_url":     ticket_url,
        "price":          price,
        "duration":       duration,
        "age_rating":     age_rating,
        "technical_sheet": technical_sheet,
    }


def _parse_dates_and_schedule(text, soup):
    """
    Extrai datas e horários da página do Maria Matos.

    Formatos encontrados:
      A) Temporada:   "18 MARÇO – 17 MAIO"  +  linhas "domingos · 17:00" etc.
      B) Sessões:     "terça 3 março • 21:00"  (lista de datas avulsas)
      C) Misto:       temporada + ensaio avulso antes
    """
    dates_label = ""
    date_start  = ""
    date_end    = ""
    schedule    = ""

    # 1. Detectar intervalo de temporada primeiro (para usar o ano como âncora)
    range_m = re.search(
        r"(\d{1,2})\s+([A-Za-z\u00e7\u00e3\u00e1\u00e9\u00ed\u00f3\u00fa]{3,}(?:\s+\d{4})?)"
        r"\s*[–—-]\s*"
        r"(\d{1,2})\s+([A-Za-z\u00e7\u00e3\u00e1\u00e9\u00ed\u00f3\u00fa]{3,}(?:\s+\d{4})?)",
        text,
    )

    # Determinar o ano âncora para sessões avulsas
    anchor_year = None
    if range_m:
        # Tentar extrair ano explícito do intervalo
        yr_m = re.search(r"\d{4}", range_m.group(0))
        if yr_m:
            anchor_year = int(yr_m.group())
        else:
            # Usar o ano inferido da data de fim do intervalo
            de_try = parse_date(f"{range_m.group(3)} {range_m.group(4)}")
            if de_try:
                anchor_year = int(de_try[:4])

    # 2. Sessões avulsas (ex: "terça 3 março • 21:00")
    session_pattern = re.compile(
        WEEKDAYS_PT + r"\s+(\d{1,2})\s+"
        r"([A-Za-z\u00e7\u00e3\u00e1\u00e9\u00ed\u00f3\u00fa]{3,})"
        r"(?:\s+(\d{4}))?\s*[•·]\s*(\d{1,2}:\d{2})",
        re.IGNORECASE,
    )

    # Extrair sessões raw para calcular o ano âncora por votação
    from collections import Counter
    raw_sessions = []
    for m in session_pattern.finditer(text):
        raw_sessions.append((m.group(1), m.group(2), m.group(3), m.group(4)))

    # Se não há âncora de temporada, calcular por votação nas sessões
    if not anchor_year and raw_sessions:
        now = datetime.now()
        year_counts: Counter = Counter()
        for d_num, mon_s, yr_s, _ in raw_sessions:
            from scrapers.utils import MONTHS as _M, HEADERS, can_scrape
            mon = _M.get(mon_s.lower()) or _M.get(mon_s.lower()[:3])
            if not mon:
                continue
            if yr_s:
                year_counts[int(yr_s)] += 1
            elif mon > now.month or (mon == now.month and int(d_num) >= now.day):
                year_counts[now.year] += 1
            else:
                year_counts[now.year + 1] += 1
        if year_counts:
            anchor_year = year_counts.most_common(1)[0][0]

    sessions = []
    for d_num, mon_s, yr_s, hhmm in raw_sessions:
        d = parse_date(
            f"{d_num} {mon_s}{' '+yr_s if yr_s else ''}",
            force_year=anchor_year,
        )
        if d:
            sessions.append((d, hhmm))

    # 3. Processar intervalo de temporada
    if range_m:
        ds = parse_date(f"{range_m.group(1)} {range_m.group(2)}", force_year=anchor_year)
        de = parse_date(f"{range_m.group(3)} {range_m.group(4)}", force_year=anchor_year)
        if ds and de:
            dates_label = range_m.group(0).strip()
            date_start  = ds
            date_end    = de

            # Horários recorrentes ("domingos · 17:00")
            scheds = [
                f"{m.group(1).capitalize()} {m.group(2)}"
                for m in SCHEDULE_LINE.finditer(text)
            ]
            if scheds:
                schedule = " | ".join(scheds)

            # Sessões avulsas fora do intervalo (ex: ensaio solidário antes da estreia)
            extra = [(d, h) for d, h in sessions if d < ds]
            if extra:
                extra_labels = [f"{d} {h}" for d, h in sorted(extra)]
                schedule = (
                    "Sessões especiais: " + ", ".join(extra_labels)
                    + (" | " + schedule if schedule else "")
                ).strip(" |")

            return dates_label, date_start, date_end, schedule

    # 4. Só sessões avulsas (sem intervalo de temporada)
    if sessions:
        sessions.sort()
        date_start  = sessions[0][0]
        date_end    = sessions[-1][0]
        dates_label = f"{date_start} – {date_end}" if len(sessions) > 1 else date_start

        sched_parts = [f"{d} {h}" for d, h in sessions]
        schedule    = " | ".join(sched_parts)
        return dates_label, date_start, date_end, schedule

    # 5. Fallback: "Até DD de MES" ou "A partir de DD MES"
    m = re.search(
        r"[Aa]t[eé]\s+(\d{1,2}\s+(?:de\s+)?[A-Za-z\u00e7\u00e3\u00e1\u00e9\u00ed\u00f3\u00fa]{3,}(?:\s+\d{4})?)",
        text,
    )
    if m:
        dates_label = m.group(0).strip()
        date_end = parse_date(m.group(1))
        date_start = date_end
        return dates_label, date_start, date_end, schedule

    m = re.search(
        r"[Aa]\s+[Pp]artir\s+[Dd]e\s+(\d{1,2}\s+[A-Za-z\u00e7\u00e3\u00e1\u00e9\u00ed\u00f3\u00fa]{3,}(?:\s+\d{4})?)",
        text,
    )
    if m:
        dates_label = m.group(0).strip()
        date_start = date_end = parse_date(m.group(1))
        return dates_label, date_start, date_end, schedule

    return dates_label, date_start, date_end, schedule


def _parse_ficha(text):
    """
    Extrai a ficha técnica como dict estruturado.
    Ex: {"encenação": "Ricardo Neves-Neves", "tradução": "Ana Sampaio", ...}

    O texto do Maria Matos tem a ficha num bloco corrido sem separadores de linha claros.
    """
    ficha = {}

    # Chaves conhecidas — ordem de prioridade/aparência
    known_keys = [
        ("texto",            r"[Tt]exto(?:\s+e\s+[Ee]ncena[çc][aã]o)?\s*[:\s]\s*"),
        ("autor",            r"[Aa]utor[a]?\s*[:\s]\s*"),
        ("dramaturgia",      r"[Dd]ramaturgia\s*[:\s]\s*"),
        ("encenação",        r"[Ee]ncena[çc][aã]o\s*[:\s]\s*"),
        ("tradução",         r"[Tt]radu[çc][aã]o\s*[:\s]\s*"),
        ("adaptação",        r"[Aa]dapta[çc][aã]o\s*[:\s]\s*"),
        ("cenário",          r"[Cc]en[aá]rio\s*[:\s]\s*"),
        ("cenografia",       r"[Cc]enografia\s*[:\s]\s*"),
        ("figurinos",        r"[Ff]igurinos?\s*[:\s]\s*"),
        ("luz",              r"[Dd]esenho\s+de\s+[Ll]uz\s*[:\s]\s*|[Ii]lumina[çc][aã]o\s*[:\s]\s*"),
        ("som",              r"[Dd]esenho\s+de\s+[Ss]om\s*[:\s]\s*|[Ss]onoplastia\s*[:\s]\s*"),
        ("música",           r"[Mm][úu]sica\s*[:\s]\s*|[Cc]omposi[çc][aã]o\s*[:\s]\s*"),
        ("coreografia",      r"[Cc]oreografia(?:\s+e\s+movimento)?\s*[:\s]\s*"),
        ("produção",         r"[Pp]rodu[çc][aã]o\s*[:\s]\s*"),
        ("coprodução",       r"[Cc]oprodu[çc][aã]o\s*[:\s]\s*"),
        ("direção",          r"[Dd]ire[çc][aã]o\s*[:\s]\s*"),
        ("ass_encenação",    r"[Aa]ss(?:istente)?\.?\s+(?:de\s+)?[Ee]ncena[çc][aã]o\s*[:\s]\s*"),
        ("interpretação",    r"[Ii]nterpreta[çc][aã]o\s*[:\s]\s*"),
        ("elenco",           r"[Cc]om\s+(?=[A-ZÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕÇÑ])"),
    ]

    # Encontrar posições de cada chave no texto
    positions = []
    for key, pattern in known_keys:
        for m in re.finditer(pattern, text):
            positions.append((m.start(), m.end(), key))

    positions.sort()

    # Extrair valor entre uma chave e a próxima
    for i, (start, end, key) in enumerate(positions):
        next_start = positions[i + 1][0] if i + 1 < len(positions) else end + 400
        value = text[end:next_start].strip()
        value = re.sub(r"\s+", " ", value).strip()
        # Para "interpretação" e "elenco": parar antes de indicação de idade ou sinopse
        if key in ("interpretação", "elenco"):
            value = re.split(r"\s+(?:M/\d+|Todos |Uma |Com |Para |O |A |As |Os )", value)[0]
        value = value[:300].strip()
        if value and key not in ficha:
            ficha[key] = value

    return ficha
