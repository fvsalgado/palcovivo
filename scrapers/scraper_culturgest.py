#!/usr/bin/env python3
"""
Palco Vivo — Scraper: Culturgest

O site carrega a listagem por JavaScript — o HTML da listagem está vazio.
Estratégia: partir de seeds conhecidos (eventos actuais hardcoded) e fazer
crawl progressivo via a secção "Próximos Eventos" que aparece em cada
página de evento (essa secção É HTML estático).

As seeds são actualizadas automaticamente: basta que um evento esteja
acessível para descobrir todos os seguintes via "Próximos Eventos".
"""
import re
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime

from scrapers.utils import make_id, parse_date, log

BASE    = "https://www.culturgest.pt"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; EmCenaBot/1.0)"}

THEATER_NAME = "Culturgest"
SOURCE_SLUG  = "culturgest"

# Seeds: páginas de eventos individuais com "Próximos Eventos" estático.
# Basta um estar válido para descobrir os restantes por crawl.
SEEDS = [
    "https://www.culturgest.pt/pt/programacao/catarina-rolo-salgueiro-e-isabel-costa-os-possessos-burn-burn-burn-2026/",
    "https://www.culturgest.pt/pt/programacao/alex-cassal-ma-criacao-hotel-paradoxo/",
    "https://www.culturgest.pt/pt/programacao/mala-voadora-polo-norte/",
]

_PT_MONTHS_ABBR = {
    "jan":1,"fev":2,"mar":3,"abr":4,"mai":5,"jun":6,
    "jul":7,"ago":8,"set":9,"out":10,"nov":11,"dez":12,
}


def scrape():
    event_urls = _discover_urls()
    log(f"[{THEATER_NAME}] {len(event_urls)} URLs descobertos")

    events   = []
    seen_ids = set()

    for url in sorted(event_urls):
        ev = _scrape_event(url)
        if ev:
            eid = ev["id"]
            if eid not in seen_ids:
                seen_ids.add(eid)
                events.append(ev)
        time.sleep(0.3)

    log(f"[{THEATER_NAME}] {len(events)} eventos")
    return events


# ── DESCOBERTA ────────────────────────────────────────────────────────────────

def _discover_urls():
    """Crawl progressivo a partir das seeds via secção 'Próximos Eventos'."""
    found   = set()
    queue   = set(SEEDS)
    visited = set()

    while queue:
        url = queue.pop()
        if url in visited:
            continue
        visited.add(url)

        links = _next_event_links(url)
        if links:
            found.add(url)            # este URL é um evento válido
            for lnk in links:
                if lnk not in visited:
                    queue.add(lnk)
        time.sleep(0.2)

    # Incluir as seeds originais mesmo que não tenham "Próximos Eventos"
    for s in SEEDS:
        found.add(s)

    return found


def _next_event_links(url):
    """Extrai links /pt/programacao/<slug>/ da secção 'Próximos Eventos'."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as e:
        log(f"[{THEATER_NAME}] Inacessível {url}: {e}")
        return set()

    soup  = BeautifulSoup(r.text, "lxml")
    links = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        full = href if href.startswith("http") else urljoin(BASE, href)
        if _is_event_url(full):
            links.add(full.rstrip("/") + "/")

    return links


def _is_event_url(url):
    """True se for página individual de evento (não listagem nem arquivo)."""
    if not url.startswith(BASE):
        return False
    path = url.replace(BASE, "").strip("/")
    if not path.startswith("pt/programacao/"):
        return False
    parts = [p for p in path.split("/") if p]
    if len(parts) < 3:
        return False
    slug = parts[2]
    # Excluir páginas de listagem conhecidas
    SKIP = {
        "por-evento", "agenda-pdf", "archive", "schedule",
        "por-tipo", "participacao", "convite", "open-call",
        "temporada-2025-26", "concluido",
    }
    return slug not in SKIP


# ── SCRAPING DE EVENTO ────────────────────────────────────────────────────────

def _scrape_event(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as e:
        log(f"[{THEATER_NAME}] Erro em {url}: {e}")
        return None

    soup = BeautifulSoup(r.text, "lxml")

    # Título: primeiro h1
    title_el = soup.find("h1")
    if not title_el:
        return None
    title = title_el.get_text(strip=True)
    if not title or len(title) < 3:
        return None

    # Subtítulo (ex: "Burn Burn Burn") — segundo h1 ou h2 imediato
    subtitle_el = soup.find_all("h1")
    subtitle = ""
    if len(subtitle_el) > 1:
        sub = subtitle_el[1].get_text(strip=True)
        if sub and sub != title:
            subtitle = sub
            title = f"{title} — {subtitle}"

    # Imagem
    image = ""
    og = soup.find("meta", property="og:image")
    if og and og.get("content", "").startswith("http"):
        image = og["content"]
    if not image:
        img = soup.find("img", src=re.compile(r"/media/filer_public"))
        if img:
            image = urljoin(BASE, img["src"])

    # Texto completo
    full_text = soup.get_text(" ")

    # Datas
    dates_label, date_start, date_end = _parse_dates(soup, full_text)

    # Categoria (tags no topo da página)
    category = "Teatro"
    for a in soup.select("ul li a[href*='typology']"):
        txt = a.get_text(strip=True)
        if txt:
            category = txt
            break

    # Descrição: parágrafos > 80 chars do conteúdo principal
    description    = ""
    synopsis_short = ""
    main_el = soup.find("main") or soup.find("article") or soup
    paras = [p.get_text(" ", strip=True) for p in main_el.find_all("p")]
    long_paras = [p for p in paras if len(p) > 80]
    if long_paras:
        description    = " ".join(long_paras)[:2000]
        synopsis_short = long_paras[0][:240]

    # Preço — "15€", "Entrada gratuita"
    price = ""
    m_p = re.search(r"(\d+\s?€(?:\s?[–\-]\s?\d+\s?€)?|[Ee]ntrada gratuita|gratuito)", full_text)
    if m_p:
        price = m_p.group(1)

    # Duração — "1h50", "90 min"
    duration = ""
    m_d = re.search(r"\bDura[çc][aã]o\s+([\w\s]+?)(?:\n|$|[A-Z])", full_text)
    if not m_d:
        m_d = re.search(r"(\d+h\d*|\d+\s?min(?:utos)?)", full_text, re.IGNORECASE)
    if m_d:
        duration = m_d.group(1).strip()

    # Classificação etária
    age_rating = ""
    m_a = re.search(r"M/\d+|Maiores de \d+", full_text, re.IGNORECASE)
    if m_a:
        age_rating = m_a.group(0)

    # Horário — primeiro "21:00" ou "21h00"
    schedule = ""
    m_s = re.search(r"\b(\d{1,2}[h:]\d{2})\b", full_text)
    if m_s:
        schedule = m_s.group(1)

    # Acessibilidade
    accessibility = ""
    m_ac = re.search(
        r"(Audiodes[ck]ri[çc][aã]o|LGP|Língua Gestual|legendas em inglês)",
        full_text, re.IGNORECASE
    )
    if m_ac:
        accessibility = m_ac.group(1)

    # Bilhetes
    ticket_url = ""
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text_a = a.get_text(strip=True).lower()
        if any(x in href.lower() for x in ["ticketline", "bol.pt", "bilhete", "comprar"]) \
           or "comprar bilhete" in text_a:
            ticket_url = href if href.startswith("http") else urljoin(BASE, href)
            break

    return {
        "id":              make_id(SOURCE_SLUG, title),
        "title":           title,
        "theater":         THEATER_NAME,
        "category":        category,
        "dates_label":     dates_label,
        "date_start":      date_start,
        "date_end":        date_end,
        "schedule":        schedule,
        "description":     description,
        "synopsis_short":  synopsis_short,
        "image":           image,
        "url":             url,
        "ticket_url":      ticket_url,
        "price":           price,
        "duration":        duration,
        "age_rating":      age_rating,
        "accessibility":   accessibility,
        "technical_sheet": {},
    }


# ── PARSE DE DATAS ────────────────────────────────────────────────────────────
# Formatos reais no site:
#   "23 ABR 2026"        (data única)
#   "23–25 ABR 2026"     (intervalo mesmo mês, formato compacto)
#   "23 ABR – 25 ABR 2026"
#   "26 JUN – 4 JUL 2026"
#   "11 Abr – 21 Jun 2026"

def _parse_dates(soup, text):
    dates_label = ""
    date_start  = ""
    date_end    = ""

    # Tentar no bloco de datas dedicado do HTML (mais fiável, menos ruído)
    # O site usa padrões como "23 ABR 2026 QUI 21:00" em blocos separados
    date_blocks = []
    for el in soup.find_all(string=re.compile(
        r"\b\d{1,2}\s+[A-Za-z]{3,4}\s+\d{4}\b"
    )):
        date_blocks.append(el.strip())
    # Usar o primeiro bloco como fonte primária para as datas
    sources = date_blocks + [text] if date_blocks else [text]

    for src in sources:
        # 1) "DD MMM YYYY – DD MMM YYYY"  (dois meses distintos, ex: "26 JUN – 4 JUL 2026")
        # Nota: o separador pode ter o ano só no fim ("26 JUN – 4 JUL 2026")
        # ou em ambos ("26 JUN 2026 – 4 JUL 2026")
        m = re.search(
            r"(\d{1,2})\s+([A-Za-z]{3,})(?:\s+(\d{4}))?"
            r"\s*[–—\-]+\s*"
            r"(\d{1,2})\s+([A-Za-z]{3,})\s+(\d{4})",
            src
        )
        if m:
            d1, mo1, y1_opt, d2, mo2, y2 = m.groups()
            n1 = _mon(mo1); n2 = _mon(mo2)
            if n1 and n2 and mo1.lower()[:3] != mo2.lower()[:3]:
                y1 = y1_opt or y2
                dates_label = f"{d1} {mo1} – {d2} {mo2} {y2}"
                date_start  = f"{y1}-{n1:02d}-{int(d1):02d}"
                date_end    = f"{y2}-{n2:02d}-{int(d2):02d}"
                return dates_label, date_start, date_end

        # 2) "DD–DD MMM YYYY"  (mesmo mês, intervalo compacto, ex: "23–25 ABR 2026")
        m = re.search(
            r"(\d{1,2})\s*[–—\-]\s*(\d{1,2})\s+([A-Za-z]{3,})\s+(\d{4})",
            src
        )
        if m:
            d1, d2, mo, y = m.groups()
            n = _mon(mo)
            if n:
                dates_label = f"{d1}–{d2} {mo} {y}"
                date_start  = f"{y}-{n:02d}-{int(d1):02d}"
                date_end    = f"{y}-{n:02d}-{int(d2):02d}"
                return dates_label, date_start, date_end

        # 3) "DD MMM YYYY"  (data única, ex: "23 ABR 2026")
        m = re.search(r"(\d{1,2})\s+([A-Za-z]{3,})\s+(\d{4})", src)
        if m:
            d, mo, y = m.groups()
            n = _mon(mo)
            if n:
                dates_label = f"{d} {mo} {y}"
                date_start  = f"{y}-{n:02d}-{int(d):02d}"
                date_end    = date_start
                return dates_label, date_start, date_end

    return dates_label, date_start, date_end


def _mon(s):
    """Converte abreviatura de mês (PT ou EN, maiúscula ou minúscula) para int."""
    k = s.lower()[:3]
    return _PT_MONTHS_ABBR.get(k)
