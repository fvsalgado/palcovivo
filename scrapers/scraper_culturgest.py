#!/usr/bin/env python3
"""
Palco Vivo — Scraper: Culturgest

Estratégia: a página de listagem (/por-evento/) é JavaScript puro e não
renderiza nada com BeautifulSoup. Em vez disso, usamos um evento conhecido
como ponto de entrada e extraímos os links da secção "Próximos Eventos"
que aparece no rodapé de cada página individual — essa secção É estática.
As páginas individuais dos eventos são também HTML estático e ricas em dados.
"""
import re
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from scrapers.utils import make_id, parse_date_range, parse_date, log

BASE    = "https://www.culturgest.pt"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; EmCenaBot/1.0)"}

# Ponto de entrada: página de listagem de teatro (mesmo sendo JS,
# serve para arrancar; se falhar usamos a seed abaixo)
AGENDA_URL  = "https://www.culturgest.pt/pt/programacao/por-evento/?typology=1"
# Seed estática: uma página de evento que sempre tem "Próximos Eventos"
SEED_URL    = "https://www.culturgest.pt/pt/programacao/por-evento/"

THEATER_NAME = "Culturgest"
SOURCE_SLUG  = "culturgest"

# Meses em português abreviados usados no site
_PT_MONTHS = {
    "jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6,
    "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12,
}


def scrape():
    event_urls = _discover_urls()

    events    = []
    seen_ids  = set()

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


# ── DESCOBERTA DE URLs ────────────────────────────────────────────────────────

def _discover_urls():
    """
    Recolhe URLs de eventos de duas formas complementares:
    1. Tenta a listagem /por-evento/?typology=1 (mesmo sendo JS, às vezes
       tem links no HTML base ou no sitemap).
    2. Vai buscar a secção "Próximos Eventos" a partir de uma página semente.
    Faz crawl progressivo: cada novo evento encontrado pode revelar mais URLs
    via a sua própria secção "Próximos Eventos".
    """
    found  = set()
    queue  = set()

    # Tentativa 1: listagem (pode não devolver nada)
    queue.update(_links_from_listing(AGENDA_URL))

    # Tentativa 2: seed via página de arquivo / listagem geral
    queue.update(_links_from_listing(SEED_URL))

    # Se ainda vazio, arrancamos com um URL de evento recente conhecido
    # (será actualizado automaticamente via "Próximos Eventos")
    if not queue:
        # URL genérico de arquivo — contém eventos recentes em HTML estático
        queue.update(_links_from_listing(
            "https://www.culturgest.pt/pt/programacao/archive/"
        ))

    # Crawl: cada evento tem secção "Próximos Eventos" → novos links
    visited = set()
    for url in list(queue):
        if url in visited:
            continue
        visited.add(url)
        found.add(url)
        new_links = _next_events_from_page(url)
        for lnk in new_links:
            if lnk not in visited:
                queue.add(lnk)
        time.sleep(0.25)

    return found


def _links_from_listing(url):
    """Tenta extrair links /pt/programacao/ de uma página de listagem."""
    urls = set()
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as e:
        log(f"[{THEATER_NAME}] Listagem inacessível ({url}): {e}")
        return urls

    soup = BeautifulSoup(r.text, "lxml")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        full = urljoin(BASE, href)
        if _is_event_url(full):
            urls.add(full)
    return urls


def _next_events_from_page(url):
    """Extrai os links da secção 'Próximos Eventos' de uma página de evento."""
    urls = set()
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except Exception:
        return urls

    soup = BeautifulSoup(r.text, "lxml")
    for a in soup.find_all("a", href=True):
        full = urljoin(BASE, a["href"])
        if _is_event_url(full):
            urls.add(full)
    return urls


def _is_event_url(url):
    """Heurística: URL de evento individual (não listagem, não arquivo genérico)."""
    if not url.startswith(BASE):
        return False
    path = url.replace(BASE, "")
    if not path.startswith("/pt/programacao/"):
        return False
    # Excluir páginas de listagem/arquivo
    skip = [
        "/por-evento", "/agenda-pdf", "/archive", "/schedule",
        "/programacao-de-participacao", "/convite", "/open-call",
    ]
    for s in skip:
        if s in path:
            return False
    # Deve ter pelo menos um segmento depois de /programacao/
    parts = [p for p in path.split("/") if p]
    return len(parts) >= 3


# ── SCRAPING DE EVENTO INDIVIDUAL ────────────────────────────────────────────

def _scrape_event(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as e:
        log(f"[{THEATER_NAME}] Erro em {url}: {e}")
        return None

    soup = BeautifulSoup(r.text, "lxml")

    # Título: h1 principal (o site repete o h1 — pegar o primeiro)
    title_el = soup.find("h1")
    if not title_el:
        return None
    title = title_el.get_text(strip=True)
    if not title or len(title) < 3:
        return None

    # Imagem: og:image ou primeira img de conteúdo
    image = ""
    og = soup.find("meta", property="og:image")
    if og and og.get("content", "").startswith("http"):
        image = og["content"]
    if not image:
        img = soup.find("img", src=re.compile(r"/media/filer_public"))
        if img:
            image = urljoin(BASE, img["src"])

    # Texto completo para extracção
    text = soup.get_text(" ")

    # Datas — formato do site: "03 NOV 2025", "23–25 Abr 2026",
    # "11 Abr – 21 Jun 2026", "28 NOV 2024 – 3 MAR 2025"
    dates_label, date_start, date_end = _parse_dates_cg(soup, text)

    # Descrição: parágrafos longos do conteúdo principal
    description   = ""
    synopsis_short = ""
    main = soup.find("main") or soup.find("article") or soup
    paragraphs = [p.get_text(strip=True) for p in main.find_all("p")]
    long_ps = [p for p in paragraphs if len(p) > 80]
    if long_ps:
        description    = " ".join(long_ps)[:2000]
        synopsis_short = long_ps[0][:240]

    # Preço
    price = ""
    m_price = re.search(r"(\d+\s?€(?:\s?[–\-]\s?\d+\s?€)?|Entrada gratuita|gratuito)", text, re.IGNORECASE)
    if m_price:
        price = m_price.group(1)

    # Classificação etária
    age_rating = ""
    m_age = re.search(r"M/\d+|maiores de \d+", text, re.IGNORECASE)
    if m_age:
        age_rating = m_age.group(0)

    # Horário
    schedule = ""
    m_sched = re.search(r"\b(\d{1,2}[h:]\d{0,2})\b", text)
    if m_sched:
        schedule = m_sched.group(1)

    # Duração
    duration = ""
    m_dur = re.search(r"(\d+h\d+|\d+\s?min(?:utos)?)", text, re.IGNORECASE)
    if m_dur:
        duration = m_dur.group(1)

    # Bilhetes
    ticket_url = ""
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text_a = a.get_text(strip=True).lower()
        if any(x in href.lower() for x in ["bilhete", "ticket", "ticketline", "bol.pt", "comprar"]) \
           or any(x in text_a for x in ["comprar bilhete", "bilheteira", "tickets"]):
            ticket_url = urljoin(BASE, href)
            break

    # Categoria a partir das tags no topo
    category = "Teatro"
    cat_links = soup.select("ul li a[href*='typology']")
    if cat_links:
        category = cat_links[0].get_text(strip=True)

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
        "accessibility":   "",
        "technical_sheet": {},
    }


# ── PARSE DE DATAS ────────────────────────────────────────────────────────────

def _parse_dates_cg(soup, text):
    """
    Formatos encontrados no site Culturgest:
      "03 NOV 2025"
      "23–25 Abr 2026"
      "11 Abr – 21 Jun 2026"
      "28 NOV 2024 – 3 MAR 2025"
      "13–22 FEV 2025"
    """
    dates_label = ""
    date_start  = ""
    date_end    = ""

    # Tentar extrair do elemento de data no HTML (mais fiável)
    # O site usa padrões como "03 NOV 2025" num bloco de texto próprio
    date_block = ""
    for el in soup.select("time, .date, .dates, [class*='date'], [class*='Date']"):
        t = el.get_text(strip=True)
        if re.search(r"\d{4}", t):
            date_block = t
            break

    # Combinar: tentar no bloco de data primeiro, depois no texto completo
    for src in ([date_block, text] if date_block else [text]):
        # Padrão: DD? MÊS YYYY – DD? MÊS YYYY  (intervalo com meses diferentes)
        m = re.search(
            r"(\d{1,2}\s+[A-Za-zçãáéíóúÇÃÁÉÍÓÚ]{3,}\.?\s+\d{4})"
            r"\s*[–—\-]\s*"
            r"(\d{1,2}\s+[A-Za-zçãáéíóúÇÃÁÉÍÓÚ]{3,}\.?\s+\d{4})",
            src
        )
        if m:
            dates_label = f"{m.group(1).strip()} – {m.group(2).strip()}"
            date_start, date_end = parse_date_range(dates_label)
            if date_start:
                return dates_label, date_start, date_end

        # Padrão: DD–DD MÊS YYYY  (mesmo mês)
        m = re.search(
            r"(\d{1,2})\s*[–—\-]\s*(\d{1,2})\s+([A-Za-zçãáéíóúÇÃÁÉÍÓÚ]{3,}\.?)\s+(\d{4})",
            src
        )
        if m:
            d1, d2, mon_str, year = m.group(1), m.group(2), m.group(3), m.group(4)
            mon_key = mon_str.lower().rstrip(".").strip()[:3]
            mon = _PT_MONTHS.get(mon_key)
            if mon:
                dates_label = f"{d1} {mon_str} {year} – {d2} {mon_str} {year}"
                date_start  = f"{year}-{mon:02d}-{int(d1):02d}"
                date_end    = f"{year}-{mon:02d}-{int(d2):02d}"
                return dates_label, date_start, date_end

        # Padrão: DD? MÊS YYYY  (data única)
        m = re.search(
            r"(\d{1,2})\s+([A-Za-zçãáéíóúÇÃÁÉÍÓÚ]{3,}\.?)\s+(\d{4})",
            src
        )
        if m:
            d, mon_str, year = m.group(1), m.group(2), m.group(3)
            mon_key = mon_str.lower().rstrip(".").strip()[:3]
            mon = _PT_MONTHS.get(mon_key)
            if mon:
                dates_label = f"{d} {mon_str} {year}"
                date_start  = f"{year}-{mon:02d}-{int(d):02d}"
                date_end    = date_start
                return dates_label, date_start, date_end

    # Fallback: parse_date padrão
    m2 = re.search(r"(\d{2}[./]\d{2}[./]\d{4})", text)
    if m2:
        dates_label = m2.group(1)
        date_start = date_end = parse_date(dates_label)

    return dates_label, date_start, date_end
