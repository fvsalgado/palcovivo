"""
scrapers/base_variedades.py
Palco Vivo — Lógica partilhada entre Capitólio e Teatro Variedades.

Ambos os teatros partilham o mesmo site (teatrovariedades-capitolio.pt),
a mesma estrutura HTML e a mesma lógica de parsing.
A única diferença é o THEATER_NAME, SOURCE_SLUG e AGENDA_URL,
que são passados como parâmetros para a função scrape_theater().

Uso:
    from scrapers.base_variedades import scrape_theater
    def scrape():
        return scrape_theater(
            theater_name="Capitólio",
            source_slug="teatro-capitolio",
            agenda_url="https://...",
        )
"""

import re
import time
import requests
from bs4 import BeautifulSoup

from scrapers.utils import (
    make_id,
    parse_date_range,
    parse_date,
    log,
    HEADERS,
    can_scrape,
    build_image_object,
)

BASE = "https://teatrovariedades-capitolio.pt"


def scrape_theater(
    theater_name: str,
    source_slug: str,
    agenda_url: str,
) -> list[dict]:
    """
    Ponto de entrada partilhado.
    Recolhe eventos do teatro indicado via agenda_url.
    """
    if not can_scrape(BASE):
        log(f"robots.txt: scraping bloqueado para {BASE}")
        return []

    urls, stubs = _collect(agenda_url, theater_name, source_slug)

    events: list[dict] = []
    seen_ids: set[str] = set()

    # Eventos com página própria — scraping completo
    for url in sorted(urls):
        ev = _scrape_event(url, theater_name, source_slug)
        if ev:
            eid = ev["id"]
            if eid not in seen_ids:
                seen_ids.add(eid)
                events.append(ev)
        time.sleep(0.3)

    # Stubs da listagem (sem página própria) — dados parciais
    for title, stub in stubs.items():
        eid = make_id(source_slug, title)
        if eid not in seen_ids:
            seen_ids.add(eid)
            events.append(stub)

    log(f"[{theater_name}] {len(events)} eventos")
    return events


# ─────────────────────────────────────────────────────────────
# Recolha de URLs e stubs da listagem
# ─────────────────────────────────────────────────────────────

def _collect(
    agenda_url: str,
    theater_name: str,
    source_slug: str,
) -> tuple[set[str], dict[str, dict]]:
    """
    Percorre a listagem e separa:
    - urls: eventos com página própria (/evento/slug/)
    - stubs: eventos sem página própria (dados da listagem apenas)
    """
    try:
        r = requests.get(agenda_url, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as e:
        log(f"[{theater_name}] Erro na listagem: {e}")
        return set(), {}

    soup  = BeautifulSoup(r.text, "lxml")
    urls  = set()
    stubs = {}

    for article in soup.find_all("article"):
        title_el = article.find(["h2", "h3", "h4"])
        if not title_el:
            continue
        title = title_el.get_text(strip=True)
        if not title:
            continue

        # Tentar encontrar link para página própria do evento
        ev_link = None
        for a in article.find_all("a", href=True):
            if "/evento/" in a["href"]:
                ev_link = (
                    a["href"] if a["href"].startswith("http")
                    else BASE + a["href"]
                )
                break

        if ev_link:
            urls.add(ev_link)
            continue

        # Sem página própria — construir stub a partir dos dados da listagem
        card_text = article.get_text(" ")
        dates_label, date_start, date_end = _parse_dates(card_text)
        if not date_start:
            continue

        ticket_url = ""
        for a in article.find_all("a", href=True):
            href = a["href"]
            if any(x in href for x in ["bol.pt", "ticketline", "eventbrite"]):
                ticket_url = href
                break

        # Imagem do card (se disponível)
        image = None
        img_el = article.find("img")
        if img_el:
            src = img_el.get("src") or img_el.get("data-src") or ""
            if src and src.startswith("http"):
                image = build_image_object(src, article, theater_name, agenda_url)

        stubs[title] = {
            "id":              make_id(source_slug, title),
            "title":           title,
            "theater":         theater_name,
            "category":        "Teatro",
            "dates_label":     dates_label,
            "date_start":      date_start,
            "date_end":        date_end,
            "schedule":        "",
            "synopsis":        "",
            "image":           image,
            "source_url":      agenda_url,
            "ticket_url":      ticket_url,
            "price_info":      "",
            "duration":        "",
            "age_rating":      "",
            "accessibility":   "",
            "technical_sheet": {},
        }

    return urls, stubs


# ─────────────────────────────────────────────────────────────
# Scraping de página de evento individual
# ─────────────────────────────────────────────────────────────

def _scrape_event(
    url: str,
    theater_name: str,
    source_slug: str,
) -> dict | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as e:
        log(f"[{theater_name}] Erro em {url}: {e}")
        return None

    soup = BeautifulSoup(r.text, "lxml")
    text = soup.get_text(" ")

    # Título
    title_el = soup.select_one("h1")
    if not title_el:
        return None
    title = title_el.get_text(strip=True)
    if not title or len(title) < 3:
        return None

    # Datas
    dates_label, date_start, date_end = _parse_dates(text)

    # Imagem
    image = None
    og = soup.find("meta", property="og:image")
    if og and og.get("content", "").startswith("http"):
        image = build_image_object(og["content"], soup, theater_name, url)

    # Bilhetes
    ticket_url = ""
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if any(x in href for x in ["bol.pt", "ticketline", "eventbrite"]):
            ticket_url = href
            break

    # Sinopse
    synopsis = ""
    og_desc = soup.find("meta", property="og:description")
    if og_desc:
        synopsis = og_desc.get("content", "").strip()
    if not synopsis:
        for p in soup.select("main p, article p, .entry-content p"):
            t = p.get_text(strip=True)
            if len(t) > 60:
                synopsis = t
                break

    # Preço
    price_info = ""
    pm = re.search(
        r"(Entrada\s+livre"
        r"|\d+(?:[,\.]\d+)?\s*€\s*[-–]\s*\d+(?:[,\.]\d+)?\s*€"
        r"|\d+(?:[,\.]\d+)?[-–]\d+(?:[,\.]\d+)?\s*€"
        r"|\d+(?:[,\.]\d+)?\s*€)",
        text, re.IGNORECASE,
    )
    if pm:
        price_info = pm.group(1).strip()

    # Duração
    duration = ""
    dm = re.search(r"(\d+\s*min\.?|\d+h\d*)", text, re.IGNORECASE)
    if dm:
        duration = dm.group(1).strip()

    # Classificação etária
    age_rating = ""
    am = re.search(r"\b(M\s*/\s*\d+|Livre|\+\d+)\b", text)
    if am:
        age_rating = am.group(1).replace(" ", "")

    return {
        "id":              make_id(source_slug, title),
        "title":           title,
        "theater":         theater_name,
        "category":        "Teatro",
        "dates_label":     dates_label,
        "date_start":      date_start,
        "date_end":        date_end,
        "schedule":        "",
        "synopsis":        synopsis,
        "image":           image,
        "source_url":      url,
        "ticket_url":      ticket_url,
        "price_info":      price_info,
        "duration":        duration,
        "age_rating":      age_rating,
        "accessibility":   "",
        "technical_sheet": {},
    }


# ─────────────────────────────────────────────────────────────
# Parse de datas (partilhado entre listagem e evento)
# ─────────────────────────────────────────────────────────────

def _parse_dates(text: str) -> tuple[str, str, str]:
    """
    Tenta extrair datas de texto livre.
    Formatos suportados:
      - DD.MM[.YYYY] – DD.MM.YYYY  (intervalo)
      - DD.MM.YYYY                 (data única)
    Devolve (dates_label, date_start, date_end).
    """
    # Intervalo: DD.MM[.YYYY] – DD.MM.YYYY
    m = re.search(
        r"(\d{2}\.\d{2}(?:\.\d{4})?)\s*[–—\-]\s*(\d{2}\.\d{2}\.\d{4})",
        text,
    )
    if m:
        dates_label = f"{m.group(1)} – {m.group(2)}"
        date_start, date_end = parse_date_range(dates_label)
        if date_start:
            return dates_label, date_start, date_end

    # Data única: DD.MM.YYYY
    m2 = re.search(r"(\d{2}\.\d{2}\.\d{4})", text)
    if m2:
        dates_label = m2.group(1)
        date_start = date_end = parse_date(dates_label)
        return dates_label, date_start, date_end

    return "", "", ""
