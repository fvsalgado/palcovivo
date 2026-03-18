"""
Scraper: Teatro Variedades
Fonte exclusiva: Agenda Teatro Variedades
"""
import re
import time
import requests
from bs4 import BeautifulSoup
from scrapers.utils import make_id, parse_date_range, parse_date, log

BASE = "https://teatrovariedades-capitolio.pt"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; EmCenaBot/1.0)"}

AGENDA_URL = f"{BASE}/agenda/teatro-variedades/?categoria=teatro&layout=grid"
THEATER_NAME = "Teatro Variedades"
SOURCE_SLUG = "teatro-variedades"


def scrape():
    urls, cards = _collect(AGENDA_URL)

    events = []
    seen_ids = set()

    for url in sorted(urls):
        ev = _scrape_event(url)
        if ev:
            eid = ev["id"]
            if eid not in seen_ids:
                seen_ids.add(eid)
                events.append(ev)
        time.sleep(0.3)

    for title, card in cards.items():
        eid = make_id(SOURCE_SLUG, title)
        if eid not in seen_ids:
            seen_ids.add(eid)
            events.append(card)

    log(f"[{THEATER_NAME}] {len(events)} eventos")
    return events


def _collect(agenda_url):
    try:
        r = requests.get(agenda_url, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as e:
        log(f"[{THEATER_NAME}] Erro na listagem: {e}")
        return set(), {}

    soup = BeautifulSoup(r.text, "lxml")
    urls = set()
    stubs = {}

    for article in soup.find_all("article"):
        title_el = article.find(["h2", "h3", "h4"])
        if not title_el:
            continue
        title = title_el.get_text(strip=True)
        if not title:
            continue

        ev_link = None
        for a in article.find_all("a", href=True):
            if "/evento/" in a["href"]:
                ev_link = a["href"] if a["href"].startswith("http") else BASE + a["href"]
                break

        if ev_link:
            urls.add(ev_link)
            continue

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

        stubs[title] = {
            "id": make_id(SOURCE_SLUG, title),
            "title": title,
            "theater": THEATER_NAME,
            "category": "Teatro",
            "dates_label": dates_label,
            "date_start": date_start,
            "date_end": date_end,
            "schedule": "",
            "description": "",
            "synopsis_short": "",
            "image": "",
            "url": agenda_url,
            "ticket_url": ticket_url,
            "price": "",
            "duration": "",
            "age_rating": "",
            "accessibility": "",
            "technical_sheet": {},
        }

    return urls, stubs


def _scrape_event(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as e:
        log(f"[{THEATER_NAME}] Erro em {url}: {e}")
        return None

    soup = BeautifulSoup(r.text, "lxml")
    text = soup.get_text(" ")

    title_el = soup.select_one("h1")
    if not title_el:
        return None
    title = title_el.get_text(strip=True)

    ticket_url = ""
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if any(x in href for x in ["bol.pt", "ticketline", "eventbrite"]):
            ticket_url = href
            break

    image = ""
    og = soup.find("meta", property="og:image")
    if og and og.get("content", "").startswith("http"):
        image = og["content"]

    dates_label, date_start, date_end = _parse_dates(text)

    return {
        "id": make_id(SOURCE_SLUG, title),
        "title": title,
        "theater": THEATER_NAME,
        "category": "Teatro",
        "dates_label": dates_label,
        "date_start": date_start,
        "date_end": date_end,
        "schedule": "",
        "description": "",
        "synopsis_short": "",
        "image": image,
        "url": url,
        "ticket_url": ticket_url,
        "price": "",
        "duration": "",
        "age_rating": "",
        "accessibility": "",
        "technical_sheet": {},
    }


def _parse_dates(text):
    dates_label = ""
    date_start = ""
    date_end = ""

    # Intervalo: DD.MM[.YYYY] – DD.MM.YYYY
    m = re.search(r"(\d{2}\.\d{2}(?:\.\d{4})?)\s*[–—\-]\s*(\d{2}\.\d{2}\.\d{4})", text)
    if m:
        dates_label = f"{m.group(1)} – {m.group(2)}"
        date_start, date_end = parse_date_range(dates_label)
        if date_start:
            return dates_label, date_start, date_end

    # Data isolada: DD.MM.YYYY
    m2 = re.search(r"(\d{2}\.\d{2}\.\d{4})", text)
    if m2:
        dates_label = m2.group(1)
        date_start = date_end = parse_date(dates_label)

    return dates_label, date_start, date_end
