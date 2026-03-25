"""
Culturgest Scraper — v12 (estável)
• Extração robusta via endpoint AJAX
• Filtros seguros
• Paragem automática quando não há novos eventos
"""

import re
import time
import json
import logging
from datetime import datetime, timezone
from typing import List, Optional

import requests
from bs4 import BeautifulSoup

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────

WEBSITE = "https://www.culturgest.pt"
EVENT_LIST_URL = f"{WEBSITE}/pt/programacao/schedule/events/"
REQUEST_DELAY = 0.6
TIMEOUT = 20
MAX_PAGES = 60

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": f"{WEBSITE}/pt/programacao/por-evento/",
}

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# HTTP
# ─────────────────────────────────────────────────────────────

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s

def get(session: requests.Session, url: str, params=None):
    try:
        r = session.get(url, params=params, timeout=TIMEOUT)
        r.raise_for_status()
        return r
    except Exception as e:
        logger.warning(f"Erro HTTP {url}: {e}")
        return None

# ─────────────────────────────────────────────────────────────
# LINKS
# ─────────────────────────────────────────────────────────────

def extract_event_links(html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    links = set()

    for a in soup.select("a[href*='/programacao/']"):
        href = a["href"].strip()

        # Ignorar páginas de navegação
        if any(x in href for x in (
            "/por-evento/",
            "/schedule/events/",
            "?page=",
            "#",
        )):
            continue

        if not href.startswith("http"):
            href = WEBSITE + href

        links.add(href.rstrip("/"))

    return list(links)

# ─────────────────────────────────────────────────────────────
# EVENT PAGE
# ─────────────────────────────────────────────────────────────

def parse_event_page(url: str, session: requests.Session) -> Optional[dict]:
    resp = get(session, url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "lxml")

    title_el = soup.find("h1")
    if not title_el:
        return None

    title = title_el.get_text(strip=True)

    desc_el = soup.select_one(".text-plugin") or soup.select_one("article")
    description = desc_el.get_text("\n", strip=True) if desc_el else None

    cover = None
    og = soup.find("meta", property="og:image")
    if og:
        cover = og.get("content")

    return {
        "source_url": url,
        "title": title,
        "description": description,
        "cover_image": cover,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }

# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def run() -> List[dict]:
    logger.info("Culturgest v12 — scraping estável iniciado")
    session = make_session()

    events = []
    seen_urls = set()
    seen_pages = []

    page = 1
    while page <= MAX_PAGES:
        time.sleep(REQUEST_DELAY)

        resp = get(session, EVENT_LIST_URL, {"page": page})
        if not resp:
            break

        links = extract_event_links(resp.text)
        page_set = frozenset(links)

        logger.info(f"Página {page}: {len(links)} eventos encontrados")

        if not links or page_set in seen_pages:
            logger.info("Paginação concluída")
            break

        seen_pages.append(page_set)

        new_links = [l for l in links if l not in seen_urls]
        if not new_links:
            break

        for link in new_links:
            seen_urls.add(link)
            ev = parse_event_page(link, session)
            if ev:
                events.append(ev)
                logger.info(f"✓ {ev['title'][:70]}")

        page += 1

    logger.info(f"Total eventos: {len(events)}")
    return events

# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    data = run()

    print(f"\nTOTAL: {len(data)} eventos\n")
    if data:
        print(json.dumps(data[0], indent=2, ensure_ascii=False))
