"""Utilitários partilhados pelos scrapers."""
import re
import logging
import urllib.robotparser
from datetime import datetime, date

# ─────────────────────────────────────────────────────────────
# NOTA: logging.basicConfig() foi removido deste módulo.
# A configuração do logging (handlers, formato, ficheiro de log)
# é agora responsabilidade exclusiva do orquestrador (scraper.py).
# Desta forma evita-se conflito de configuração quando o módulo
# é importado antes do orquestrador ter configurado os seus handlers.
# ─────────────────────────────────────────────────────────────

logger = logging.getLogger(__name__)


def log(msg: str) -> None:
    """
    Compatibilidade com scrapers que ainda usam log().
    Encaminha para o logger do módulo.
    Mantido para não partir os scrapers individuais antes de serem actualizados.
    """
    logger.info(msg)


def make_id(prefix: str, title: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", title.lower().strip()).strip("-")[:50]
    return f"{prefix}-{slug}"


MONTHS = {
    "jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6,
    "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12,
    "janeiro": 1, "fevereiro": 2, "março": 3, "marco": 3, "abril": 4,
    "maio": 5, "junho": 6, "julho": 7, "agosto": 8,
    "setembro": 9, "outubro": 10, "novembro": 11, "dezembro": 12,
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
    "feb": 2, "apr": 4, "aug": 8, "sep": 9, "oct": 10, "dec": 12,
}


def parse_date(s: str, force_year: int | None = None) -> str:
    """
    Converte string de data para 'YYYY-MM-DD'.
    force_year: se fornecido, usa esse ano em vez de inferir.
    """
    if not s:
        return ""
    s = s.strip()
    # DD/MM/YYYY ou DD.MM.YYYY
    m = re.match(r"(\d{1,2})[/.](\d{1,2})[/.](\d{4})", s)
    if m:
        d, mo, y = int(m[1]), int(m[2]), int(m[3])
        try:
            date(y, mo, d)
            return f"{y:04d}-{mo:02d}-{d:02d}"
        except ValueError:
            return ""
    # DD [de] MES [YYYY]
    m = re.match(
        r"(\d{1,2})\s+(?:de\s+)?([A-Za-zçãáéíóúàèìòùÇÃÁÉÍÓÚ]{3,})(?:\s+(\d{4}))?",
        s, re.IGNORECASE,
    )
    if m:
        d = int(m[1])
        mon = MONTHS.get(m[2].lower()) or MONTHS.get(m[2].lower()[:3])
        if not mon:
            return ""
        if m[3]:
            y = int(m[3])
        elif force_year:
            y = force_year
        else:
            now = datetime.now()
            y = now.year
            if mon < now.month or (mon == now.month and d < now.day):
                y = now.year + 1
        try:
            date(y, mon, d)
            return f"{y:04d}-{mon:02d}-{d:02d}"
        except ValueError:
            return ""
    return ""


def parse_date_range(s: str) -> tuple[str, str]:
    """Converte intervalo para (date_start, date_end). Aceita variados formatos."""
    if not s:
        return "", ""
    s = s.strip()
    parts = re.split(r"\s*[–—]\s*|\s+[aA]\s+", s, maxsplit=1)
    if len(parts) == 2:
        start_s, end_s = parts[0].strip(), parts[1].strip()
        date_end = parse_date(end_s)
        year_end = re.search(r"\d{4}", end_s)
        year_end_val = int(year_end.group()) if year_end else None
        if not year_end_val and date_end:
            year_end_val = int(date_end[:4])
        if re.match(r"^\d{2}\.\d{2}$", start_s) and year_end_val:
            start_s += f".{year_end_val}"
            date_start = parse_date(start_s)
        elif re.match(r"^\d{1,2}$", start_s):
            month_m = re.search(r"[A-Za-zçãáéíóúÇÃÁÉÍÓÚ]{3,}", end_s)
            if month_m:
                start_s = f"{start_s} {month_m.group()}"
                if year_end_val:
                    start_s += f" {year_end_val}"
            date_start = parse_date(start_s, force_year=year_end_val)
        elif not re.search(r"\d{4}", start_s) and year_end_val:
            date_start = parse_date(start_s, force_year=year_end_val)
        else:
            date_start = parse_date(start_s)
        return date_start, date_end
    d = parse_date(s)
    return d, d


# ─────────────────────────────────────────────────────────────
# CONFORMIDADE — funções de scraping ético
# ─────────────────────────────────────────────────────────────

# User-Agent e headers padrão para todos os scrapers
HEADERS = {
    "User-Agent": "PalcoVivo-Scraper/1.0 (+https://www.palcovivo.pt; fabio@palcovivo.pt)",
    "Accept-Language": "pt-PT,pt;q=0.9",
}


def truncate_synopsis(text: str, max_chars: int = 300) -> str:
    """
    Devolve excerto máximo de max_chars caracteres.
    Corta na última frase completa (., !, ?) antes do limite.
    Só corta em frase se resultado tiver mais de 150 chars.
    Adiciona '…' no final se truncado.
    """
    if not text:
        return ""
    text = text.strip()
    if len(text) <= max_chars:
        return text
    truncated = text[:max_chars]
    last_sentence = max(
        truncated.rfind("."),
        truncated.rfind("!"),
        truncated.rfind("?"),
    )
    if last_sentence > 150:
        return truncated[:last_sentence + 1] + "…"
    return truncated + "…"


def build_image_object(
    url: str,
    page_soup,
    theater_name: str,
    source_url: str,
) -> dict | None:
    """
    Tenta extrair crédito fotográfico da página BeautifulSoup.
    Devolve dict {url, credit, source, theater} ou None se url vazio.
    """
    if not url:
        return None
    credit = None
    if page_soup:
        try:
            for fig in (page_soup.find_all("figure") or []):
                img_tag = fig.find("img")
                if img_tag and img_tag.get("src", "") == url:
                    cap = fig.find("figcaption")
                    if cap and cap.get_text(strip=True):
                        credit = cap.get_text(strip=True)[:120]
                        break
            if not credit:
                for img_tag in (page_soup.find_all("img") or []):
                    if img_tag.get("src", "") == url:
                        alt = (img_tag.get("alt") or "").strip()
                        if len(alt) >= 10:
                            credit = alt[:120]
                        break
            if not credit:
                page_text = page_soup.get_text(" ", strip=True)
                m = re.search(
                    r"(?:Foto:|Fotografia:|©\s*|Crédito:)\s*(.{5,80})",
                    page_text,
                    re.IGNORECASE,
                )
                if m:
                    credit = m.group(1).strip()[:80]
        except Exception:
            pass
    return {
        "url": url,
        "credit": credit,
        "source": source_url,
        "theater": theater_name,
    }


def can_scrape(base_url: str, path: str = "/") -> bool:
    """
    Verifica robots.txt para o user-agent PalcoVivo-Scraper.
    Se inacessível, assume True e faz log.
    """
    rp = urllib.robotparser.RobotFileParser()
    try:
        robots_url = base_url.rstrip("/") + "/robots.txt"
        rp.set_url(robots_url)
        import socket
        old_timeout = socket.getdefaulttimeout()
        socket.setdefaulttimeout(5)
        try:
            rp.read()
        finally:
            socket.setdefaulttimeout(old_timeout)
        return rp.can_fetch("PalcoVivo-Scraper", base_url.rstrip("/") + path)
    except Exception as e:
        logger.warning(
            f"can_scrape: não foi possível verificar robots.txt "
            f"de {base_url} ({e}). A assumir permitido."
        )
        return True
