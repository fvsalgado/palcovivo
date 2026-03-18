"""Utilitários partilhados pelos scrapers."""
import re
import logging
from datetime import datetime, date

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
)

def log(msg):
    logging.info(msg)

def make_id(prefix, title):
    slug = re.sub(r"[^a-z0-9]+", "-", title.lower().strip()).strip("-")[:50]
    return f"{prefix}-{slug}"

MONTHS = {
    "jan":1, "fev":2, "mar":3, "abr":4, "mai":5, "jun":6,
    "jul":7, "ago":8, "set":9, "out":10, "nov":11, "dez":12,
    "janeiro":1, "fevereiro":2, "março":3, "marco":3, "abril":4,
    "maio":5, "junho":6, "julho":7, "agosto":8,
    "setembro":9, "outubro":10, "novembro":11, "dezembro":12,
    "january":1, "february":2, "march":3, "april":4, "may":5, "june":6,
    "july":7, "august":8, "september":9, "october":10, "november":11, "december":12,
    "feb":2, "apr":4, "aug":8, "sep":9, "oct":10, "dec":12,
}

def parse_date(s, force_year=None):
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
            # sem ano: inferir — se o mês já passou, usar próximo ano
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

def parse_date_range(s):
    """Converte intervalo para (date_start, date_end). Aceita variados formatos."""
    if not s:
        return "", ""
    s = s.strip()
    # separadores: –, —, -, " a "
    parts = re.split(r"\s*[–—]\s*|\s+[aA]\s+", s, maxsplit=1)
    if len(parts) == 2:
        start_s, end_s = parts[0].strip(), parts[1].strip()

        # Calcular date_end primeiro para obter o ano
        date_end = parse_date(end_s)

        # Extrair ano do fim para usar no início se necessário
        year_end = re.search(r"\d{4}", end_s)
        year_end_val = int(year_end.group()) if year_end else None

        # Se não há ano no fim, inferir do parse_date e extrair
        if not year_end_val and date_end:
            year_end_val = int(date_end[:4])

        # formato DD.MM sem ano: propagar como DD.MM.YYYY
        if re.match(r"^\d{2}\.\d{2}$", start_s) and year_end_val:
            start_s += f".{year_end_val}"
            date_start = parse_date(start_s)
        # início só com dígito(s): propagar mês do fim
        elif re.match(r"^\d{1,2}$", start_s):
            month_m = re.search(r"[A-Za-zçãáéíóúÇÃÁÉÍÓÚ]{3,}", end_s)
            if month_m:
                start_s = f"{start_s} {month_m.group()}"
                if year_end_val:
                    start_s += f" {year_end_val}"
            date_start = parse_date(start_s, force_year=year_end_val)
        # início sem ano explícito: usar o mesmo ano que o fim
        elif not re.search(r"\d{4}", start_s) and year_end_val:
            date_start = parse_date(start_s, force_year=year_end_val)
        else:
            date_start = parse_date(start_s)

        return date_start, date_end
    d = parse_date(s)
    return d, d
