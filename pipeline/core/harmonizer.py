"""
Primeira Plateia — Harmonizador
Converte dados raw de qualquer venue para o schema canónico.
"""

import re
import unicodedata
import hashlib
import json
from datetime import datetime, date, timezone
from typing import Optional
from .taxonomy import ALIASES, AUDIENCE_MAP, SERIES_PREFIX_PATTERNS, CATEGORIES, DOMAINS, log_unknown_tag


# ---------------------------------------------------------------------------
# TEXTO
# ---------------------------------------------------------------------------

def slugify(text: str) -> str:
    """Converte texto para slug kebab-case sem acentos."""
    text = text.lower().strip()
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = re.sub(r"[^a-z0-9\s-]", "", text)
    text = re.sub(r"[\s]+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text.strip("-")


def normalize_title(title: str) -> str:
    """
    Normaliza título: title case português, remove duplos espaços,
    normaliza aspas, remove HTML tags.
    """
    if not title:
        return ""
    # Remove HTML
    title = re.sub(r"<[^>]+>", "", title)
    # Normaliza espaços
    title = re.sub(r"\s+", " ", title).strip()
    # Normaliza aspas
    title = title.replace('"', '«').replace('"', '»').replace('"', '«')
    # Remove pontuação desnecessária no fim
    title = re.sub(r"[.,:;]+$", "", title).strip()
    return title


def clean_description(text: str) -> str:
    """Remove HTML tags mantendo parágrafos."""
    if not text:
        return ""
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</p>", "\n\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def truncate_description(text: str, max_chars: int = 300) -> str:
    """Corta descrição em limite de caracteres sem quebrar palavras."""
    if not text or len(text) <= max_chars:
        return text
    truncated = text[:max_chars]
    last_space = truncated.rfind(" ")
    if last_space > 0:
        truncated = truncated[:last_space]
    return truncated.rstrip(".,;:") + "…"


# ---------------------------------------------------------------------------
# DATAS
# ---------------------------------------------------------------------------

MONTH_MAP = {
    "janeiro": "01", "fevereiro": "02", "março": "03", "marco": "03",
    "abril": "04", "maio": "05", "junho": "06", "julho": "07",
    "agosto": "08", "setembro": "09", "outubro": "10",
    "novembro": "11", "dezembro": "12",
    "jan": "01", "fev": "02", "mar": "03", "abr": "04",
    "mai": "05", "jun": "06", "jul": "07", "ago": "08",
    "set": "09", "out": "10", "nov": "11", "dez": "12",
}

DAY_MAP = {
    "seg": "monday", "ter": "tuesday", "qua": "wednesday",
    "qui": "thursday", "sex": "friday", "sáb": "saturday",
    "sab": "saturday", "dom": "sunday",
}


def parse_date(raw: str, reference_year: Optional[int] = None) -> Optional[str]:
    """
    Tenta converter qualquer formato de data raw para ISO 8601 (YYYY-MM-DD).
    Retorna None se não conseguir.
    """
    if not raw:
        return None

    raw = raw.strip()
    year = reference_year or datetime.now().year

    # ISO 8601 direto
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", raw)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    # DD/MM/YYYY ou DD-MM-YYYY
    m = re.match(r"^(\d{1,2})[/\-.](\d{1,2})[/\-.](\d{4})", raw)
    if m:
        return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"

    # DD/MM/YY
    m = re.match(r"^(\d{1,2})[/\-.](\d{1,2})[/\-.](\d{2})$", raw)
    if m:
        y = int(m.group(3))
        full_year = 2000 + y if y < 50 else 1900 + y
        return f"{full_year}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"

    # "15 de março de 2026" / "15 março 2026" / "15 março"
    m = re.match(
        r"^(\d{1,2})\s+(?:de\s+)?([a-záéíóúâêôãõç]+)(?:\s+(?:de\s+)?(\d{4}))?",
        raw.lower()
    )
    if m:
        day = m.group(1).zfill(2)
        month_str = m.group(2)
        yr = m.group(3) or str(year)
        month = MONTH_MAP.get(month_str[:3])
        if month:
            return f"{yr}-{month}-{day}"

    # "SAB 15 MAR" / "QUI 23 ABR"
    m = re.match(r"^[a-záéíóú]{3}\s+(\d{1,2})\s+([a-z]{3})", raw.lower())
    if m:
        day = m.group(1).zfill(2)
        month = MONTH_MAP.get(m.group(2))
        if month:
            return f"{year}-{month}-{day}"

    return None


def parse_time(raw: str) -> Optional[str]:
    """Converte hora raw para HH:MM."""
    if not raw:
        return None
    raw = raw.strip()

    # HH:MM ou H:MM
    m = re.match(r"^(\d{1,2}):(\d{2})", raw)
    if m:
        return f"{m.group(1).zfill(2)}:{m.group(2)}"

    # 19h00 / 19H / 19h
    m = re.match(r"^(\d{1,2})[hH](\d{2})?", raw)
    if m:
        h = m.group(1).zfill(2)
        mi = m.group(2) or "00"
        return f"{h}:{mi}"

    # 19.00
    m = re.match(r"^(\d{1,2})\.(\d{2})", raw)
    if m:
        return f"{m.group(1).zfill(2)}:{m.group(2)}"

    # 7pm / 7am
    m = re.match(r"^(\d{1,2})(?::(\d{2}))?\s*(am|pm)", raw.lower())
    if m:
        h = int(m.group(1))
        mi = m.group(2) or "00"
        if m.group(3) == "pm" and h < 12:
            h += 12
        elif m.group(3) == "am" and h == 12:
            h = 0
        return f"{h:02d}:{mi}"

    return None


# ---------------------------------------------------------------------------
# PREÇOS
# ---------------------------------------------------------------------------

FREE_PATTERNS = [
    r"entr(?:ada)?\s*(?:livre|gratuita|free)",
    r"^gratuito$",
    r"^free$",
    r"^0[,.]?00\s*€?$",
    r"^sem\s+custo$",
]

def parse_price(raw: str) -> dict:
    """
    Converte string de preço raw para dict canónico.
    Retorna: {is_free, price_min, price_max, price_display, price_raw}
    """
    if not raw:
        return {"is_free": False, "price_min": None, "price_max": None,
                "price_display": None, "price_raw": None}

    raw_clean = raw.strip()

    # Verificar gratuito
    for pattern in FREE_PATTERNS:
        if re.search(pattern, raw_clean, re.IGNORECASE):
            return {
                "is_free": True, "price_min": None, "price_max": None,
                "price_display": "Entrada livre", "price_raw": raw_clean,
                "has_discounts": False, "discount_notes": None
            }

    # Extrair todos os valores numéricos (aceita vírgula decimal PT ou ponto EN)
    nums = re.findall(r"\d+[,.]?\d*", raw_clean)
    values = []
    for n in nums:
        try:
            values.append(float(n.replace(",", ".")))
        except ValueError:
            pass

    if not values:
        return {"is_free": False, "price_min": None, "price_max": None,
                "price_display": raw_clean, "price_raw": raw_clean,
                "has_discounts": False, "discount_notes": None}

    price_min = min(values)
    price_max = max(values)

    if price_min == price_max:
        display = f"{price_min:.0f}€" if price_min == int(price_min) else f"{price_min:.2f}€"
    else:
        mn = f"{price_min:.0f}" if price_min == int(price_min) else f"{price_min:.2f}"
        mx = f"{price_max:.0f}" if price_max == int(price_max) else f"{price_max:.2f}"
        display = f"{mn}€ – {mx}€"

    return {
        "is_free": False,
        "price_min": price_min,
        "price_max": price_max if price_max != price_min else price_min,
        "price_display": display,
        "price_raw": raw_clean,
        "has_discounts": bool(re.search(r"desconto|jovem|sénior|estudante|reforma", raw_clean, re.I)),
        "discount_notes": None,
    }


# ---------------------------------------------------------------------------
# CATEGORIAS
# ---------------------------------------------------------------------------

def harmonize_category(raw_categories: list[str], venue_id: str = "") -> dict:
    """
    Recebe lista de categorias raw (como vieram do venue).
    Devolve {domain, category, subcategory, flags_extras}.
    venue_id é opcional — usado para log_unknown_tag quando nenhum alias é encontrado.
    """
    result = {"domain": "outros", "category": "outros", "subcategory": None, "flags": {}}

    for raw in raw_categories:
        key = raw.strip().lower()
        # Tentativa direta
        if key in ALIASES:
            entry = ALIASES[key]
            if entry["domain"]:  # não é um contexto ignorado
                result["domain"] = entry["domain"]
                result["category"] = entry["category"]
                result["subcategory"] = entry["subcategory"]
            result["flags"].update(entry.get("flags", {}))
            return result

        # Tentativa por slug
        slug_key = slugify(raw)
        if slug_key in ALIASES:
            entry = ALIASES[slug_key]
            if entry["domain"]:
                result["domain"] = entry["domain"]
                result["category"] = entry["category"]
                result["subcategory"] = entry["subcategory"]
            result["flags"].update(entry.get("flags", {}))
            return result

    # Nenhum alias encontrado — registar tags desconhecidas que caem em "outros"
    if result["domain"] == "outros" and raw_categories:
        for raw in raw_categories:
            tag = raw.strip()
            if tag:
                log_unknown_tag(tag, venue_id or "unknown")

    return result


# ---------------------------------------------------------------------------
# PÚBLICO / IDADE
# ---------------------------------------------------------------------------

def harmonize_audience(raw: str) -> dict:
    """Converte string de público raw para dict canónico."""
    default = {
        "label": None, "label_raw": raw, "age_min": None, "age_max": None,
        "is_family": False, "is_educational": False, "school_level": None, "notes": None
    }
    if not raw:
        return default

    key = raw.strip().lower()
    if key in AUDIENCE_MAP:
        entry = AUDIENCE_MAP[key]
        return {
            "label": entry["label"],
            "label_raw": raw,
            "age_min": entry["age_min"],
            "age_max": entry["age_max"],
            "is_family": entry["is_family"],
            "is_educational": entry.get("school_level") is not None and entry["age_min"] is not None and entry["age_min"] < 18,
            "school_level": entry["school_level"],
            "notes": None,
        }

    # Tentativa: extrair M/NN
    m = re.match(r"m[/\s]?(\d+)", key)
    if m:
        age = int(m.group(1))
        return {**default, "age_min": age, "label": f"M/{age} anos", "label_raw": raw}

    return {**default, "label": raw}


# ---------------------------------------------------------------------------
# STATUS DO EVENTO
# ---------------------------------------------------------------------------

STATUS_PATTERNS = {
    "estreia": [r"\bestreia\b", r"\bpremiere\b", r"\bworld premiere\b"],
    "reposicao": [r"\breposição\b", r"\breposicao\b", r"\breprise\b"],
    "ultima-sessao": [r"\b[uú]ltima\s+sess[aã]o\b", r"\b[uú]ltima\s+noite\b", r"\b[uú]ltimos\s+dias\b"],
    "unica-sessao": [],  # inferido se só há 1 data
}

def detect_event_status(title: str, description: str, num_dates: int) -> str:
    """Deteta status do evento a partir de texto e número de sessões."""
    text = f"{title} {description}".lower()
    for status, patterns in STATUS_PATTERNS.items():
        for p in patterns:
            if re.search(p, text, re.IGNORECASE):
                return status
    if num_dates == 1:
        return "unica-sessao"
    return "em-cartaz"


# ---------------------------------------------------------------------------
# FESTIVAL / SÉRIE
# ---------------------------------------------------------------------------

def extract_series(title: str) -> tuple[str, Optional[str]]:
    """
    Tenta extrair série programática do título.
    Ex: "Sexta Maior — Beethoven" → ("Beethoven", "Sexta Maior")
    Retorna (titulo_limpo, series_name_ou_None)
    """
    for pattern in SERIES_PREFIX_PATTERNS:
        m = re.match(pattern, title, re.IGNORECASE)
        if m:
            series = m.group(1).strip()
            clean_title = title[m.end():].strip()
            return clean_title, series
    return title, None


def detect_festival(title: str, description: str) -> tuple[bool, Optional[str]]:
    """Deteta se é um festival e extrai nome."""
    text = f"{title} {description}"
    m = re.search(r"(festival\s+[a-záéíóúâêôãõç\s]+)", text, re.IGNORECASE)
    if m:
        return True, m.group(1).strip().title()
    return False, None


# ---------------------------------------------------------------------------
# DEDUPLICAÇÃO
# ---------------------------------------------------------------------------

def generate_fingerprint(title: str, venue_id: str, date_first: str) -> str:
    """Gera fingerprint para detetar duplicados entre venues."""
    # Normalizar título: lowercase, sem acentos, só alfanumérico
    norm = slugify(normalize_title(title))
    # Usar só mês+ano para tolerar espetáculos com datas ligeiramente diferentes
    date_prefix = date_first[:7] if date_first else "unknown"  # YYYY-MM
    raw = f"{norm}|{date_prefix}"
    return raw


def generate_event_id(venue_id: str, title: str, date_first: str) -> str:
    """Gera ID único canónico para o evento."""
    title_slug = slugify(normalize_title(title))[:40]
    date_part = date_first or "0000-00-00"
    return f"{venue_id}-{date_part[:7]}-{title_slug}"


def hash_raw(data: dict) -> str:
    """Hash SHA256 dos dados raw para detetar alterações."""
    raw = json.dumps(data, sort_keys=True, ensure_ascii=False)
    return "sha256:" + hashlib.sha256(raw.encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# HARMONIZADOR PRINCIPAL
# ---------------------------------------------------------------------------

def harmonize_event(raw_event: dict, venue_id: str, scraper_id: str) -> dict:
    """
    Recebe evento raw (qualquer formato) e devolve evento no schema canónico.
    raw_event deve ter pelo menos: title, dates[], source_id, source_url
    """
    now = datetime.now(timezone.utc).isoformat()  # ISO 8601 com +00:00, sem Z redundante

    # Título
    title_raw = raw_event.get("title", "")
    title = normalize_title(title_raw)
    title, series_name = extract_series(title)

    # Descrição
    desc_raw = raw_event.get("description", "") or ""
    description = clean_description(desc_raw)
    description_short = truncate_description(description)

    # Datas
    raw_dates = raw_event.get("dates", [])
    harmonized_dates = []
    for d in raw_dates:
        date_iso = parse_date(d.get("date", "")) or d.get("date")
        time_start = parse_time(d.get("time_start", ""))
        time_end = parse_time(d.get("time_end", ""))
        harmonized_dates.append({
            "date": date_iso,
            "time_start": time_start,
            "time_end": time_end,
            "duration_minutes": d.get("duration_minutes"),
            "is_cancelled": d.get("is_cancelled", False),
            "is_sold_out": d.get("is_sold_out", False),
            "notes": d.get("notes"),
        })

    valid_dates = [d["date"] for d in harmonized_dates if d["date"]]
    date_first = min(valid_dates) if valid_dates else None
    date_last = max(valid_dates) if valid_dates else None

    # Categorias
    raw_cats = raw_event.get("categories", [])
    cat_result = harmonize_category(raw_cats, venue_id=venue_id)
    extra_flags = cat_result.pop("flags", {})

    # Série / Festival
    is_festival, festival_name = detect_festival(title, description)
    if not series_name:
        series_name = extra_flags.pop("series_name", None)
    if extra_flags.get("series_name"):
        series_name = extra_flags.pop("series_name")

    # Público
    raw_audience = raw_event.get("audience", "")
    audience = harmonize_audience(raw_audience)
    if extra_flags.get("is_family"):
        audience["is_family"] = True
    if extra_flags.get("is_educational"):
        audience["is_educational"] = True

    # Propagar flags residuais para pipeline.extra_flags
    # (is_online, is_outdoor, is_digital, geographic_scope, is_accessible)
    _CONSUMED_FLAGS = {"series_name", "is_family", "is_educational"}
    pipeline_extra_flags = {k: v for k, v in extra_flags.items()
                            if k not in _CONSUMED_FLAGS and v}

    # Preço
    price = parse_price(raw_event.get("price_raw", ""))
    if raw_event.get("ticketing_url"):
        price["ticketing_url"] = raw_event["ticketing_url"]

    # Status
    event_status = detect_event_status(title, description, len(harmonized_dates))

    # IDs e dedup
    event_id = generate_event_id(venue_id, title, date_first)
    fingerprint = generate_fingerprint(title, venue_id, date_first)

    # Acessibilidade (do raw, se disponível)
    acc_raw = raw_event.get("accessibility", {})
    accessibility = {
        "has_sign_language": acc_raw.get("has_sign_language", False),
        "has_audio_description": acc_raw.get("has_audio_description", False),
        "has_subtitles": acc_raw.get("has_subtitles", False),
        "subtitle_language": acc_raw.get("subtitle_language"),
        "is_relaxed_performance": acc_raw.get("is_relaxed_performance", False),
        "wheelchair_accessible": acc_raw.get("wheelchair_accessible", True),
        "notes": acc_raw.get("notes"),
    }
    # Detetar LGP na descrição
    if re.search(r"\blgp\b|língua gestual|lingua gestual", description, re.I):
        accessibility["has_sign_language"] = True
    if re.search(r"audiodescrição|audiodescricao|audio.?descri", description, re.I):
        accessibility["has_audio_description"] = True
    if re.search(r"legenda[sd]?", description, re.I):
        accessibility["has_subtitles"] = True
    if re.search(r"sessão relaxada|sessao relaxada|relaxed performance", description, re.I):
        accessibility["is_relaxed_performance"] = True

    return {
        "schema_version": "1.0",
        "id": event_id,
        "source_id": str(raw_event.get("source_id", "")),
        "source_url": raw_event.get("source_url", ""),
        "venue_id": venue_id,
        "space_id": raw_event.get("space_id"),
        "title": title,
        "title_raw": title_raw,
        "subtitle": raw_event.get("subtitle"),
        "description": description,
        "description_short": description_short,
        "language_of_performance": raw_event.get("language_of_performance"),
        "language_notes": raw_event.get("language_notes"),
        "domain": cat_result["domain"],
        "category": cat_result["category"],
        "subcategory": cat_result["subcategory"],
        "tags": raw_event.get("tags", []),
        "categories_raw": raw_cats,
        "dates": harmonized_dates,
        "date_first": date_first,
        "date_last": date_last,
        "total_sessions": len(harmonized_dates),
        "is_ongoing": raw_event.get("is_ongoing", False),
        "date_open": raw_event.get("date_open"),
        "date_close": raw_event.get("date_close"),
        "event_status": event_status,
        "is_premiere": event_status == "estreia",
        "is_national_premiere": "nacional" in (raw_event.get("description") or "").lower(),
        "is_reprise": event_status == "reposicao",
        "production_origin": raw_event.get("production_origin"),
        "is_festival": is_festival,
        "festival_name": festival_name,
        "festival_id": None,
        "is_multi_venue": False,
        "multi_venue_ids": [],
        "series_name": series_name,
        "series_edition": None,
        "audience": audience,
        "price": price,
        "accessibility": accessibility,
        "credits": raw_event.get("credits", {
            "company": None, "director": None, "conductor": None,
            "choreographer": None, "cast": [], "creative_team": [],
            "musicians": [], "credits_raw": raw_event.get("credits_raw"),
        }),
        "work": raw_event.get("work", {
            "original_title": None, "composer": None, "playwright": None,
            "choreographer": None, "year_created": None,
            "country_of_origin": None, "genre_original": None,
        }),
        "media": {
            "cover_image": raw_event.get("cover_image"),
            "cover_image_local": None,
            "thumbnail": raw_event.get("thumbnail"),
            "trailer_url": raw_event.get("trailer_url"),
            "gallery": raw_event.get("gallery", []),
        },
        "dedup": {
            "fingerprint": fingerprint,
            "duplicate_of": None,
            "is_canonical": True,
            "seen_at_venues": [venue_id],
        },
        "pipeline": {
            "scraped_at": raw_event.get("scraped_at", now),
            "scraper_id": scraper_id,
            "raw_data_hash": hash_raw(raw_event),
            "harmonized_at": now,
            "validated": False,
            "validation_errors": [],
            "manually_edited": False,
            "manually_edited_at": None,
            "is_active": True,
            "extra_flags": pipeline_extra_flags or None,
        },
    }
