"""
Primeira Plateia — Validador
Valida eventos harmonizados contra o schema e regras de negócio.
"""

import re
import logging
from datetime import datetime, date
from typing import Optional

logger = logging.getLogger(__name__)

REQUIRED_FIELDS = [
    "id", "source_id", "source_url", "venue_id",
    "title", "domain", "category", "dates", "pipeline"
]

VALID_DOMAINS = [
    "musica", "artes-palco", "artes-visuais",
    "pensamento", "cinema", "formacao", "outros"
]

VALID_STATUSES = [
    "estreia", "reposicao", "ultima-sessao", "unica-sessao", "em-cartaz"
]

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
TIME_RE = re.compile(r"^\d{2}:\d{2}$")


def validate_event(event: dict) -> tuple[bool, list[str]]:
    """
    Valida evento harmonizado.
    Retorna (is_valid, lista_de_erros).
    """
    errors = []

    # Campos obrigatórios
    for field in REQUIRED_FIELDS:
        if field not in event or event[field] is None:
            errors.append(f"Campo obrigatório ausente: {field}")

    # Título
    title = event.get("title", "")
    if not title or len(title.strip()) < 2:
        errors.append("Título inválido ou demasiado curto")
    if len(title) > 500:
        errors.append(f"Título demasiado longo ({len(title)} chars)")

    # Domínio
    domain = event.get("domain", "")
    if domain not in VALID_DOMAINS:
        errors.append(f"Domínio inválido: '{domain}'")

    # Status
    status = event.get("event_status", "")
    if status and status not in VALID_STATUSES:
        errors.append(f"Status inválido: '{status}'")

    # Datas
    dates = event.get("dates", [])
    if not dates:
        errors.append("Evento sem datas")
    else:
        for i, d in enumerate(dates):
            date_str = d.get("date", "")
            if not date_str or not DATE_RE.match(str(date_str)):
                errors.append(f"Data inválida na sessão {i}: '{date_str}'")
            time_str = d.get("time_start")
            if time_str and not TIME_RE.match(str(time_str)):
                errors.append(f"Hora inválida na sessão {i}: '{time_str}'")

    # Preço
    price = event.get("price", {})
    if price:
        p_min = price.get("price_min")
        p_max = price.get("price_max")
        if p_min is not None and p_max is not None:
            if p_min > p_max:
                errors.append(f"Preço mínimo ({p_min}) maior que máximo ({p_max})")
        if not price.get("is_free") and p_min is None and price.get("price_display") is None:
            pass  # Preço desconhecido é aceitável

    # Público — idade
    audience = event.get("audience", {})
    age_min = audience.get("age_min")
    age_max = audience.get("age_max")
    if age_min is not None and age_max is not None:
        if age_min > age_max:
            errors.append(f"Idade mínima ({age_min}) maior que máxima ({age_max})")

    # URL
    source_url = event.get("source_url", "")
    if source_url and not source_url.startswith("http"):
        errors.append(f"source_url inválida: '{source_url}'")

    # ID
    event_id = event.get("id", "")
    if event_id and not re.match(r"^[a-z0-9-]+$", event_id):
        errors.append(f"ID com caracteres inválidos: '{event_id}'")

    is_valid = len(errors) == 0
    return is_valid, errors


def validate_and_annotate(event: dict) -> dict:
    """Valida evento e anota os resultados no campo pipeline."""
    is_valid, errors = validate_event(event)
    event["pipeline"]["validated"] = is_valid
    event["pipeline"]["validation_errors"] = errors
    if not is_valid:
        logger.warning(f"Evento inválido [{event.get('id')}]: {errors}")
    return event


def validate_batch(events: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Valida lista de eventos.
    Retorna (válidos, inválidos).
    """
    valid = []
    invalid = []
    for event in events:
        annotated = validate_and_annotate(event)
        if annotated["pipeline"]["validated"]:
            valid.append(annotated)
        else:
            invalid.append(annotated)

    total = len(events)
    logger.info(f"Validação: {len(valid)}/{total} válidos, {len(invalid)} com erros")
    return valid, invalid
