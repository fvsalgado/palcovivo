"""
Primeira Plateia — Validador v1.1
Valida eventos harmonizados contra o schema e regras de negócio.

Novidades v1.1:
  - quality_score() — score 0.0-1.0 por evento
  - field_quality_report() — percentagem de preenchimento por campo, por venue
  - alertas granulares por campo em falta (WARNING, nao erro fatal)
  - validate_event() distingue erros estruturais (bloqueantes) de
    alertas de qualidade (informativos)
"""

import re
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)

REQUIRED_FIELDS = [
    "id", "source_id", "source_url", "venue_id",
    "title", "domain", "category", "dates", "pipeline",
]

VALID_DOMAINS = [
    "musica", "artes-palco", "artes-visuais",
    "pensamento", "cinema", "formacao", "outros",
]

VALID_STATUSES = [
    "estreia", "reposicao", "ultima-sessao", "unica-sessao", "em-cartaz",
]

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
TIME_RE = re.compile(r"^\d{2}:\d{2}$")

# ---------------------------------------------------------------------------
# CAMPOS DE QUALIDADE — peso no score (soma = 1.0)
# Cada entrada: (nome, peso, funcao_de_check)
# ---------------------------------------------------------------------------
QUALITY_FIELDS: list[tuple[str, float, object]] = [
    ("description",       0.15, lambda e: bool(e.get("description") and len(e.get("description", "")) > 30)),
    ("cover_image",       0.10, lambda e: bool((e.get("media") or {}).get("cover_image"))),
    ("domain_not_outros", 0.10, lambda e: e.get("domain") != "outros"),
    ("subcategory",       0.05, lambda e: bool(e.get("subcategory"))),
    ("tags",              0.05, lambda e: bool(e.get("tags"))),
    ("price_display",     0.10, lambda e: bool(
        (e.get("price") or {}).get("price_display") or
        (e.get("price") or {}).get("is_free")
    )),
    ("ticketing_url",     0.05, lambda e: bool((e.get("price") or {}).get("ticketing_url"))),
    ("credits_any",       0.10, lambda e: bool(
        (e.get("credits") or {}).get("director") or
        (e.get("credits") or {}).get("company") or
        (e.get("credits") or {}).get("cast") or
        (e.get("credits") or {}).get("credits_raw")
    )),
    ("audience_age",      0.05, lambda e: (e.get("audience") or {}).get("age_min") is not None),
    ("duration_minutes",  0.05, lambda e: bool(
        e.get("duration_minutes") or
        any(d.get("duration_minutes") for d in (e.get("dates") or []))
    )),
    ("accessibility",     0.10, lambda e: bool(
        (e.get("accessibility") or {}).get("wheelchair_accessible") is not None or
        (e.get("accessibility") or {}).get("notes")
    )),
    ("source_url_valid",  0.10, lambda e: bool(e.get("source_url", "").startswith("http"))),
]

# ---------------------------------------------------------------------------
# ALERTAS DE QUALIDADE — nao bloqueantes, informativos
# ---------------------------------------------------------------------------
QUALITY_ALERTS: list[tuple[str, object, str]] = [
    ("description",
     lambda e: bool(e.get("description") and len(e.get("description", "")) > 30),
     "descricao ausente ou demasiado curta (< 30 chars)"),
    ("cover_image",
     lambda e: bool((e.get("media") or {}).get("cover_image")),
     "imagem de capa ausente"),
    ("price",
     lambda e: bool(
         (e.get("price") or {}).get("price_display") or
         (e.get("price") or {}).get("is_free")
     ),
     "preco nao determinado (nem entrada livre nem valor)"),
    ("ticketing_url",
     lambda e: bool((e.get("price") or {}).get("ticketing_url")),
     "url de bilheteira ausente"),
    ("credits",
     lambda e: bool(
         (e.get("credits") or {}).get("director") or
         (e.get("credits") or {}).get("company") or
         (e.get("credits") or {}).get("credits_raw")
     ),
     "creditos ausentes (director, companhia ou texto raw)"),
    ("domain_outros",
     lambda e: e.get("domain") != "outros",
     "evento em dominio outros — verificar categories_raw"),
    ("subcategory",
     lambda e: bool(e.get("subcategory")),
     "subcategoria nao determinada"),
    ("tags",
     lambda e: bool(e.get("tags")),
     "tags ausentes"),
    ("audience_age",
     lambda e: (e.get("audience") or {}).get("age_min") is not None,
     "faixa etaria nao determinada"),
    ("duration",
     lambda e: bool(
         e.get("duration_minutes") or
         any(d.get("duration_minutes") for d in (e.get("dates") or []))
     ),
     "duration_minutes nao preenchida"),
]


# ---------------------------------------------------------------------------
# VALIDACAO ESTRUTURAL
# ---------------------------------------------------------------------------

def validate_event(event: dict) -> tuple[bool, list[str], list[str]]:
    """
    Valida evento harmonizado.

    Retorna:
        (is_valid, erros_bloqueantes, alertas_qualidade)

    Erros bloqueantes: impedem o evento de entrar no master.json.
    Alertas de qualidade: informativos, evento e aceite mas marcado.
    """
    errors: list[str] = []
    warnings: list[str] = []

    for field in REQUIRED_FIELDS:
        if field not in event or event[field] is None:
            errors.append(f"campo obrigatorio ausente: {field}")

    title = event.get("title", "")
    if not title or len(title.strip()) < 2:
        errors.append("titulo invalido ou demasiado curto")
    elif len(title) > 500:
        errors.append(f"titulo demasiado longo ({len(title)} chars)")

    domain = event.get("domain", "")
    if domain not in VALID_DOMAINS:
        errors.append(f"dominio invalido: '{domain}'")

    status = event.get("event_status", "")
    if status and status not in VALID_STATUSES:
        errors.append(f"status invalido: '{status}'")

    dates = event.get("dates", [])
    if not dates:
        errors.append("evento sem datas")
    else:
        for i, d in enumerate(dates):
            date_str = d.get("date", "")
            if not date_str or not DATE_RE.match(str(date_str)):
                errors.append(f"data invalida na sessao {i}: '{date_str}'")
            time_str = d.get("time_start")
            if time_str and not TIME_RE.match(str(time_str)):
                errors.append(f"hora invalida na sessao {i}: '{time_str}'")

    price = event.get("price") or {}
    p_min = price.get("price_min")
    p_max = price.get("price_max")
    if p_min is not None and p_max is not None and p_min > p_max:
        errors.append(f"price_min ({p_min}) > price_max ({p_max})")

    audience = event.get("audience") or {}
    age_min = audience.get("age_min")
    age_max = audience.get("age_max")
    if age_min is not None and age_max is not None and age_min > age_max:
        errors.append(f"age_min ({age_min}) > age_max ({age_max})")

    source_url = event.get("source_url", "")
    if source_url and not source_url.startswith("http"):
        errors.append(f"source_url invalida: '{source_url[:80]}'")

    event_id = event.get("id", "")
    if event_id and not re.match(r"^[a-z0-9-]+$", event_id):
        errors.append(f"id com caracteres invalidos: '{event_id[:60]}'")

    date_first = event.get("date_first")
    date_last  = event.get("date_last")
    if date_first and date_last and date_first > date_last:
        errors.append(f"date_first ({date_first}) > date_last ({date_last})")

    total_sessions = len(event.get("dates") or [])
    if total_sessions > 500:
        errors.append(f"total_sessions improvavel ({total_sessions}) — possivel bug no scraper")

    for _field, check_fn, msg in QUALITY_ALERTS:
        if not check_fn(event):
            warnings.append(msg)

    return len(errors) == 0, errors, warnings


# ---------------------------------------------------------------------------
# SCORE DE QUALIDADE
# ---------------------------------------------------------------------------

def quality_score(event: dict) -> float:
    """Score de qualidade 0.0-1.0 baseado no preenchimento dos campos."""
    score = 0.0
    for _name, weight, check_fn in QUALITY_FIELDS:
        if check_fn(event):
            score += weight
    return round(min(1.0, score), 3)


# ---------------------------------------------------------------------------
# ANOTACAO E BATCH
# ---------------------------------------------------------------------------

def validate_and_annotate(event: dict) -> dict:
    """
    Valida evento e anota resultados em pipeline:
      pipeline.validated, pipeline.validation_errors,
      pipeline.quality_warnings, pipeline.quality_score
    """
    is_valid, errors, warnings = validate_event(event)

    pipeline = event.get("pipeline", {})
    pipeline["validated"]         = is_valid
    pipeline["validation_errors"] = errors
    pipeline["quality_warnings"]  = warnings
    pipeline["quality_score"]     = quality_score(event)
    event["pipeline"] = pipeline

    if errors:
        logger.warning(f"Evento invalido [{event.get('id')}]: {errors}")
    elif warnings:
        logger.debug(f"Alertas [{event.get('id')}]: {len(warnings)} campo(s)")

    return event


def validate_batch(events: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Valida lista de eventos. Retorna (validos, invalidos).
    Validos podem ter quality_warnings.
    """
    valid, invalid = [], []
    for event in events:
        annotated = validate_and_annotate(event)
        if annotated["pipeline"]["validated"]:
            valid.append(annotated)
        else:
            invalid.append(annotated)

    total = len(events)
    n_warn = sum(1 for e in valid if e["pipeline"].get("quality_warnings"))
    logger.info(
        f"Validacao: {len(valid)}/{total} validos "
        f"({n_warn} com alertas), {len(invalid)} com erros estruturais"
    )
    return valid, invalid


# ---------------------------------------------------------------------------
# RELATORIO DE QUALIDADE POR VENUE
# ---------------------------------------------------------------------------

def field_quality_report(events: list[dict]) -> dict:
    """
    Produz relatorio de preenchimento por campo e por venue.

    Retorna:
    {
        "total": int,
        "by_venue": {
            "venue_id": {
                "total": int,
                "avg_score": float,
                "fields": {
                    "field_name": {"filled": int, "missing": int, "pct": float}
                }
            }
        },
        "global": {"avg_score": float, "fields": {...}}
    }
    """
    venues: dict[str, list] = defaultdict(list)
    for e in events:
        venues[e.get("venue_id", "unknown")].append(e)

    def _stats(evs):
        stats = {}
        for name, _weight, check_fn in QUALITY_FIELDS:
            filled  = sum(1 for e in evs if check_fn(e))
            missing = len(evs) - filled
            stats[name] = {
                "filled":  filled,
                "missing": missing,
                "pct":     round(filled / len(evs) * 100, 1) if evs else 0.0,
            }
        return stats

    report: dict = {"total": len(events), "by_venue": {}, "global": {}}

    for venue_id, evs in sorted(venues.items()):
        avg = round(sum(quality_score(e) for e in evs) / len(evs), 3) if evs else 0.0
        report["by_venue"][venue_id] = {
            "total":     len(evs),
            "avg_score": avg,
            "fields":    _stats(evs),
        }

    avg_g = round(sum(quality_score(e) for e in events) / len(events), 3) if events else 0.0
    report["global"] = {"avg_score": avg_g, "fields": _stats(events)}

    return report


def print_quality_report(report: dict) -> None:
    """Imprime relatorio de qualidade em formato legivel para logs/terminal."""
    sep = "=" * 70
    print(f"\n{sep}")
    print(f"  RELATORIO DE QUALIDADE — {report['total']} eventos")
    print(f"{sep}")
    print(f"  Score medio global: {report['global']['avg_score']:.3f}\n")

    for venue_id, vdata in report["by_venue"].items():
        print(f"  — {venue_id} ({vdata['total']} eventos | score={vdata['avg_score']:.3f})")
        for field, fdata in vdata["fields"].items():
            icon = "+" if fdata["pct"] >= 80 else ("!" if fdata["pct"] >= 40 else "X")
            gap  = f"  [em falta: {fdata['missing']}]" if fdata["missing"] else ""
            print(f"    [{icon}] {field:<25} {fdata['pct']:5.1f}%{gap}")
        print()

    critical = [
        (n, d) for n, d in report["global"]["fields"].items() if d["pct"] < 40
    ]
    if critical:
        print("  — Campos criticos (< 40% global):")
        for name, data in sorted(critical, key=lambda x: x[1]["pct"]):
            print(f"    [X] {name:<25} {data['pct']:5.1f}%  (em falta: {data['missing']})")
        print()

    print(f"{sep}\n")
