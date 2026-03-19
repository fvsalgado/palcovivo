"""
Validador de eventos Palco Vivo.
Corre após scraping e antes de escrever o events.json.
Produz um validation_report.json com estatísticas e erros.
"""
import re
from datetime import date, datetime
from scrapers.utils import log

VALID_THEATERS = {
    "Teatro Variedades",
    "Capitólio",
    "Teatro Variedades & Capitólio",   # compatibilidade legada
    "São Luiz Teatro Municipal",
    "Teatro Nacional São João",
    "Teatro Tivoli BBVA",
    "Teatro Maria Matos",
    "CCB — Centro Cultural de Belém",
}

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def validate(events: list[dict]) -> tuple[list[dict], dict]:
    """
    Recebe lista raw de eventos, devolve (eventos_válidos, relatório).
    """
    today = date.today().isoformat()
    accepted = []
    rejected = []
    warnings = []

    for ev in events:
        errors = []
        warns = []

        # Campos obrigatórios
        if not ev.get("title") or len(ev["title"].strip()) < 3:
            errors.append("title ausente ou demasiado curto")

        ds = ev.get("date_start", "")
        de = ev.get("date_end", "")

        if not ds:
            errors.append("date_start ausente")
        elif not DATE_RE.match(ds):
            errors.append(f"date_start formato inválido: {ds!r}")
        else:
            # Avisar (não rejeitar) se evento já terminou
            end = de if (de and DATE_RE.match(de)) else ds
            if end < today:
                warns.append(f"evento já terminou ({end})")

        if de and DATE_RE.match(de) and ds and DATE_RE.match(ds):
            if de < ds:
                warns.append(f"date_end ({de}) < date_start ({ds}) — corrigido")
                ev["date_end"] = ds

        if ev.get("image") and not ev["image"].startswith("http"):
            warns.append("image URL inválida — limpa")
            ev["image"] = ""

        if ev.get("url") and not ev["url"].startswith("http"):
            warns.append("url inválida")

        theater = ev.get("theater", "")
        if theater not in VALID_THEATERS:
            warns.append(f"theater desconhecido: {theater!r}")

        if errors:
            rejected.append({"id": ev.get("id", "?"), "title": ev.get("title", "?"), "errors": errors})
        else:
            accepted.append(ev)
            if warns:
                warnings.append({"id": ev.get("id", "?"), "title": ev.get("title", "?"), "warnings": warns})

    report = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "total_raw":     len(events),
        "total_accepted": len(accepted),
        "total_rejected": len(rejected),
        "total_warnings": len(warnings),
        "rejected":      rejected,
        "warnings":      warnings,
    }

    log(f"[Validação] {len(accepted)} aceites | {len(rejected)} rejeitados | {len(warnings)} avisos")
    for r in rejected:
        log(f"  REJEITADO {r['id']}: {r['errors']}")

    return accepted, report
