#!/usr/bin/env python3
"""
Primeira Plateia — Relatório de Qualidade de Dados
Uso: python pipeline/scripts/quality_report.py [--venue VENUE_ID] [--top N] [--json]

Lê todos os data/events/*.json e imprime:
  1. Tabela por venue: contagem, % imagem, % preço, % descrição, score médio
  2. Top N eventos de pior score com id, título, score e primeiro erro
  3. Resumo global
"""

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

# Garantir que o módulo pipeline é encontrado independentemente de onde o script é corrido
_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_ROOT))

from pipeline.core.cache import credibility_score

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DATA_DIR   = _ROOT / "data"
EVENTS_DIR = DATA_DIR / "events"
LOGS_DIR   = DATA_DIR / "logs"


# ---------------------------------------------------------------------------
# Cores ANSI (desactivadas se a saída não é um terminal)
# ---------------------------------------------------------------------------
_USE_COLOR = sys.stdout.isatty()

def _c(text: str, code: str) -> str:
    if not _USE_COLOR:
        return text
    return f"\033[{code}m{text}\033[0m"

def _green(t):  return _c(t, "32")
def _yellow(t): return _c(t, "33")
def _red(t):    return _c(t, "31")
def _bold(t):   return _c(t, "1")
def _dim(t):    return _c(t, "2")


# ---------------------------------------------------------------------------
# Cálculos de qualidade
# ---------------------------------------------------------------------------

def _pct(count: int, total: int) -> float:
    return round(count / total * 100, 1) if total else 0.0


def _score_color(score: float) -> str:
    pct = int(score * 100)
    text = f"{pct:3d}%"
    if score >= 0.70:
        return _green(text)
    elif score >= 0.40:
        return _yellow(text)
    return _red(text)


def _pct_color(pct: float) -> str:
    text = f"{pct:5.1f}%"
    if pct >= 80:
        return _green(text)
    elif pct >= 40:
        return _yellow(text)
    return _red(text)


def _analyse_venue(venue_id: str, events: list[dict]) -> dict:
    """Calcula métricas de qualidade para um conjunto de eventos de um venue."""
    total = len(events)
    if total == 0:
        return {"venue_id": venue_id, "total": 0}

    now_str = date.today().isoformat()

    with_image   = sum(1 for e in events if (e.get("media") or {}).get("cover_image"))
    with_price   = sum(1 for e in events if
                       (e.get("price") or {}).get("price_display") or
                       (e.get("price") or {}).get("is_free"))
    with_desc    = sum(1 for e in events if e.get("description") and len(e["description"]) > 30)
    with_cat     = sum(1 for e in events if e.get("category") and e["category"] != "outros")
    with_dates   = sum(1 for e in events if e.get("dates"))
    with_ticket  = sum(1 for e in events if (e.get("price") or {}).get("ticketing_url"))
    active       = sum(1 for e in events if (e.get("pipeline") or {}).get("is_active", True))
    future       = sum(1 for e in events if (e.get("date_last") or e.get("date_first") or "") >= now_str)
    with_errors  = sum(1 for e in events if (e.get("pipeline") or {}).get("validation_errors"))
    scores       = [credibility_score(e) for e in events]
    avg_score    = round(sum(scores) / total, 3) if scores else 0.0

    return {
        "venue_id":    venue_id,
        "total":       total,
        "active":      active,
        "future":      future,
        "avg_score":   avg_score,
        "scores":      scores,
        "with_image":  _pct(with_image, total),
        "with_price":  _pct(with_price, total),
        "with_desc":   _pct(with_desc, total),
        "with_cat":    _pct(with_cat, total),
        "with_dates":  _pct(with_dates, total),
        "with_ticket": _pct(with_ticket, total),
        "with_errors": with_errors,
        "events":      events,
    }


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def _print_venue_table(analyses: list[dict]) -> None:
    """Imprime tabela de venues."""
    sep = "─" * 110
    header = (
        f"{'Venue':<22} {'Total':>6} {'Activo':>7} {'Futuro':>7} "
        f"{'Score':>6} {'Img':>7} {'Preço':>7} {'Desc':>7} {'Cat':>7} {'Ticket':>7} {'Erros':>6}"
    )
    print()
    print(_bold("━" * 110))
    print(_bold("  RELATÓRIO DE QUALIDADE — PRIMEIRA PLATEIA"))
    print(_bold("━" * 110))
    print(_dim(f"  Gerado em: {datetime.now().strftime('%Y-%m-%d %H:%M')}"))
    print()
    print(_bold(header))
    print(_dim(sep))

    for a in sorted(analyses, key=lambda x: x.get("avg_score", 0), reverse=True):
        if a["total"] == 0:
            continue
        score_str   = _score_color(a["avg_score"])
        img_str     = _pct_color(a["with_image"])
        price_str   = _pct_color(a["with_price"])
        desc_str    = _pct_color(a["with_desc"])
        cat_str     = _pct_color(a["with_cat"])
        ticket_str  = _pct_color(a["with_ticket"])
        errs        = a["with_errors"]
        errs_str    = _red(f"{errs:>6}") if errs > 0 else _dim(f"{'—':>6}")

        print(
            f"  {a['venue_id']:<20} {a['total']:>6} {a['active']:>7} {a['future']:>7} "
            f" {score_str}  {img_str}  {price_str}  {desc_str}  {cat_str}  {ticket_str} {errs_str}"
        )

    print(_dim(sep))


def _print_global_summary(analyses: list[dict]) -> None:
    """Imprime resumo global."""
    all_events = [e for a in analyses for e in a.get("events", [])]
    total = len(all_events)
    if total == 0:
        return

    now_str = date.today().isoformat()
    future  = sum(1 for e in all_events if (e.get("date_last") or e.get("date_first") or "") >= now_str)
    scores  = [credibility_score(e) for e in all_events]
    avg     = round(sum(scores) / total, 3) if scores else 0.0
    domains = {}
    for e in all_events:
        d = e.get("domain", "outros")
        domains[d] = domains.get(d, 0) + 1

    print()
    print(_bold("  RESUMO GLOBAL"))
    print(_dim("  " + "─" * 50))
    print(f"  Total de eventos:    {_bold(str(total))}")
    print(f"  Eventos futuros:     {future}")
    print(f"  Score médio global:  {_score_color(avg)}")
    print()
    print("  Por domínio:")
    for domain, count in sorted(domains.items(), key=lambda x: -x[1]):
        bar_len = int(count / total * 30)
        bar = "█" * bar_len + "░" * (30 - bar_len)
        pct = _pct(count, total)
        color_bar = _green(bar) if pct >= 40 else _yellow(bar) if pct >= 15 else _dim(bar)
        print(f"    {domain:<25} {color_bar}  {count:>4} ({pct:.1f}%)")
    print()


def _print_worst_events(analyses: list[dict], top_n: int = 10) -> None:
    """Imprime os N eventos com pior score de credibilidade."""
    all_events = [e for a in analyses for e in a.get("events", [])]
    if not all_events:
        return

    scored = [(credibility_score(e), e) for e in all_events]
    scored.sort(key=lambda x: x[0])
    worst = scored[:top_n]

    print(_bold(f"  TOP {top_n} EVENTOS COM PIOR SCORE DE CREDIBILIDADE"))
    print(_dim("  " + "─" * 80))
    for i, (score, e) in enumerate(worst, 1):
        eid     = e.get("id", "?")[:45]
        title   = (e.get("title") or "?")[:45]
        venue   = e.get("venue_id", "?")
        errors  = (e.get("pipeline") or {}).get("validation_errors", [])
        first_error = errors[0] if errors else "—"
        score_str = _score_color(score)
        print(f"  {i:>2}. {score_str}  {_bold(venue):<15} {title:<46}")
        print(f"       {_dim(eid)}")
        if first_error != "—":
            print(f"       {_red('⚠')} {_dim(first_error)}")
        print()


def _print_validation_summary(analyses: list[dict]) -> None:
    """Imprime resumo de erros de validação por tipo."""
    error_counts: dict[str, int] = {}
    for a in analyses:
        for e in a.get("events", []):
            for err in (e.get("pipeline") or {}).get("validation_errors", []):
                # Normalizar: remover valores específicos para agrupar
                key = err.split(":")[0].strip() if ":" in err else err
                error_counts[key] = error_counts.get(key, 0) + 1

    if not error_counts:
        print("  " + _green("✓ Sem erros de validação."))
        return

    print(_bold("  ERROS DE VALIDAÇÃO (agrupados)"))
    print(_dim("  " + "─" * 60))
    for err, count in sorted(error_counts.items(), key=lambda x: -x[1]):
        badge = _red(f"  ✗ {count:>4}x")
        print(f"{badge}  {err}")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Primeira Plateia — Relatório de Qualidade de Dados"
    )
    parser.add_argument(
        "--venue", "-v",
        help="Filtrar por venue_id (ex: ccb, culturgest)",
        default=None,
    )
    parser.add_argument(
        "--top", "-t",
        type=int,
        default=10,
        help="Número de eventos piores a mostrar (default: 10)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output em JSON em vez de texto",
    )
    parser.add_argument(
        "--no-worst",
        action="store_true",
        help="Omitir a lista de piores eventos",
    )
    args = parser.parse_args()

    # Carregar ficheiros de eventos
    event_files = sorted(EVENTS_DIR.glob("*.json"))
    if not event_files:
        print(f"Erro: sem ficheiros em {EVENTS_DIR}", file=sys.stderr)
        sys.exit(1)

    if args.venue:
        event_files = [f for f in event_files if f.stem == args.venue]
        if not event_files:
            print(f"Erro: venue '{args.venue}' não encontrado", file=sys.stderr)
            sys.exit(1)

    analyses = []
    for path in event_files:
        if path.name == ".gitkeep":
            continue
        try:
            with open(path, encoding="utf-8") as f:
                events = json.load(f)
            analyses.append(_analyse_venue(path.stem, events))
        except Exception as e:
            print(f"Aviso: erro ao ler {path.name}: {e}", file=sys.stderr)

    if not analyses:
        print("Sem dados para analisar.", file=sys.stderr)
        sys.exit(1)

    # Output JSON
    if args.json:
        output = []
        for a in analyses:
            a_out = {k: v for k, v in a.items() if k != "events"}
            output.append(a_out)
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return

    # Output texto
    _print_venue_table(analyses)
    _print_global_summary(analyses)

    print(_bold("  ERROS DE VALIDAÇÃO"))
    _print_validation_summary(analyses)

    if not args.no_worst:
        _print_worst_events(analyses, top_n=args.top)

    print(_bold("━" * 110))
    print()


if __name__ == "__main__":
    main()
