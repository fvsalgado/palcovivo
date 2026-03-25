"""
Primeira Plateia — Notificações
Envia relatório diário por email HTML (Gmail SMTP) e Ntfy.
"""

import json
import os
import smtplib
import logging
import requests
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CONFIG (via env vars / GitHub Secrets)
# ---------------------------------------------------------------------------
GMAIL_USER = os.environ.get("GMAIL_USER", "")           # fabio@...
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")
NOTIFY_TO = os.environ.get("NOTIFY_EMAIL", "fabio@primeiraplateia.pt")
NTFY_URL = os.environ.get("NTFY_URL", "")               # ex: https://ntfy.sh/primeira-plateia-xyz
SITE_URL = "https://primeiraplateia.pt"


# ---------------------------------------------------------------------------
# EMAIL HTML
# ---------------------------------------------------------------------------

def build_email_html(report: dict) -> str:
    """Gera o corpo HTML do email de relatório diário."""
    s = report.get("summary", {})
    venues = report.get("venues", [])
    run_at = report.get("run_at", "")
    duration = report.get("duration_seconds", 0)

    # Formatar data
    try:
        dt = datetime.fromisoformat(run_at.replace("Z", "+00:00"))
        date_str = dt.strftime("%d de %B de %Y, %H:%M UTC")
    except Exception:
        date_str = run_at

    # Cor do status global
    has_errors = s.get("total_errors", 0) > 0
    status_color = "#E63946" if has_errors else "#2A9D8F"
    status_label = "⚠️ Com erros" if has_errors else "✅ Sem erros"

    # Linhas de venues
    venue_quality = report.get("venue_quality", {})
    venue_rows = ""
    for v in venues:
        vid = v.get("venue_id", "")
        err_badge = ""
        if v.get("errors"):
            n_err = len(v["errors"])
            err_badge = f'<span style="color:#E63946;font-size:11px;">⚠️ {n_err} erro(s)</span>'
        elif v.get("cache_hit"):
            err_badge = '<span style="color:#888;font-size:11px;">📦 cache</span>'
        vq = venue_quality.get(vid, {})
        score = vq.get("avg_credibility", 0)
        if score:
            sc = "#2A9D8F" if score >= 0.7 else "#E9C46A" if score >= 0.5 else "#E63946"
            score_cell = f'<span style="color:{sc};font-weight:600;">{int(score*100)}%</span>'
        else:
            score_cell = "—"
        venue_rows += (
            "<tr>"
            f'<td style="padding:8px 12px;border-bottom:1px solid #f0f0f0;font-weight:600;">{v["venue_name"]}</td>'
            f'<td style="padding:8px 12px;border-bottom:1px solid #f0f0f0;text-align:center;">{v["scraped"]}</td>'
            f'<td style="padding:8px 12px;border-bottom:1px solid #f0f0f0;text-align:center;color:#2A9D8F;">{v["valid"]}</td>'
            f'<td style="padding:8px 12px;border-bottom:1px solid #f0f0f0;text-align:center;color:#E63946;">{v["invalid"]}</td>'
            f'<td style="padding:8px 12px;border-bottom:1px solid #f0f0f0;text-align:center;">{score_cell}</td>'
            f'<td style="padding:8px 12px;border-bottom:1px solid #f0f0f0;">{err_badge}</td>'
            "</tr>"
        )

    # Erros detalhados
    error_section = ""
    all_errors = [(v["venue_name"], e) for v in venues for e in v.get("errors", [])]
    if all_errors:
        error_items = "".join(
            f'<li style="margin-bottom:4px;"><strong>{vname}:</strong> {err}</li>'
            for vname, err in all_errors
        )
        error_section = f"""
        <div style="margin-top:24px;background:#fff5f5;border-left:4px solid #E63946;padding:16px;border-radius:4px;">
          <strong style="color:#E63946;">Erros Detectados</strong>
          <ul style="margin:8px 0 0 0;padding-left:20px;color:#555;">{error_items}</ul>
        </div>"""

    return f"""<!DOCTYPE html>
<html lang="pt">
<head><meta charset="UTF-8"><title>Primeira Plateia — Relatório Diário</title></head>
<body style="margin:0;padding:0;background:#f5f5f5;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
  <div style="max-width:600px;margin:32px auto;background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.08);">

    <!-- Header -->
    <div style="background:#1a1a2e;padding:24px 32px;">
      <div style="font-size:22px;font-weight:700;color:#fff;letter-spacing:-0.5px;">Primeira Plateia</div>
      <div style="font-size:13px;color:#aaa;margin-top:4px;">Relatório do Pipeline Diário</div>
    </div>

    <!-- Status bar -->
    <div style="background:{status_color};padding:12px 32px;color:#fff;font-weight:600;font-size:14px;">
      {status_label} &nbsp;·&nbsp; {date_str} &nbsp;·&nbsp; {duration}s
    </div>

    <!-- Summary cards -->
    <div style="display:flex;gap:0;border-bottom:1px solid #eee;">
      <div style="flex:1;padding:20px 24px;text-align:center;border-right:1px solid #eee;">
        <div style="font-size:32px;font-weight:700;color:#1a1a2e;">{s.get('total_after_dedup', 0)}</div>
        <div style="font-size:12px;color:#888;margin-top:4px;text-transform:uppercase;letter-spacing:.5px;">Eventos únicos</div>
      </div>
      <div style="flex:1;padding:20px 24px;text-align:center;border-right:1px solid #eee;">
        <div style="font-size:32px;font-weight:700;color:#2A9D8F;">{s.get('total_valid', 0)}</div>
        <div style="font-size:12px;color:#888;margin-top:4px;text-transform:uppercase;letter-spacing:.5px;">Válidos</div>
      </div>
      <div style="flex:1;padding:20px 24px;text-align:center;border-right:1px solid #eee;">
        <div style="font-size:32px;font-weight:700;color:#E63946;">{s.get('total_invalid', 0)}</div>
        <div style="font-size:12px;color:#888;margin-top:4px;text-transform:uppercase;letter-spacing:.5px;">Inválidos</div>
      </div>
      <div style="flex:1;padding:20px 24px;text-align:center;border-right:1px solid #eee;">
        <div style="font-size:32px;font-weight:700;color:#457B9D;">{s.get('venues_processed', 0)}</div>
        <div style="font-size:12px;color:#888;margin-top:4px;text-transform:uppercase;letter-spacing:.5px;">Venues</div>
      </div>
      <div style="flex:1;padding:20px 24px;text-align:center;">
        <div style="font-size:32px;font-weight:700;color:#6C63FF;">{int(s.get('avg_credibility', 0)*100)}%</div>
        <div style="font-size:12px;color:#888;margin-top:4px;text-transform:uppercase;letter-spacing:.5px;">Credibilidade</div>
      </div>
    </div>

    <!-- Venues table -->
    <div style="padding:24px 32px;">
      <h3 style="margin:0 0 16px 0;font-size:15px;color:#1a1a2e;">Detalhe por Venue</h3>
      <table style="width:100%;border-collapse:collapse;font-size:13px;">
        <thead>
          <tr style="background:#f8f8f8;">
            <th style="padding:8px 12px;text-align:left;color:#666;font-weight:600;border-bottom:2px solid #eee;">Venue</th>
            <th style="padding:8px 12px;text-align:center;color:#666;font-weight:600;border-bottom:2px solid #eee;">Scraped</th>
            <th style="padding:8px 12px;text-align:center;color:#666;font-weight:600;border-bottom:2px solid #eee;">Válidos</th>
            <th style="padding:8px 12px;text-align:center;color:#666;font-weight:600;border-bottom:2px solid #eee;">Inválidos</th>
            <th style="padding:8px 12px;text-align:center;color:#666;font-weight:600;border-bottom:2px solid #eee;">Score</th>
            <th style="padding:8px 12px;text-align:left;color:#666;font-weight:600;border-bottom:2px solid #eee;">Estado</th>
          </tr>
        </thead>
        <tbody>{venue_rows}</tbody>
      </table>
    </div>

    {error_section}

    <!-- Footer -->
    <div style="padding:20px 32px;border-top:1px solid #eee;display:flex;justify-content:space-between;align-items:center;">
      <div style="font-size:12px;color:#aaa;">
        <a href="{SITE_URL}" style="color:#457B9D;text-decoration:none;">primeiraplateia.pt</a>
      </div>
      <div style="font-size:11px;color:#ccc;">Pipeline automático · GitHub Actions</div>
    </div>

  </div>
</body>
</html>"""


def send_email(report: dict) -> bool:
    """Envia relatório por email via Gmail SMTP."""
    if not GMAIL_USER or not GMAIL_APP_PASSWORD:
        logger.warning("Email: credenciais Gmail não configuradas (GMAIL_USER / GMAIL_APP_PASSWORD)")
        return False

    s = report.get("summary", {})
    has_errors = s.get("total_errors", 0) > 0
    subject_prefix = "⚠️" if has_errors else "✅"
    subject = (
        f"{subject_prefix} Primeira Plateia — "
        f"{s.get('total_after_dedup', 0)} eventos · "
        f"{s.get('venues_processed', 0)} venues · "
        f"{datetime.now(timezone.utc).strftime('%d/%m/%Y')}"
    )

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"Primeira Plateia Pipeline <{GMAIL_USER}>"
    msg["To"] = NOTIFY_TO

    # Plain text fallback
    plain = (
        f"Primeira Plateia — Relatório Diário\n"
        f"Venues: {s.get('venues_processed', 0)}\n"
        f"Scraped: {s.get('total_scraped', 0)}\n"
        f"Válidos: {s.get('total_valid', 0)}\n"
        f"Após dedup: {s.get('total_after_dedup', 0)}\n"
        f"Erros: {s.get('total_errors', 0)}\n"
    )
    msg.attach(MIMEText(plain, "plain"))
    msg.attach(MIMEText(build_email_html(report), "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
            server.sendmail(GMAIL_USER, NOTIFY_TO, msg.as_string())
        logger.info(f"Email enviado para {NOTIFY_TO}")
        return True
    except smtplib.SMTPException as e:
        logger.error(f"Erro ao enviar email: {e}")
        return False


# ---------------------------------------------------------------------------
# NTFY
# ---------------------------------------------------------------------------

def send_ntfy(report: dict) -> bool:
    """Envia notificação push via Ntfy."""
    if not NTFY_URL:
        logger.warning("Ntfy: NTFY_URL não configurado")
        return False

    s = report.get("summary", {})
    has_errors = s.get("total_errors", 0) > 0

    title = "Primeira Plateia - OK" if not has_errors else "Primeira Plateia - ERRO"
    message = (
        f"{s.get('total_after_dedup', 0)} eventos · "
        f"{s.get('venues_processed', 0)} venues · "
        f"{s.get('total_errors', 0)} erros"
    )

    try:
        resp = requests.post(
            NTFY_URL,
            data=message.encode("utf-8"),
            headers={
                "Title": title,
                "Priority": "high" if has_errors else "default",
                "Tags": "warning" if has_errors else "white_check_mark",
                "Click": SITE_URL,
            },
            timeout=10,
        )
        resp.raise_for_status()
        logger.info("Ntfy: notificação enviada")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Ntfy: erro — {e}")
        return False


# ---------------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------------

def notify(report: dict) -> None:
    """Envia todas as notificações configuradas."""
    send_email(report)
    send_ntfy(report)


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    logs_dir = Path(__file__).parent.parent.parent / "data" / "logs"
    latest = logs_dir / "latest.json"
    if latest.exists():
        with open(latest) as f:
            report = json.load(f)
        notify(report)
    else:
        # Sem relatório — pipeline pode ter falhado antes de o escrever
        # Não falhar o job por isso (exit 0)
        print("Aviso: sem relatório latest.json — pipeline pode não ter concluído")
        sys.exit(0)
