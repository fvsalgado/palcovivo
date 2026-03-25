"""
Primeira Plateia — Build Index
Reconstrói o index.html injectando os dados actualizados do master.json.
Invocado pelo pipeline após cada run.
"""
import json
import html as html_mod
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
INDEX_PATH = ROOT / "index.html"
MASTER_PATH = ROOT / "data" / "master.json"


def build_slim_events(master: dict) -> list[dict]:
    events = master.get("events", [])

    def clean(s):
        if not isinstance(s, str): return s
        return html_mod.unescape(s)

    slim = []
    for e in events:
        dates = e.get("dates", [])
        slim.append({
            "id":         e.get("id", ""),
            "title":      clean(e.get("title", "")),
            "subtitle":   clean(e.get("subtitle")) if e.get("subtitle") else None,
            "desc":       clean((e.get("description") or "")[:200]),
            "domain":     e.get("domain", "outros"),
            "cat":        e.get("category", "outros"),
            "venue_id":   e.get("venue_id", ""),
            "date_first": e.get("date_first", ""),
            "date_last":  e.get("date_last", ""),
            "time":       dates[0].get("time_start") if dates else None,
            "sessions":   len(dates) or 1,
            "ongoing":    e.get("is_ongoing", False),
            "status":     e.get("event_status", "unica-sessao"),
            "premiere":   e.get("event_status") == "estreia",
            "family":     e.get("audience", {}).get("is_family", False)
                         if isinstance(e.get("audience"), dict) else False,
            "free":       e.get("price", {}).get("is_free", False)
                         if isinstance(e.get("price"), dict) else False,
            "img":        e.get("media", {}).get("cover_image")
                         if isinstance(e.get("media"), dict) else None,
            "url":        e.get("source_url", ""),
            "tags":       e.get("tags", []),
            "a11y":       e.get("accessibility", {}).get("has_sign_language", False)
                         if isinstance(e.get("accessibility"), dict) else False,
            "origin":     None,
            "all_dates":  [d.get("date") for d in dates if d.get("date")],
        })
    return slim


def main():
    if not MASTER_PATH.exists():
        print(f"ERRO: {MASTER_PATH} não encontrado", file=sys.stderr)
        sys.exit(1)

    with open(MASTER_PATH, encoding="utf-8") as f:
        master = json.load(f)

    slim_events = build_slim_events(master)
    print(f"Eventos: {len(slim_events)}")

    # Serializar como JSON puro (ensure_ascii=False para manter UTF-8)
    events_json = json.dumps(slim_events, ensure_ascii=False, separators=(",", ":"))

    with open(INDEX_PATH, encoding="utf-8") as f:
        content = f.read()

    # Substituir o bloco <script type="application/json" id="events-data">
    OPEN  = '<script type="application/json" id="events-data">'
    CLOSE = "</script>"
    start = content.find(OPEN)
    if start == -1:
        # Inserir antes do <script> principal
        script_pos = content.find('<script>\n// ===')
        if script_pos == -1:
            print("ERRO: não encontrei ponto de injecção no index.html", file=sys.stderr)
            sys.exit(1)
        data_block = f'{OPEN}\n{events_json}\n{CLOSE}\n\n'
        content = content[:script_pos] + data_block + content[script_pos:]
    else:
        end = content.find(CLOSE, start + len(OPEN))
        if end == -1:
            print("ERRO: tag de fecho não encontrada", file=sys.stderr)
            sys.exit(1)
        content = content[:start + len(OPEN)] + f"\n{events_json}\n" + content[end:]

    with open(INDEX_PATH, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"index.html actualizado — {len(slim_events)} eventos injectados")


if __name__ == "__main__":
    main()
