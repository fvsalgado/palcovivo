"""
Primeira Plateia — Actualizar index.html com dados do master.json

Corre após o aggregate. Lê data/master.json e substitui o bloco
<script type="application/json" id="events-data"> no index.html.
Desta forma o site fica sempre actualizado sem deploy manual.
"""

import json
import html as html_mod
import re
import sys
from pathlib import Path

ROOT       = Path(__file__).parent.parent.parent
MASTER     = ROOT / "data" / "master.json"
INDEX_HTML = ROOT / "index.html"


def slim_event(e: dict) -> dict:
    """Converte evento harmonizado para formato slim do frontend."""
    dates = e.get("dates", [])
    aud   = e.get("audience") or {}
    price = e.get("price") or {}
    media = e.get("media") or {}
    acc   = e.get("accessibility") or {}

    def clean(s):
        if not isinstance(s, str): return s
        return html_mod.unescape(s)

    return {
        "id":         e.get("id", ""),
        "title":      clean(e.get("title", "")),
        "subtitle":   clean(e.get("subtitle")) if e.get("subtitle") else None,
        "desc":       clean((e.get("description") or "")[:200]).replace("\n", " ").replace("\r", "").strip(),
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
        "family":     bool(aud.get("is_family")) if isinstance(aud, dict) else False,
        "free":       bool(price.get("is_free")) if isinstance(price, dict) else False,
        "img":        media.get("cover_image") if isinstance(media, dict) else None,
        "url":        e.get("source_url", ""),
        "tags":       e.get("tags", []),
        "a11y":       bool(acc.get("has_sign_language")) if isinstance(acc, dict) else False,
        "origin":     None,
        "all_dates":  [d.get("date") for d in dates if d.get("date")],
    }


def update_index(master_path: Path = MASTER, index_path: Path = INDEX_HTML) -> int:
    # Carregar master.json
    with open(master_path, encoding="utf-8") as f:
        master = json.load(f)

    events = master.get("events", [])
    slim   = [slim_event(e) for e in events]
    print(f"update_index: {len(slim)} eventos do master.json")

    # Serializar
    events_json = json.dumps(slim, ensure_ascii=False, separators=(",", ":"))

    # Carregar index.html
    with open(index_path, encoding="utf-8") as f:
        content = f.read()

    # Substituir o bloco <script type="application/json" id="events-data">
    pattern = re.compile(
        r'(<script type="application/json" id="events-data">)'
        r'[\s\S]*?'
        r'(</script>)',
        re.MULTILINE
    )

    new_block = f'\\1\n{events_json}\n\\2'
    new_content, n_subs = pattern.subn(new_block, content)

    if n_subs == 0:
        # Bloco não existe — inserir antes do <script> principal
        insert_marker = '<script>\n// ==='
        pos = new_content.find(insert_marker)
        if pos == -1:
            print("ERRO: marcador do script não encontrado no index.html", file=sys.stderr)
            sys.exit(1)
        data_block = (
            f'<script type="application/json" id="events-data">\n'
            f'{events_json}\n'
            f'</script>\n\n'
        )
        new_content = new_content[:pos] + data_block + new_content[pos:]
        print("update_index: bloco inserido (era inexistente)")
    else:
        print(f"update_index: bloco substituído ({n_subs}x)")

    # Assegurar que o JS usa getElementById
    if "document.getElementById('events-data')" not in new_content:
        # Substituir a declaração EVENTS antiga (literal array ou JSON.parse antigo)
        new_content = re.sub(
            r"const EVENTS\s*=\s*(?:JSON\.parse\([^)]+\)|\[[\s\S]*?\]);",
            "const EVENTS = JSON.parse(document.getElementById('events-data').textContent);",
            new_content,
            count=1
        )
        print("update_index: declaração EVENTS corrigida para getElementById")

    with open(index_path, "w", encoding="utf-8") as f:
        f.write(new_content)

    print(f"update_index: index.html actualizado com {len(slim)} eventos")
    return len(slim)


if __name__ == "__main__":
    count = update_index()
    print(f"Concluído: {count} eventos no index")
