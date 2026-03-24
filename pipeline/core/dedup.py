"""
Primeira Plateia — Deduplicador
Deteta e resolve duplicados entre venues e entre sessões.
"""

import logging
from collections import defaultdict
from .harmonizer import slugify, normalize_title

logger = logging.getLogger(__name__)


def build_fingerprint_index(events: list[dict]) -> dict[str, list[int]]:
    """
    Constrói índice de fingerprints → índices de eventos.
    Usado para detetar duplicados.
    """
    index = defaultdict(list)
    for i, event in enumerate(events):
        fp = event.get("dedup", {}).get("fingerprint", "")
        if fp:
            index[fp].append(i)
    return dict(index)


def resolve_duplicates(events: list[dict]) -> list[dict]:
    """
    Dado um conjunto de eventos (potencialmente de vários venues),
    deteta duplicados pelo fingerprint e:
    - Marca o mais completo como canónico
    - Marca os outros como duplicate_of
    - Actualiza seen_at_venues em todos
    - Remove os não-canónicos da lista output (mantém referência)
    """
    index = build_fingerprint_index(events)
    duplicate_groups = {fp: idxs for fp, idxs in index.items() if len(idxs) > 1}

    if duplicate_groups:
        logger.info(f"Dedup: {len(duplicate_groups)} grupos de duplicados encontrados")

    for fp, idxs in duplicate_groups.items():
        group = [events[i] for i in idxs]

        # Escolher canónico: o com mais informação (mais campos preenchidos)
        def completeness_score(e: dict) -> int:
            score = 0
            score += 1 if e.get("description") else 0
            score += 1 if e.get("subtitle") else 0
            score += len(e.get("dates", []))
            score += 1 if e.get("price", {}).get("price_display") else 0
            score += 1 if e.get("media", {}).get("cover_image") else 0
            score += len(e.get("credits", {}).get("cast", []))
            return score

        canonical_idx_in_group = max(range(len(group)), key=lambda i: completeness_score(group[i]))
        canonical_event = group[canonical_idx_in_group]

        # Recolher todos os venues onde o evento foi visto
        all_venues = list(set(
            venue
            for e in group
            for venue in e.get("dedup", {}).get("seen_at_venues", [])
        ))

        # Actualizar canónico
        canonical_event["dedup"]["is_canonical"] = True
        canonical_event["dedup"]["seen_at_venues"] = all_venues
        canonical_event["is_multi_venue"] = len(all_venues) > 1
        canonical_event["multi_venue_ids"] = all_venues if len(all_venues) > 1 else []

        # Marcar duplicados
        for i, event in enumerate(group):
            if i != canonical_idx_in_group:
                event["dedup"]["is_canonical"] = False
                event["dedup"]["duplicate_of"] = canonical_event["id"]
                event["dedup"]["seen_at_venues"] = all_venues
                event["pipeline"]["is_active"] = False

        logger.info(
            f"Dedup: '{canonical_event['title'][:40]}' — "
            f"canónico: {canonical_event['venue_id']}, "
            f"duplicados em: {[e['venue_id'] for j, e in enumerate(group) if j != canonical_idx_in_group]}"
        )

    # Retornar apenas os canónicos e os sem duplicados
    output = [e for e in events if e.get("dedup", {}).get("is_canonical", True)]
    removed = len(events) - len(output)
    if removed:
        logger.info(f"Dedup: {removed} duplicados removidos do output ({len(output)} eventos únicos)")

    return output


def merge_sessions(events: list[dict]) -> list[dict]:
    """
    Consolida sessões separadas do mesmo espetáculo no mesmo venue
    num único evento com múltiplas datas.
    
    Alguns APIs retornam cada sessão como evento separado.
    Detectamos isso pelo fingerprint + venue_id iguais.
    """
    # Agrupar por (venue_id, fingerprint)
    groups: dict[str, list[dict]] = defaultdict(list)
    for event in events:
        fp = event.get("dedup", {}).get("fingerprint", "")
        vid = event.get("venue_id", "")
        key = f"{vid}|{fp}"
        groups[key].append(event)

    merged_groups = {k: v for k, v in groups.items() if len(v) > 1}
    if merged_groups:
        logger.info(f"Merge: {len(merged_groups)} grupos de sessões a consolidar")

    result = []
    processed_keys = set()

    for event in events:
        fp = event.get("dedup", {}).get("fingerprint", "")
        vid = event.get("venue_id", "")
        key = f"{vid}|{fp}"

        if key in processed_keys:
            continue

        if key in merged_groups:
            group = merged_groups[key]
            # Usar o evento com mais informação como base
            base = max(group, key=lambda e: len(e.get("description", "") or ""))
            # Agregar todas as datas
            all_dates = []
            seen_dates = set()
            for e in group:
                for d in e.get("dates", []):
                    date_key = f"{d.get('date')}_{d.get('time_start')}"
                    if date_key not in seen_dates:
                        all_dates.append(d)
                        seen_dates.add(date_key)

            all_dates.sort(key=lambda d: (d.get("date", ""), d.get("time_start", "")))
            base["dates"] = all_dates
            base["total_sessions"] = len(all_dates)
            base["date_first"] = all_dates[0]["date"] if all_dates else None
            base["date_last"] = all_dates[-1]["date"] if all_dates else None

            # Recalcular status se agora tem mais sessões
            if len(all_dates) == 1:
                base["event_status"] = "unica-sessao"

            result.append(base)
            processed_keys.add(key)
            logger.info(
                f"Merge: '{base['title'][:40]}' — "
                f"{len(group)} sessões → 1 evento com {len(all_dates)} datas"
            )
        else:
            result.append(event)
            processed_keys.add(key)

    return result


def deduplicate(events: list[dict]) -> list[dict]:
    """
    Pipeline completo de deduplicação:
    1. Merge de sessões separadas do mesmo venue
    2. Resolução de duplicados entre venues
    """
    logger.info(f"Dedup: início com {len(events)} eventos")
    events = merge_sessions(events)
    logger.info(f"Dedup: após merge de sessões: {len(events)} eventos")
    events = resolve_duplicates(events)
    logger.info(f"Dedup: após resolução de duplicados: {len(events)} eventos")
    return events
