"""
Scraper: São Luiz Teatro Municipal
URL listagem: https://www.teatrosaoluiz.pt/programacao/
URLs eventos:  /espetaculo/slug/

Estrutura da página de evento (WordPress estático):
  - Breadcrumb: ano / "DD mes - DD mes" / categoria / "ESTREIA" (opcional)
  - Título:     <h1>
  - Subtítulo:  linha após o h1 (autores/encenação breve)
  - Campos info com <span class="subtitle"> como label:
      DATAS E HORÁRIOS  → datas + horários detalhados
      LOCAL             → sala
      DURAÇÃO           → duração
      PREÇO             → preço
      CLASSIFICAÇÃO     → indicação de idade
      ACESSIBILIDADE    → sessões acessíveis
  - Descrição:  secção .event-description
  - Ficha técnica: secção .event-tech-details com <span class="subtitle"> como labels
  - Bilhetes:   href='saoluiz.bol.pt/...' (aspas simples — atenção!)
  - Imagem:     og:image
"""
import re
import time
import requests
from bs4 import BeautifulSoup
from scrapers.utils import make_id, parse_date_range, parse_date, log, HEADERS, can_scrape, truncate_synopsis, build_image_object

BASE    = "https://www.teatrosaoluiz.pt"
AGENDA  = f"{BASE}/programacao/"
THEATER = "São Luiz Teatro Municipal"
IMG_DOMAIN = "www.teatrosaoluiz.pt"


def scrape():
    if not can_scrape(BASE):
        log(f"robots.txt: scraping bloqueado para {BASE}")
        return []
    try:
        r = requests.get(AGENDA, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as e:
        log(f"[São Luiz] Erro na listagem: {e}")
        return []

    soup = BeautifulSoup(r.text, "lxml")
    seen, events = set(), []

    for a in soup.find_all("a", href=re.compile(r"/espetaculo/")):
        href = a["href"]
        full = href if href.startswith("http") else BASE + href
        if full in seen:
            continue
        seen.add(full)
        ev = _scrape_event(full)
        if ev:
            events.append(ev)
        time.sleep(0.3)

    log(f"[São Luiz] {len(events)} eventos")
    return events


def _scrape_event(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as e:
        log(f"[São Luiz] Erro em {url}: {e}")
        return None

    soup = BeautifulSoup(r.text, "lxml")
    raw  = r.text

    # --- Título ---
    title_el = soup.select_one("h1")
    if not title_el:
        return None
    title = title_el.get_text(strip=True)
    if not title or len(title) < 3:
        return None

    # --- Subtítulo (autores / encenação breve — linha após o h1) ---
    subtitle = ""
    sub_el = title_el.find_next_sibling()
    if sub_el:
        sub = sub_el.get_text(strip=True)
        if sub and len(sub) < 120 and not re.match(
            r"^(©|COMPRAR|BILHETE|DATAS|LOCAL|DURA|PRE[ÇC]O|CLASSI|ACESSI)",
            sub, re.IGNORECASE
        ):
            subtitle = sub

    # --- Categoria (breadcrumb) ---
    category = "Teatro"
    cat_el = soup.select_one(".breadcrumbs a, [class*='breadcrumb'] a, [class*='categoria'] a")
    if not cat_el:
        # Fallback: "teatro", "música", "dança" no breadcrumb text
        bc = soup.select_one(".breadcrumbs, [class*='breadcrumb']")
        if bc:
            cat_m = re.search(
                r"\b(teatro|m[uú]sica|dan[çc]a|circo|performance|"
                r"pensamento|exposi[çc][aã]o|visita|espa[çc]o p[uú]blico)\b",
                bc.get_text(" "), re.IGNORECASE,
            )
            if cat_m:
                category = cat_m.group(1).capitalize()
    else:
        category = cat_el.get_text(strip=True).capitalize()

    # --- Campos estruturados via <span class="subtitle"> ---
    # Cada campo é um container com: <span class="subtitle">LABEL</span> + conteúdo
    fields = _parse_subtitle_fields(soup)
    dates_label = fields.get("datas_label", "")
    schedule    = fields.get("schedule", "")
    sala        = fields.get("local", "")
    duration    = fields.get("duracao", "")
    price       = fields.get("preco", "")
    age_rating  = fields.get("classificacao", "")
    accessibility = fields.get("acessibilidade", "")

    # --- Datas (parse das strings do campo DATAS E HORÁRIOS) ---
    date_start, date_end = _parse_dates_from_field(dates_label)

    # --- Imagem ---
    image = _get_image(soup, raw)

    # --- Bilhetes (aspas simples E duplas no HTML) ---
    ticket_url = ""
    # Procurar com BeautifulSoup — apanha ambos os tipos de aspas
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "saoluiz.bol.pt" in href or "bol.pt/Comprar" in href or "ticketline" in href:
            ticket_url = href
            break
    # Fallback: regex no HTML raw para aspas simples
    if not ticket_url:
        m = re.search(r"href='(https?://[^']*(?:saoluiz\.bol\.pt|bol\.pt/Comprar)[^']*)'", raw)
        if m:
            ticket_url = m.group(1)

    # --- Descrição ---
    description = ""
    desc_el = soup.select_one(".event-description, section.event-description")
    if desc_el:
        for p in desc_el.select("p"):
            t = p.get_text(strip=True)
            if len(t) > 60:
                description += (" " if description else "") + t
                if len(description) > 1000:
                    break
    if not description:
        og_desc = soup.find("meta", property="og:description")
        if og_desc:
            description = og_desc.get("content", "").strip()

    # --- Ficha técnica ---
    technical_sheet = _parse_ficha(soup)

    return {
        "id":              make_id("saoluiz", title),
        "title":           title,
        "subtitle":        subtitle,
        "theater":         THEATER,
        "category":        category,
        "dates_label":     dates_label,
        "date_start":      date_start,
        "date_end":        date_end,
        "schedule":        schedule,
        "description":     truncate_synopsis(description),
        "image":           build_image_object(image, None, "São Luiz Teatro Municipal", url),
        "url":             url,
            "source_url":      url,
        "ticket_url":      ticket_url,
        "price":           price,
        "duration":        duration,
        "age_rating":      age_rating,
        "accessibility":   accessibility,
        "sala":            sala,
        "technical_sheet": technical_sheet,
    }


# ---------------------------------------------------------------------------
# Parsing dos campos estruturados
# ---------------------------------------------------------------------------

def _parse_subtitle_fields(soup):
    """
    Extrai os campos de info do evento.
    Cada campo é um bloco com <span class="subtitle">LABEL</span> seguido do conteúdo.
    Retorna dict com as chaves normalizadas.
    """
    result = {}

    # Mapeamento label → chave interna
    LABEL_MAP = {
        "DATAS E HORÁRIOS": "datas_label",
        "DATAS":            "datas_label",
        "LOCAL":            "local",
        "DURAÇÃO":          "duracao",
        "PREÇO":            "preco",
        "CLASSIFICAÇÃO":    "classificacao",
        "ACESSIBILIDADE":   "acessibilidade",
    }

    for span in soup.select("span.subtitle"):
        label_raw = span.get_text(strip=True).upper()
        # Normalizar: remover acentos para comparação
        key = LABEL_MAP.get(label_raw)
        if not key:
            continue

        # O container do campo é o elemento pai do span
        container = span.parent
        if not container:
            continue

        # Texto do container sem o próprio label
        full_text = container.get_text("\n", strip=True)
        # Remover o label do início
        label_len = len(span.get_text(strip=True))
        value = full_text[label_len:].strip()
        value = re.sub(r"\n{3,}", "\n\n", value).strip()

        if value:
            result[key] = value

    # Separar datas_label de schedule: a primeira linha é a data, o resto é o horário
    if "datas_label" in result:
        lines = [l.strip() for l in result["datas_label"].splitlines() if l.strip()]
        if lines:
            result["datas_label"] = lines[0]   # ex: "21 março a 4 abril"
            if len(lines) > 1:
                result["schedule"] = "\n".join(lines[1:])  # ex: "quarta a sábado, 19h30; domingo, 16h"

    return result


def _parse_dates_from_field(dates_label):
    """
    Converte o texto do campo DATAS E HORÁRIOS para date_start e date_end.
    Formatos: "18 a 29 março", "21 março a 4 abril", "27 março" (data única)
    """
    if not dates_label:
        return "", ""

    # Intervalo "DD mes a DD mes" ou "DD a DD mes"
    date_start, date_end = parse_date_range(dates_label)
    if date_start:
        return date_start, date_end

    # Data única
    d = parse_date(dates_label)
    return d, d


def _parse_ficha(soup):
    """
    Extrai a ficha técnica da secção .event-tech-details.
    Os labels são <span class="subtitle"> e os valores são texto corrido.
    """
    ficha = {}
    tech_el = soup.select_one(".event-tech-details")
    if not tech_el:
        return ficha

    # Obter o texto corrido completo da secção
    text = tech_el.get_text(" ")

    # As chaves são os <span class="subtitle"> dentro desta secção
    spans = tech_el.select("span.subtitle")
    if not spans:
        return ficha

    # Construir lista de posições (início/fim de cada label no texto corrido)
    positions = []
    for span in spans:
        label = span.get_text(strip=True)
        # Normalizar chave
        key = _normalise_ficha_key(label)
        if not key:
            continue
        # Encontrar posição no texto
        idx = text.find(label)
        if idx >= 0:
            positions.append((idx, idx + len(label), key))

    positions.sort()

    for i, (start, end, key) in enumerate(positions):
        next_start = positions[i + 1][0] if i + 1 < len(positions) else end + 400
        value = text[end:next_start].strip()
        value = re.sub(r"\s+", " ", value).strip()
        # Parar antes de "VEJA TAMBÉM" ou "COPRODUÇÃO" se não for a chave
        if key not in ("coprodução", "parceria", "apoio"):
            value = re.split(r"\s+COPRODUÇÃO\b|\s+PARCERIA\b|\s+APOIO\b|\s+AGRADECIMENTOS\b", value, flags=re.IGNORECASE)[0]
        value = value[:300].strip()
        if value and key not in ficha:
            ficha[key] = value

    return ficha


def _normalise_ficha_key(label):
    """Converte label da ficha para chave normalizada."""
    label_up = label.upper().strip()

    # Ordem importa: chaves mais específicas primeiro
    KEY_MAP = [
        ("TEXTO E ENCENAÇÃO",       "texto_encenação"),
        ("TEXTO",                   "texto"),
        ("ENCENAÇÃO",               "encenação"),
        ("DRAMATURGIA",             "dramaturgia"),
        ("DIREÇÃO ARTÍSTICA",       "direção"),
        ("DIREÇÃO DE PRODUÇÃO",     "direção_produção"),
        ("DIREÇÃO",                 "direção"),
        ("TRADUÇÃO",                "tradução"),
        ("ADAPTAÇÃO",               "adaptação"),
        ("CENOGRAFIA E FIGURINOS",  "cenografia"),
        ("ESPAÇO CÉNICO",           "cenografia"),
        ("CENOGRAFIA",              "cenografia"),
        ("FIGURINOS",               "figurinos"),
        ("DESENHO DE LUZ",          "luz"),
        ("ILUMINAÇÃO",              "luz"),
        ("MÚSICA E ESPAÇO SONORO",  "música"),
        ("MÚSICA E DESENHO DE SOM", "música"),
        ("DESENHO DE SOM",          "som"),
        ("SONOPLASTIA",             "som"),
        ("MÚSICA",                  "música"),
        ("COMPOSIÇÃO",              "música"),
        ("COREOGRAFIA",             "coreografia"),
        ("INTERPRETAÇÃO",           "interpretação"),
        ("ELENCO",                  "interpretação"),
        ("PRODUÇÃO EXECUTIVA",      "produção"),
        ("PRODUÇÃO E COMUNICAÇÃO",  "produção"),
        ("PRODUÇÃO",                "produção"),
        ("COPRODUÇÃO",              "coprodução"),
        ("ASSISTENTE DE ENCENAÇÃO", "ass_encenação"),
        ("ASSISTÊNCIA DE ENCENAÇÃO","ass_encenação"),
    ]

    for label_key, mapped in KEY_MAP:
        if label_up == label_key:
            return mapped

    # Nenhum match — ignorar
    return None


# ---------------------------------------------------------------------------
# Imagem
# ---------------------------------------------------------------------------

def _get_image(soup, raw):
    # 1. og:image — mais fiável
    og = soup.find("meta", property="og:image")
    if og:
        src = og.get("content", "")
        if src.startswith("http"):
            return src

    # 2. img src directo
    skip = {"blank", "logo", "tsl/icons", "tsl/assets", "lgp.svg", "ad.svg"}
    for img in soup.find_all("img"):
        src = img.get("src", "")
        if not src or not src.startswith("http"):
            continue
        if any(s in src for s in skip):
            continue
        if len(src) > 30:
            return src

    # 3. lazy-load attrs
    for img in soup.find_all("img"):
        for attr in ["data-lazysrc", "data-src", "data-original"]:
            src = img.get(attr, "")
            if src and "blank" not in src and len(src) > 20:
                return src if src.startswith("http") else BASE + src

    # 4. regex no HTML raw — wp-content/uploads
    pattern = r"https?://" + re.escape(IMG_DOMAIN) + r"/wp-content/uploads/[\w/._-]+\.(?:jpg|jpeg|png|webp)"
    m = re.search(pattern, raw)
    if m:
        return m.group(0)

    return ""
