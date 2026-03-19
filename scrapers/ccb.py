"""
Scraper: CCB — Centro Cultural de Belém
Categoria: Teatro (PT)
Listagem: https://www.ccb.pt/eventos/category/teatro/

Estrutura:
  - A listagem é HTML estático (WordPress + The Events Calendar).
    Os links /evento/slug/ ou /evento/slug/YYYY-MM-DD/ estão no HTML.
  - A página de evento é renderizada server-side. Todo o conteúdo está
    dentro de .tribe_events-template-default.
  - As datas fiáveis vêm do JSON-LD (startDate / endDate ISO 8601).
  - O conteúdo detalhado (horários, sinopse, ficha, preço, idades)
    vem do innerText do bloco do evento, dividido por tabs:
      DATAS / HORÁRIOS → horários detalhados por dia
      IDADES           → classificação etária
      FICHA TÉCNICA    → ficha em texto corrido
      Preços e Descontos → preço base
  - Imagem: og:image
  - Bilhetes: primeiro link ccb.bol.pt ou bol.pt/Comprar na página
"""
import re
import time
import json
import requests
from bs4 import BeautifulSoup
from scrapers.utils import make_id, log, HEADERS, can_scrape, truncate_synopsis, build_image_object

BASE    = "https://www.ccb.pt"
AGENDA  = f"{BASE}/eventos/category/teatro/"
THEATER = "CCB — Centro Cultural de Belém"


def scrape():
    if not can_scrape(BASE):
        log(f"robots.txt: scraping bloqueado para {BASE}")
        return []
    urls = _collect_urls()
    events = []
    for url in sorted(urls):
        ev = _scrape_event(url)
        if ev:
            events.append(ev)
        time.sleep(0.4)
    log(f"[CCB] {len(events)} eventos de {len(urls)} URLs")
    return events


# ---------------------------------------------------------------------------
# Recolha de URLs da listagem
# ---------------------------------------------------------------------------

def _collect_urls():
    """
    A listagem é HTML estático — os links /evento/ estão no source.
    Normaliza URLs com data (/evento/slug/YYYY-MM-DD/) para base slug (/evento/slug/).
    """
    try:
        r = requests.get(AGENDA, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as e:
        log(f"[CCB] Erro na listagem: {e}")
        return set()

    # Todos os hrefs /evento/ no HTML
    raw = set(re.findall(
        r'href="(https?://www\.ccb\.pt/evento/[^"]+)"',
        r.text,
    ))

    urls = set()
    skip = {"mercado-ccb"}  # slugs que não são espetáculos

    for url in raw:
        # Normalizar: remover sufixo de data → URL base do evento
        url = re.sub(r"/\d{4}-\d{2}-\d{2}/?$", "/", url)
        url = url.rstrip("/") + "/"
        slug = url.rstrip("/").split("/")[-1]
        if slug and slug not in skip:
            urls.add(url)

    return urls


# ---------------------------------------------------------------------------
# Scrape de página individual
# ---------------------------------------------------------------------------

def _scrape_event(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as e:
        log(f"[CCB] Erro em {url}: {e}")
        return None

    soup = BeautifulSoup(r.text, "lxml")

    # ----- JSON-LD — fonte mais fiável para título e datas -----
    ld_data = {}
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, list):
                data = data[0]
            if isinstance(data, dict) and data.get("@type") == "Event":
                ld_data = data
                break
        except Exception:
            continue

    # ----- Título -----
    title = ld_data.get("name", "").strip()
    if not title:
        h1 = soup.select_one("h1")
        title = h1.get_text(strip=True) if h1 else ""
    if not title or len(title) < 3:
        return None

    # ----- Datas (JSON-LD é sempre fiável) -----
    date_start = ""
    date_end   = ""
    dates_label = ""
    if ld_data.get("startDate"):
        date_start = ld_data["startDate"][:10]
    if ld_data.get("endDate"):
        date_end = ld_data["endDate"][:10]
    if not date_end:
        date_end = date_start
    if date_start:
        dates_label = (
            f"{date_start} – {date_end}"
            if date_end and date_end != date_start
            else date_start
        )

    if not date_start:
        return None  # sem datas → descarta

    # ----- Conteúdo principal — .tribe_events-template-default -----
    content_el = soup.select_one(".tribe_events-template-default")
    content_text = content_el.get_text("\n") if content_el else soup.get_text("\n")

    # ----- Imagem -----
    image = ""
    og_img = soup.find("meta", property="og:image")
    if og_img and og_img.get("content", "").startswith("http"):
        image = og_img["content"]
    if not image and ld_data.get("image"):
        img = ld_data["image"]
        image = img if isinstance(img, str) else (img.get("url", "") if isinstance(img, dict) else "")

    # ----- Bilhetes -----
    ticket_url = ""
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if "ccb.bol.pt" in href or ("bol.pt/Comprar" in href and "ccb" in href.lower()):
            ticket_url = href
            break
    if not ticket_url:
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            if "bol.pt/Comprar" in href:
                ticket_url = href
                break

    # ----- Sala -----
    # Está no cabeçalho das tabs: "BLACK BOX", "PEQUENO AUDITÓRIO", "GRANDE AUDITÓRIO"
    sala = ""
    sala_m = re.search(
        r"\b(BLACK BOX|PEQUENO AUDIT[OÓ]RIO|GRANDE AUDIT[OÓ]RIO|ESPA[CÇ]O F[AÁ]BRICA DAS ARTES|"
        r"F[AÁ]BRICA DAS ARTES|AUDIT[OÓ]RIO|GALERIA)\b",
        content_text, re.IGNORECASE,
    )
    if sala_m:
        sala = sala_m.group(1).title()

    # ----- Horários detalhados -----
    # "DATAS / HORÁRIOS" aparece duas vezes: primeiro como tab header, depois como título do bloco.
    # O conteúdo real está após a SEGUNDA ocorrência.
    schedule = _extract_schedule(content_text)

    # ----- Sinopse -----
    # A sinopse vem após o bloco "Teatro\n" (label da categoria) antes da "PARTILHAR"
    synopsis = ""
    # Tentar og:description primeiro (versão curta)
    og_desc = soup.find("meta", property="og:description")
    if og_desc:
        synopsis = og_desc.get("content", "").strip()
    # Versão completa: parágrafos do content_el que sejam narrativa
    if content_el:
        for p in content_el.select("p"):
            t = p.get_text(strip=True)
            if len(t) > 80 and not re.match(
                r"^(A Funda|PARTILHAR|Para quaisquer|Descarregue|FICHA|IDADES|PREÇOS|Preços)",
                t, re.IGNORECASE,
            ):
                synopsis = t if not synopsis else synopsis + " " + t
                if len(synopsis) > 1000:
                    break

    # ----- Indicação de idade -----
    age_rating = ""
    age_block = _extract_block(content_text, "IDADES", ["FICHA TÉCNICA", "PREÇOS", "Preços"])
    if age_block:
        age_m = re.search(r"(\+\s*\d+|M\s*/\s*\d+|Livre|[Tt]odas as idades|[Cc]lassificação etária a designar)", age_block)
        if age_m:
            age_rating = age_m.group(1).strip()
        else:
            age_rating = age_block.strip()[:40]

    # ----- Ficha técnica -----
    technical_sheet = {}
    ficha_block = _extract_block(content_text, "FICHA TÉCNICA", ["Preços e Descontos", "PREÇOS", "COMPRAR"])
    if ficha_block:
        technical_sheet = _parse_ficha(ficha_block)

    # ----- Preço -----
    price = ""
    price_block = _extract_block(content_text, "Preços e Descontos", ["DESCONTOS", "COMPRAR BILHETE\n"])
    if not price_block:
        price_block = _extract_block(content_text, "PREÇOS", ["COMPRAR", "DESCONTOS"])
    if price_block:
        pm = re.search(
            r"(Entrada\s+livre"
            r"|\d+(?:[,\.]\d+)?€\s*[-–]\s*\d+(?:[,\.]\d+)?€"
            r"|\d+(?:[,\.]\d+)?[-–]\d+(?:[,\.]\d+)?€"
            r"|\d+(?:[,\.]\d+)?\s*€)",
            price_block, re.IGNORECASE,
        )
        if pm:
            price = pm.group(1).strip()

    # ----- Subtítulo / autores -----
    # No DOM: linha imediatamente após o <h1>
    subtitle = ""
    if content_el:
        h1_el = content_el.select_one("h1")
        if h1_el:
            next_el = h1_el.find_next_sibling()
            if next_el:
                sub = next_el.get_text(strip=True)
                # É subtítulo se for curto e não for uma tab
                if sub and len(sub) < 120 and not re.match(r"^(DATAS|PREÇOS|IDADES|COMPRAR|BLACK|PEQU|GRAND)", sub, re.IGNORECASE):
                    subtitle = sub

    return {
        "id":              make_id("ccb", title),
        "title":           title,
        "subtitle":        subtitle,
        "theater":         THEATER,
        "category":        "Teatro",
        "dates_label":     dates_label,
        "date_start":      date_start,
        "date_end":        date_end,
        "schedule":        schedule,
        "description":     truncate_synopsis(synopsis),
        "image":           build_image_object(image, None, "CCB — Centro Cultural de Belém", url),
        "url":             url,
            "source_url":      url,
        "ticket_url":      ticket_url,
        "price":           price,
        "age_rating":      age_rating,
        "sala":            sala,
        "technical_sheet": technical_sheet,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_schedule(text):
    """
    "DATAS / HORÁRIOS" aparece duas vezes no texto do evento:
      1.ª vez — como tab header (seguida do nome da sala e outras tabs)
      2.ª vez — como título do bloco de conteúdo (seguida dos horários reais)
    O conteúdo útil está após a segunda ocorrência.
    """
    label = "DATAS / HORÁRIOS"
    first = text.find(label)
    if first == -1:
        return ""
    second = text.find(label, first + len(label))
    content_start = (second + len(label)) if second != -1 else (first + len(label))

    # Termina antes da sinopse ("Teatro\n"), IDADES ou FICHA TÉCNICA
    end_idx = len(text)
    for end_label in ["Teatro\n", "\nIDADES", "\nFICHA", "PARTILHAR", "\nPREÇOS"]:
        pos = text.find(end_label, content_start)
        if pos != -1 and pos < end_idx:
            end_idx = pos

    block = text[content_start:end_idx].strip()
    return re.sub(r"\n{3,}", "\n\n", block)


def _extract_block(text, start_label, end_labels):
    """
    Extrai o texto entre start_label e o primeiro end_label encontrado.
    Retorna string limpa ou "" se não encontrado.
    """
    # Encontrar o início
    idx = text.find(start_label)
    if idx == -1:
        # Tentar case-insensitive
        m = re.search(re.escape(start_label), text, re.IGNORECASE)
        if not m:
            return ""
        idx = m.start()

    content_start = idx + len(start_label)

    # Encontrar o fim (primeiro end_label que aparece depois)
    end_idx = len(text)
    for label in end_labels:
        pos = text.find(label, content_start)
        if pos != -1 and pos < end_idx:
            end_idx = pos

    block = text[content_start:end_idx].strip()
    return re.sub(r"\n{3,}", "\n\n", block)  # comprimir linhas em branco


def _parse_ficha(text):
    """
    Extrai ficha técnica como dict a partir de texto corrido.
    Formato CCB: cada linha começa com uma chave (pode ter vírgulas e 'e')
    seguida do valor, ex:
      "Criação Flávia Gusmão e Jacinto Lucas Pires"
      "Conceito, realização, argumento e narração Joana Craveiro"
      "Desenho de Luz Nuno Meira"
    """
    ficha = {}

    known_keys = [
        # Chaves compostas com vírgulas (ex: "Conceito, realização, argumento e narração")
        ("conceito",         r"(?<!\w)[Cc]onceito(?:[^A-ZÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕ\n]{0,80}?(?<=[a-záéíóú\s]))\s+(?=[A-ZÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕ])"),
        ("criação",          r"(?<!\w)[Cc]ria[çc][aã]o\s+"),
        ("texto",            r"(?<!\w)[Tt]exto\s+"),
        ("autor",            r"(?<!\w)[Aa]utor[a]?\s*[:\s]\s*"),
        ("dramaturgia",      r"(?<!\w)[Dd]ramaturgia\s+"),
        ("encenação",        r"(?<!\w)[Ee]ncena[çc][aã]o\s+"),
        ("direção",          r"(?<!\w)[Dd]ire[çc][aã]o\s+(?:artística\s+|de\s+[Pp]rodu[çc][aã]o\s+)?"),
        ("tradução",         r"(?<!\w)[Tt]radu[çc][aã]o\s+"),
        ("adaptação",        r"(?<!\w)[Aa]dapta[çc][aã]o\s+"),
        ("cenografia",       r"(?<!\w)[Cc]enografia\s+"),
        ("figurinos",        r"(?<!\w)[Ff]igurinos?\s+"),
        ("luz",              r"(?<!\w)[Dd]esenho\s+de\s+[Ll]uz\s+|(?<!\w)[Ii]lumina[çc][aã]o\s+"),
        ("som",              r"(?<!\w)[Mm][úu]sica\s+e\s+[Dd]esenho\s+de\s+[Ss]om\s+"
                              r"|(?<!\w)[Dd]esenho\s+de\s+[Ss]om\s+|(?<!\w)[Ss]onoplastia\s+"),
        ("música",           r"(?<!\w)[Mm][úu]sica\s+"),
        ("coreografia",      r"(?<!\w)[Cc]oreografia\s+"),
        ("interpretação",    r"(?<!\w)[Ii]nterpreta[çc][aã]o\s+"),
        ("produção",         r"(?<!\w)[Pp]rodu[çc][aã]o\s+[Ee]xecutiva\s+|(?<!\w)[Pp]rodu[çc][aã]o\s+"),
        ("coprodução",       r"(?<!\w)[Cc]oprodu[çc][aã]o\s+"),
        ("elenco",           r"(?<!\w)[Cc]om\s+(?=[A-ZÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕÇÑ])"),
    ]

    positions = []
    for key, pattern in known_keys:
        for m in re.finditer(pattern, text):
            positions.append((m.start(), m.end(), key))

    positions.sort()

    for i, (start, end, key) in enumerate(positions):
        next_start = positions[i + 1][0] if i + 1 < len(positions) else end + 300
        value = text[end:next_start].strip()
        value = re.sub(r"\s+", " ", value).strip()
        value = re.split(r"\s+(?:Preços|COMPRAR|Planta|DESCONTOS|Agradecimentos)", value)[0].strip()
        value = value[:300]
        if value and key not in ficha:
            ficha[key] = value

    return ficha
