# scraper/scrape_calendar.py
# -*- coding: utf-8 -*-

import sys
import csv
import json
import re
import time
import unicodedata
from pathlib import Path
from datetime import datetime
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

URL = "https://med.estrategia.com/portal/residencia-medica/calendario-de-residencia-medica-confira-as-datas-das-proximas-provas/"
HEADERS_WANTED = ["UF", "SELEÇÃO", "INSCRIÇÕES", "PROVA OBJETIVA", "EDITAL"]

# ------------------ Rede ------------------

def fetch_html(url: str) -> str:
    """GET com cache-buster e headers para evitar CDN cacheada."""
    resp = requests.get(
        url,
        params={"_": int(time.time())},  # cache-buster
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) CalendarioBot/1.1",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.text

# ------------------ Util ------------------

DATE_RE = re.compile(r"\b(\d{2})/(\d{2})/(\d{4})\b")

def normalize_label(s: str) -> str:
    s = (s or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s

def anos_meses_de(texto: str):
    """Retorna lista de (ano, mes) encontrados no texto."""
    out = []
    for _d, m, y in DATE_RE.findall(texto or ""):
        out.append((int(y), int(m)))
    return out

def ajustar_ano_prova_pelo_periodo_inscricao(inscricoes: str, prova: str) -> str:
    """
    Se houver inscrição em out/nov/dez de ano Y e prova em jan/fev/mar do mesmo Y,
    ajusta prova para ano Y+1.
    """
    if not prova:
        return prova

    ins = anos_meses_de(inscricoes)
    prv = list(DATE_RE.finditer(prova))
    if not ins or not prv:
        return prova

    anos_q4 = {y for (y, m) in ins if m >= 10}
    new_prova = prova

    # substitui de trás pra frente para manter índices corretos
    for m in reversed(prv):
        d, mm, yy = map(int, m.groups())
        if yy in anos_q4 and 1 <= mm <= 3:
            yy_new = yy + 1
            start, end = m.span()
            new_prova = new_prova[:start] + f"{d:02d}/{mm:02d}/{yy_new}" + new_prova[end:]

    return new_prova

def parse_date_br_first(s: str):
    """Primeira data dd/mm/yyyy no texto -> datetime (ou None)."""
    m = DATE_RE.search(s or "")
    if not m:
        return None
    d, mth, y = map(int, m.groups())
    try:
        return datetime(y, mth, d)
    except ValueError:
        return None

# ------------------ Parsing página principal ------------------

def find_calendar_table(soup: BeautifulSoup):
    """Tenta encontrar a tabela correta por cabeçalhos."""
    tables = soup.find_all("table")
    for tb in tables:
        thead = tb.find("thead")
        tbody = tb.find("tbody")
        if not thead or not tbody:
            continue
        headers = [th.get_text(strip=True).upper() for th in thead.find_all("th")]
        score = sum(1 for h in HEADERS_WANTED if h in headers)
        if score >= 3:
            return tb, headers
    return None, None

def parse_main_table(table, headers_upper):
    """Extrai colunas desejadas + URL do EDITAL (para scraping detalhado)."""
    idx = {h: headers_upper.index(h) if h in headers_upper else None for h in HEADERS_WANTED}

    rows_out = []
    for tr in table.find("tbody").find_all("tr"):
        tds = tr.find_all(["td", "th"])
        if not tds:
            continue

        def get_text(h):
            i = idx[h]
            if i is None or i >= len(tds):
                return ""
            return tds[i].get_text(strip=True)

        def get_edital_href():
            i = idx.get("EDITAL")
            if i is None or i >= len(tds):
                return None
            a = tds[i].find("a")
            if a and a.get("href"):
                return a["href"].strip()
            return None

        uf = get_text("UF")
        selecao = get_text("SELEÇÃO")
        inscr = get_text("INSCRIÇÕES")
        prova = get_text("PROVA OBJETIVA")
        edital_href = get_edital_href()

        if not uf or not selecao:
            continue

        rows_out.append({
            "UF": uf,
            "INSTITUIÇÃO": selecao,
            "INSCRIÇÕES": inscr,
            "PROVA_OBJETIVA": prova,
            "_EDITAL_URL": edital_href,   # campo interno, não sai no CSV/JSON final
        })
    return rows_out

# ------------------ Parsing página de edital ------------------

DETAIL_KEYS = {
    "data da prova": "data_prova",
    "gabarito preliminar": "gabarito_preliminar",
    "resultado final": "resultado_final",
}

def parse_detail_page(edital_url: str) -> dict:
    """
    Acessa a página de notícias do edital e tenta ler a tabela "Resumo edital ...".
    Retorna dict com possíveis chaves: data_prova, gabarito_preliminar, resultado_final.
    """
    if not edital_url:
        return {}

    html = fetch_html(edital_url if edital_url.startswith("http") else urljoin(URL, edital_url))
    soup = BeautifulSoup(html, "lxml")

    # Heurística: procurar por tabelas onde a 1ª coluna parece "rótulo" e a 2ª o "valor".
    out = {}
    for tb in soup.find_all("table"):
        trs = tb.find_all("tr")
        if not trs:
            continue
        for tr in trs:
            tds = tr.find_all(["td", "th"])
            if len(tds) < 2:
                continue
            label = normalize_label(tds[0].get_text(" ", strip=True))
            value = tds[1].get_text(" ", strip=True)

            if label in DETAIL_KEYS:
                out[DETAIL_KEYS[label]] = value

        # Se já encontramos o trio, podemos parar
        if any(k in out for k in ("data_prova", "gabarito_preliminar", "resultado_final")):
            # não quebra: outras tabelas podem complementar, mas isso já cobre o normal
            pass

    return out

# ------------------ Persistência ------------------

def save_csv(data, path_csv):
    keys = ["UF", "INSTITUIÇÃO", "INSCRIÇÕES", "PROVA_OBJETIVA", "GABARITO_PRELIMINAR", "RESULTADO_FINAL"]
    with open(path_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        for row in data:
            w.writerow({k: row.get(k, "") for k in keys})

def save_json(data, path_json):
    with open(path_json, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def save_meta():
    """Grava meta.json com horário de Brasília fixado em 07:00 do dia da execução."""
    Path("data").mkdir(exist_ok=True)
    hoje_brt = datetime.now(ZoneInfo("America/Sao_Paulo")).date()
    meta_dt = datetime(hoje_brt.year, hoje_brt.month, hoje_brt.day, 7, 0, tzinfo=ZoneInfo("America/Sao_Paulo"))
    meta = {"last_update_brt": meta_dt.strftime("%d/%m/%Y %H:%M")}
    with open("data/meta.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

# ------------------ Main ------------------

def main():
    # 1) Página principal
    html = fetch_html(URL)
    soup = BeautifulSoup(html, "lxml")

    table, headers = find_calendar_table(soup)
    if not table:
        print("[ERRO] Tabela do calendário não encontrada.", file=sys.stderr)
        sys.exit(1)

    rows = parse_main_table(table, headers_upper=[h.upper() for h in headers])

    # 2) Para cada linha, abrir o EDITAL e coletar dados extras
    for r in rows:
        edital_url = r.get("_EDITAL_URL")
        if not edital_url:
            r["GABARITO_PRELIMINAR"] = "-"
            r["RESULTADO_FINAL"] = "-"
            continue

        try:
            detail = parse_detail_page(edital_url)
            # Reconcilia Data da Prova (prioriza a do detalhe se existir)
            if detail.get("data_prova"):
                r["PROVA_OBJETIVA"] = detail["data_prova"]
            # Novas colunas:
            r["GABARITO_PRELIMINAR"] = detail.get("gabarito_preliminar", "-") or "-"
            r["RESULTADO_FINAL"] = detail.get("resultado_final", "-") or "-"
            # Respeita site; se vier vazio, mantém "-"
        except Exception as e:
            # Se algo falhar no detalhe, não quebra o pipeline
            r["GABARITO_PRELIMINAR"] = "-"
            r["RESULTADO_FINAL"] = "-"
            # opcional: printar erro no stderr
            print(f"[WARN] Falha ao coletar detalhe {edital_url}: {e}", file=sys.stderr)
        # Acesso educado (evita bater forte na origem)
        time.sleep(0.4)

    # 3) Ajuste de virada de ano (após reconciliação da prova)
    for r in rows:
        r["PROVA_OBJETIVA"] = ajustar_ano_prova_pelo_periodo_inscricao(r.get("INSCRIÇÕES", ""), r.get("PROVA_OBJETIVA", ""))

    # 4) Deduplicação (evita linhas repetidas por flutuação na origem)
    seen = set()
    unique_rows = []
    def key_of(x):
        return (
            (x.get("UF") or "").strip().upper(),
            (x.get("INSTITUIÇÃO") or "").strip().upper(),
            (x.get("INSCRIÇÕES") or "").strip(),
            (x.get("PROVA_OBJETIVA") or "").strip(),
        )
    for r in rows:
        k = key_of(r)
        if k in seen:
            continue
        seen.add(k)
        unique_rows.append(r)

    # 5) Ordena pela primeira data detectada na prova (quando houver)
    unique_rows.sort(key=lambda r: (parse_date_br_first(r.get("PROVA_OBJETIVA", "")) or datetime.max))

    # 6) Salva
    Path("data").mkdir(exist_ok=True)
    save_csv(unique_rows, "data/calendario_residencia.csv")
    # Remove campo interno antes do JSON final
    for r in unique_rows:
        r.pop("_EDITAL_URL", None)
    save_json(unique_rows, "data/calendario_residencia.json")
    save_meta()

    print(f"[OK] Registros salvos: {len(unique_rows)} (antes: {len(rows)})")

if __name__ == "__main__":
    main()
