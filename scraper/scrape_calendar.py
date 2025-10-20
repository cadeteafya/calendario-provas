# scraper/scrape_calendar.py
# -*- coding: utf-8 -*-

import sys
import csv
import json
import re
import time
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

URL = "https://med.estrategia.com/portal/residencia-medica/calendario-de-residencia-medica-confira-as-datas-das-proximas-provas/"
HEADERS_WANTED = ["UF", "SELEÇÃO", "INSCRIÇÕES", "PROVA OBJETIVA"]

# ---------- Helpers de rede ----------

def fetch_html(url: str) -> str:
    """GET com cache-buster e headers para evitar CDN cacheada."""
    resp = requests.get(
        url,
        params={"_": int(time.time())},  # cache-buster
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) CalendarioBot/1.0",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.text

# ---------- Parsing da tabela ----------

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
        if score >= 3:  # bom indicativo
            return tb, headers
    return None, None

DATE_RE = re.compile(r"\b(\d{2})/(\d{2})/(\d{4})\b")

def anos_meses_de(texto: str):
    """Retorna lista de (ano, mes) encontrados no texto."""
    out = []
    for d, m, y in DATE_RE.findall(texto or ""):
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
            old = m.group(0)
            new = f"{d:02d}/{mm:02d}/{yy_new}"
            start, end = m.span()
            new_prova = new_prova[:start] + new + new_prova[end:]

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

def parse_table(table, headers_upper):
    """Extrai as colunas desejadas, corrige PROVA quando necessário."""
    idx = {h: headers_upper.index(h) if h in headers_upper else None for h in HEADERS_WANTED}

    rows_out = []
    for tr in table.find("tbody").find_all("tr"):
        tds = tr.find_all(["td", "th"])
        if not tds:
            continue

        def get(h):
            i = idx[h]
            if i is None or i >= len(tds):
                return ""
            return tds[i].get_text(strip=True)

        uf = get("UF")
        selecao = get("SELEÇÃO")
        inscr = get("INSCRIÇÕES")
        prova = get("PROVA OBJETIVA")

        if not uf or not selecao:
            continue

        prova_corrigida = ajustar_ano_prova_pelo_periodo_inscricao(inscr, prova)

        rows_out.append({
            "UF": uf,
            "INSTITUIÇÃO": selecao,     # renomeado
            "INSCRIÇÕES": inscr,
            "PROVA_OBJETIVA": prova_corrigida
        })
    return rows_out

# ---------- Persistência ----------

def save_csv(data, path_csv):
    keys = ["UF", "INSTITUIÇÃO", "INSCRIÇÕES", "PROVA_OBJETIVA"]
    with open(path_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(data)

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

# ---------- Main ----------

def main():
    html = fetch_html(URL)
    soup = BeautifulSoup(html, "lxml")

    table, headers = find_calendar_table(soup)
    if not table:
        print("[ERRO] Tabela do calendário não encontrada.", file=sys.stderr)
        sys.exit(1)

    rows = parse_table(table, headers_upper=[h.upper() for h in headers])

    # Ordena pela 1ª data da PROVA (quando houver)
    rows.sort(key=lambda r: (parse_date_br_first(r["PROVA_OBJETIVA"]) or datetime.max))

    Path("data").mkdir(exist_ok=True)
    save_csv(rows, "data/calendario_residencia.csv")
    save_json(rows, "data/calendario_residencia.json")
    save_meta()

    print(f"[OK] Registros salvos: {len(rows)}")

if __name__ == "__main__":
    main()
