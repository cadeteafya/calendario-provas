# scraper/scrape_calendar.py
import csv, json, sys
from datetime import datetime
import requests
from bs4 import BeautifulSoup

from datetime import datetime
from zoneinfo import ZoneInfo
import json, os
from pathlib import Path


URL = "https://med.estrategia.com/portal/residencia-medica/calendario-de-residencia-medica-confira-as-datas-das-proximas-provas/"

HEADERS_WANTED = ["UF", "SELEÇÃO", "INSCRIÇÕES", "PROVA OBJETIVA"]

def fetch_html(url: str) -> str:
    resp = requests.get(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) CalendarioBot/1.0"
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.text

def find_calendar_table(soup: BeautifulSoup):
    tables = soup.find_all("table")
    # escolhe a tabela cujo thead contém pelo menos UF e SELEÇÃO
    for tb in tables:
        thead = tb.find("thead")
        tbody = tb.find("tbody")
        if not thead or not tbody:
            continue
        headers = [th.get_text(strip=True).upper() for th in thead.find_all("th")]
        score = sum(1 for h in HEADERS_WANTED if h in headers)
        if score >= 3:  # bom indicativo de ser a tabela certa
            return tb, headers
    return None, None

def parse_table(table, headers_upper):
    # cria um índice para cada coluna que nos interessa
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

        rows_out.append({
            "UF": uf,
            "INSTITUIÇÃO": selecao,           # renomeado
            "INSCRIÇÕES": inscr,
            "PROVA_OBJETIVA": prova
        })
    return rows_out

def parse_date_br(s: str):
    # tenta achar um dd/mm/yyyy dentro do texto (ex.: '09/10/2025', '09/10/2025 a 20/10/2025')
    import re
    m = re.search(r"\b(\d{2})/(\d{2})/(\d{4})\b", s)
    if not m:
        return None
    d, mth, y = map(int, m.groups())
    try:
        return datetime(y, mth, d)
    except ValueError:
        return None

def save_csv(data, path_csv):
    keys = ["UF", "INSTITUIÇÃO", "INSCRIÇÕES", "PROVA_OBJETIVA"]
    with open(path_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(data)

def save_json(data, path_json):
    with open(path_json, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

Path("data").mkdir(exist_ok=True)

# horário da última atualização em Brasília (BRT)
hoje_brt = datetime.now(ZoneInfo("America/Sao_Paulo")).date()
meta_dt = datetime(hoje_brt.year, hoje_brt.month, hoje_brt.day, 7, 0, tzinfo=ZoneInfo("America/Sao_Paulo"))
meta = {"last_update_brt": meta_dt.strftime("%d/%m/%Y %H:%M")}

with open("data/meta.json", "w", encoding="utf-8") as f:
    json.dump(meta, f, ensure_ascii=False, indent=2)


def main():
    html = fetch_html(URL)
    soup = BeautifulSoup(html, "lxml")

    table, headers = find_calendar_table(soup)
    if not table:
        print("[ERRO] Tabela do calendário não encontrada.", file=sys.stderr)
        sys.exit(1)

    rows = parse_table(table, headers_upper=[h.upper() for h in headers])

    # ordena pela primeira data detectada em PROVA_OBJETIVA (quando houver)
    rows.sort(key=lambda r: parse_date_br(r["PROVA_OBJETIVA"]) or datetime.max)

    # garante que os arquivos vão para /data
    from pathlib import Path
    Path("data").mkdir(exist_ok=True)

    save_csv(rows, "data/calendario_residencia.csv")
    save_json(rows, "data/calendario_residencia.json")
    print(f"[OK] Registros salvos: {len(rows)}")

if __name__ == "__main__":
    main()
