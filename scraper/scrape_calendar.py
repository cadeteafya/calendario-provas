import requests
from bs4 import BeautifulSoup
import csv
import json
from datetime import datetime

URL = "https://med.estrategia.com/portal/residencia-medica/calendario-de-residencia-medica-confira-as-datas-das-proximas-provas/"

def fetch_html(url):
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.text

def parse_table(html):
    soup = BeautifulSoup(html, "html.parser")
    # localizar a tabela (há apenas uma tabela na página com o calendário)
    table = soup.find("table")
    if not table:
        raise RuntimeError("Não encontrou <table> na página")
    # extrair cabeçalhos
    headers = [th.get_text(strip=True) for th in table.find("thead").find_all("th")]
    # extrações das linhas
    rows = []
    for tr in table.find("tbody").find_all("tr"):
        cols = [td.get_text(strip=True) for td in tr.find_all("td")]
        if len(cols) != len(headers):
            # pular linhas inválidas
            continue
        row = dict(zip(headers, cols))
        rows.append(row)
    return rows

def transform_rows(rows):
    output = []
    for row in rows:
        uf = row.get("UF", "").strip()
        selecao = row.get("SELEÇÃO", "").strip()
        inscricoes = row.get("INSCRIÇÕES", "").strip()
        prova = row.get("PROVA OBJETIVA", "").strip()
        # apenas linhar dados não vazios
        if not uf or not selecao:
            continue
        rec = {
            "UF": uf,
            "INSTITUIÇÃO": selecao,
            "INSCRIÇÕES": inscricoes,
            "PROVA_OBJETIVA": prova
        }
        output.append(rec)
    return output

def save_csv(data, path_csv):
    keys = ["UF", "INSTITUIÇÃO", "INSCRIÇÕES", "PROVA_OBJETIVA"]
    with open(path_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for item in data:
            writer.writerow(item)

def save_json(data, path_json):
    with open(path_json, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def main():
    html = fetch_html(URL)
    rows = parse_table(html)
    data = transform_rows(rows)
    # opcional: ordenar por data de prova (convertendo para objeto datetime)
    def parse_date(dt_str):
        try:
            return datetime.strptime(dt_str, "%d/%m/%Y")
        except Exception:
            return None
    data_sorted = sorted(data, key=lambda r: (parse_date(r["PROVA_OBJETIVA"]) or datetime.max))
    # salvar
    save_csv(data_sorted, "data/calendario_residencia.csv")
    save_json(data_sorted, "data/calendario_residencia.json")
    print(f"Salvo {len(data_sorted)} registros.")

if __name__ == "__main__":
    main()
