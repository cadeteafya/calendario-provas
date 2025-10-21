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

# ------------------ Rede (com retries) ------------------

def _req_get(url, params=None, timeout=30, tries=4, base_sleep=0.8):
    last_err = None
    for i in range(tries):
        try:
            resp = requests.get(
                url,
                params=params,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) CalendarioBot/1.4",
                    "Cache-Control": "no-cache",
                    "Pragma": "no-cache",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                },
                timeout=timeout,
            )
            if resp.status_code >= 500:
                raise requests.HTTPError(f"HTTP {resp.status_code}")
            resp.raise_for_status()
            return resp
        except Exception as e:
            last_err = e
            time.sleep(base_sleep * (2 ** i))
    raise last_err

def fetch_html(url: str) -> str:
    return _req_get(url, params={"_": int(time.time())}).text

# ------------------ Utils ------------------

DATE_RE = re.compile(r"\b(\d{2})/(\d{2})/(\d{4})\b")

def normalize_text(s: str) -> str:
    s = (s or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"\s+", " ", s)
    return s

def has_date(s: str) -> bool:
    return bool(DATE_RE.search(s or ""))

def anos_meses_de(texto: str):
    out = []
    for _d, m, y in DATE_RE.findall(texto or ""):
        out.append((int(y), int(m)))
    return out

def ajustar_ano_prova_pelo_periodo_inscricao(inscricoes: str, prova: str) -> str:
    if not prova:
        return prova
    ins = anos_meses_de(inscricoes)
    prv = list(DATE_RE.finditer(prova))
    if not ins or not prv:
        return prova
    anos_q4 = {y for (y, m) in ins if m >= 10}
    new_prova = prova
    for m in reversed(prv):
        d, mm, yy = map(int, m.groups())
        if yy in anos_q4 and 1 <= mm <= 3:
            yy_new = yy + 1
            start, end = m.span()
            new_prova = new_prova[:start] + f"{d:02d}/{mm:02d}/{yy_new}" + new_prova[end:]
    return new_prova

def parse_date_br_first(s: str):
    m = DATE_RE.search(s or "")
    if not m:
        return None
    d, mth, y = map(int, m.groups())
    try:
        return datetime(y, mth, d)
    except ValueError:
        return None

# ------------------ Parsing tabela principal (robusto) ------------------

ALIASES = {
    "uf": {"uf"},
    "selecao": {"selecao", "selecao/instituicao", "instituicao", "instituicao/seleção", "selecao - instituicao"},
    "inscricoes": {"inscricoes", "inscricao", "periodo de inscricoes"},
    "prova": {"prova objetiva", "data da prova", "prova", "data prova"},
    "edital": {"edital", "link do edital"},
}

def header_key(h_txt: str):
    h = normalize_text(h_txt)
    for k, opts in ALIASES.items():
        for opt in opts:
            if opt in h:
                return k
    return None

def find_calendar_table(soup: BeautifulSoup):
    candidates = []
    for tb in soup.find_all("table"):
        thead = tb.find("thead")
        tbody = tb.find("tbody")
        if not tbody:
            continue
        headers = []
        if thead:
            headers = [th.get_text(strip=True) for th in thead.find_all("th")]
        else:
            first_tr = tb.find("tr")
            if first_tr:
                headers = [td.get_text(strip=True) for td in first_tr.find_all(["td", "th"])]
        keys = set(filter(None, (header_key(h) for h in headers)))
        score = len({"uf", "selecao", "inscricoes", "prova"} & keys)
        if score >= 3:
            candidates.append((score, tb, headers))
    if not candidates:
        return None, []
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1], candidates[0][2]

def _get_link(tag):
    a = tag.find("a", href=True)
    return a["href"].strip() if a else None

def parse_main_table(table, headers_texts):
    header_map = {}
    if headers_texts:
        for idx, h in enumerate(headers_texts):
            k = header_key(h)
            if k and k not in header_map:
                header_map[k] = idx

    rows_out = []
    body_rows = table.find_all("tr")
    if table.find("thead") and body_rows:
        body_rows = body_rows[1:]  # pula cabeçalho

    for tr in body_rows:
        tds = tr.find_all(["td", "th"])
        if not tds or len(tds) < 3:
            continue

        def col_text(k):
            i = header_map.get(k)
            if i is None or i >= len(tds):
                return ""
            return tds[i].get_text(strip=True)

        uf = col_text("uf")
        selecao = col_text("selecao") or tds[1].get_text(strip=True)
        inscr = col_text("inscricoes")
        prova = col_text("prova")

        # Captura do link do edital (prioridade: EDITAL > INSTITUIÇÃO > 1º link da linha)
        edital_url = None
        i_edital = header_map.get("edital")
        if i_edital is not None and i_edital < len(tds):
            edital_url = _get_link(tds[i_edital])
        if not edital_url:
            i_sel = header_map.get("selecao", 1)
            if i_sel is not None and i_sel < len(tds):
                edital_url = _get_link(tds[i_sel])
        if not edital_url:
            edital_url = _get_link(tr)

        if not (uf and selecao):
            continue

        rows_out.append({
            "UF": uf,
            "INSTITUIÇÃO": selecao,
            "INSCRIÇÕES": inscr,
            "PROVA_OBJETIVA": prova,
            "_EDITAL_URL": edital_url,
        })
    return rows_out

# ------------------ Parsing página de edital ------------------

def _extract_detail_from_soup(soup: BeautifulSoup) -> dict:
    out = {}

    def accept_data_da_prova(label_norm: str) -> bool:
        # Aceita APENAS os rótulos corretos para data da prova
        return label_norm in {"data da prova", "prova objetiva", "data prova"}

    def accept_gabarito(label_norm: str) -> bool:
        # precisa conter "gabarito" e "preliminar"
        return "gabarito" in label_norm and "preliminar" in label_norm

    def accept_resultado(label_norm: str) -> bool:
        # aceita "resultado final" OU "classificacao final"
        return ("resultado final" in label_norm) or ("classificacao final" in label_norm)

    # 1) Tabelas
    for tb in soup.find_all("table"):
        for tr in tb.find_all("tr"):
            tds = tr.find_all(["td", "th"])
            if len(tds) < 2:
                continue
            label_norm = normalize_text(tds[0].get_text(" ", strip=True))
            value = tds[1].get_text(" ", strip=True)

            if accept_data_da_prova(label_norm):
                # Só aceita se houver ao menos uma data
                if has_date(value):
                    out["data_prova"] = value
            elif accept_gabarito(label_norm):
                if has_date(value):
                    out["gabarito_preliminar"] = value
            elif accept_resultado(label_norm):
                if has_date(value):
                    out["resultado_final"] = value

    # 2) <dl> (algumas páginas usam esse formato)
    if not any(k in out for k in ("data_prova", "gabarito_preliminar", "resultado_final")):
        for dl in soup.find_all("dl"):
            dts = dl.find_all("dt")
            dds = dl.find_all("dd")
            for dt, dd in zip(dts, dds):
                label_norm = normalize_text(dt.get_text(" ", strip=True))
                value = dd.get_text(" ", strip=True)

                if accept_data_da_prova(label_norm):
                    if has_date(value):
                        out["data_prova"] = value
                elif accept_gabarito(label_norm):
                    if has_date(value):
                        out["gabarito_preliminar"] = value
                elif accept_resultado(label_norm):
                    if has_date(value):
                        out["resultado_final"] = value

    return out

def parse_detail_page(edital_url: str) -> dict:
    if not edital_url:
        return {}
    base = edital_url if edital_url.startswith("http") else urljoin(URL, edital_url)
    variants = [
        base,
        base + "/" if not base.endswith("/") else base,
        (base + ("&" if "?" in base else "?") + "amp") if "amp" not in base else base,
        base.rstrip("/") + "/amp/",
    ]
    out = {}
    for u in variants:
        try:
            html = fetch_html(u)
            soup = BeautifulSoup(html, "lxml")
            out = _extract_detail_from_soup(soup)
            if any(k in out for k in ("data_prova", "gabarito_preliminar", "resultado_final")):
                break
        except Exception:
            continue
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
    Path("data").mkdir(exist_ok=True)
    hoje_brt = datetime.now(ZoneInfo("America/Sao_Paulo")).date()
    meta_dt = datetime(hoje_brt.year, hoje_brt.month, hoje_brt.day, 7, 0, tzinfo=ZoneInfo("America/Sao_Paulo"))
    meta = {"last_update_brt": meta_dt.strftime("%d/%m/%Y %H:%M")}
    with open("data/meta.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

# ------------------ Main ------------------

def main():
    # 1) Página principal
    try:
        html = fetch_html(URL)
    except Exception as e:
        print(f"[ERRO] Falha ao baixar página principal: {e}", file=sys.stderr)
        html = ""

    soup = BeautifulSoup(html or "", "lxml")
    table, headers = find_calendar_table(soup)
    if not table:
        title = soup.title.get_text(strip=True) if soup and soup.title else "(sem título)"
        print(f"[ERRO] Tabela do calendário não encontrada. HTML title: {title}", file=sys.stderr)
        rows = []
    else:
        rows = parse_main_table(table, headers_texts=headers)

    # 2) Detalhes por edital
    for r in rows:
        edital_url = r.get("_EDITAL_URL")
        if not edital_url:
            r["GABARITO_PRELIMINAR"] = "-"
            r["RESULTADO_FINAL"] = "-"
            continue
        try:
            detail = parse_detail_page(edital_url)

            # (a) Reconciliação de "Data da Prova": só substitui se detalhe tiver data válida
            detail_dp = detail.get("data_prova")
            if detail_dp and has_date(detail_dp):
                r["PROVA_OBJETIVA"] = detail_dp
            # (b) Novos campos: só aceita se tiver data
            r["GABARITO_PRELIMINAR"] = detail.get("gabarito_preliminar") if has_date(detail.get("gabarito_preliminar", "")) else "-"
            r["RESULTADO_FINAL"] = detail.get("resultado_final") if has_date(detail.get("resultado_final", "")) else "-"
        except Exception as e:
            r["GABARITO_PRELIMINAR"] = "-"
            r["RESULTADO_FINAL"] = "-"
            print(f"[WARN] Falha ao detalhar {edital_url}: {e}", file=sys.stderr)
        time.sleep(0.5)  # gentil com a origem

    # 3) Ajuste de virada de ano (após reconciliação)
    for r in rows:
        r["PROVA_OBJETIVA"] = ajustar_ano_prova_pelo_periodo_inscricao(r.get("INSCRIÇÕES", ""), r.get("PROVA_OBJETIVA", ""))

    # 4) Dedup
    seen = set()
    def key_of(x):
        return (
            (x.get("UF") or "").strip().upper(),
            (x.get("INSTITUIÇÃO") or "").strip().upper(),
            (x.get("INSCRIÇÕES") or "").strip(),
            (x.get("PROVA_OBJETIVA") or "").strip(),
        )
    unique_rows = []
    for r in rows:
        k = key_of(r)
        if k in seen:
            continue
        seen.add(k)
        unique_rows.append(r)

    # 5) Ordenação
    unique_rows.sort(key=lambda r: (parse_date_br_first(r.get("PROVA_OBJETIVA", "")) or datetime.max))

    # 6) Salvar
    Path("data").mkdir(exist_ok=True)
    save_csv(unique_rows, "data/calendario_residencia.csv")
    for r in unique_rows:
        r.pop("_EDITAL_URL", None)
    save_json(unique_rows, "data/calendario_residencia.json")
    save_meta()
    print(f"[OK] Registros salvos: {len(unique_rows)} (antes: {len(rows)})")

if __name__ == "__main__":
    main()
