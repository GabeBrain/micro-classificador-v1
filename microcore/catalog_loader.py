import re
import urllib.parse
import pandas as pd
import requests
from .utils import norm_text, safe_lower

GSHEETS_CSV_TPL = "https://docs.google.com/spreadsheets/d/1egrGImJrXqfvxa7U4QirKrePE7w8QtuOG8Jc_H_AsJE/edit?usp=sharing"

def _extract_sheet_id(sheet_url: str) -> str:
    """
    Aceita URLs como:
    https://docs.google.com/spreadsheets/d/<SID>/edit#gid=0
    """
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", sheet_url)
    if not m:
        raise ValueError("URL do Google Sheets inválida. Ex.: https://docs.google.com/spreadsheets/d/<ID>/edit")
    return m.group(1)

def _load_single_tab_csv(sheet_id: str, sheet_name: str) -> pd.DataFrame:
    url = GSHEETS_CSV_TPL.format(sid=sheet_id, sheet=urllib.parse.quote(sheet_name))
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    df = pd.read_csv(pd.io.common.StringIO(r.text))
    df["_tab_name"] = sheet_name  # mantém de onde veio
    return df

def load_mapping_gsheets(sheet_url: str, tab_names: list[str]) -> pd.DataFrame:
    """
    Carrega múltiplas abas públicas do Google Sheets e consolida:
    - Espera colunas: 'SubCat Original' e 'Nova SubCat' (ou equivalentes)
    - Adiciona 'categoria_oficial' (nome da aba)
    - Cria chaves normalizadas: k_original, k_nova
    """
    sid = _extract_sheet_id(sheet_url)
    frames = []
    for tab in tab_names:
        try:
            df = _load_single_tab_csv(sid, tab)
        except Exception as e:
            raise RuntimeError(f"Falha ao carregar a aba '{tab}': {e}") from e

        # mapear nomes de colunas prováveis
        col_orig = next((c for c in df.columns if safe_lower(c) in {
            "subcat original","subcat_original","subcat","subcategoria original","subcategoria_original"
        }), None)
        col_new  = next((c for c in df.columns if safe_lower(c) in {
            "nova subcat","nova_subcat","nova subcategoria","nova_subcategoria","nova"
        }), None)

        if not col_orig or not col_new:
            raise ValueError(f"Aba '{tab}' precisa ter colunas 'SubCat Original' e 'Nova SubCat' (ou equivalentes).")

        df = df.rename(columns={col_orig: "SubCat Original", col_new: "Nova SubCat"}).copy()
        df["categoria_oficial"] = tab  # guard-rail: subcat desta aba pertence a esta categoria
        frames.append(df[["SubCat Original","Nova SubCat","categoria_oficial"]])

    cat = pd.concat(frames, ignore_index=True)
    # chaves normalizadas
    cat["k_original"] = cat["SubCat Original"].astype(str).map(norm_text)
    cat["k_nova"]     = cat["Nova SubCat"].astype(str).map(norm_text)
    cat["k_categoria"] = cat["categoria_oficial"].astype(str).map(norm_text)
    return cat

# Carregador local antigo (continua disponível se quiser usar XLSX local)
def load_mapping_xlsx(path: str) -> pd.DataFrame:
    df = pd.read_excel(path, engine="openpyxl")
    col_orig = next((c for c in df.columns if safe_lower(c) in {"subcat original","subcat_original","subcat","subcategoria original"}), None)
    col_new  = next((c for c in df.columns if safe_lower(c) in {"nova subcat","nova_subcat","nova subcategoria","nova_subcategoria","nova"}), None)
    
    if not col_orig or not col_new:
        raise ValueError("Planilha de mapeamento deve conter colunas 'SubCat Original' e 'Nova SubCat' (ou equivalentes).")
    
    df = df.rename(columns={col_orig: "SubCat Original", col_new: "Nova SubCat"}).copy()
    df["categoria_oficial"] = "CATALOGO_LOCAL"
    df["k_original"] = df["SubCat Original"].astype(str).map(norm_text)
    df["k_nova"]     = df["Nova SubCat"].astype(str).map(norm_text)
    df["k_categoria"]= df["categoria_oficial"].astype(str).map(norm_text)
    return df[["SubCat Original","Nova SubCat","categoria_oficial","k_original","k_nova","k_categoria"]]
