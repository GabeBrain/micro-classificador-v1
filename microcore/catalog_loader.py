import re
import urllib.parse
import pandas as pd
import requests
from .utils import norm_text
import unicodedata


GSHEETS_CSV_TPL = "https://docs.google.com/spreadsheets/d/1egrGImJrXqfvxa7U4QirKrePE7w8QtuOG8Jc_H_AsJE/edit?usp=sharing"

# ---------- helpers de robustez ----------
def _strip_accents(s: str) -> str:
    return "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))

def _header_key(colname: str) -> str:
    """
    Normaliza cabeçalho para matching:
    - lower
    - remove acentos
    - troca qualquer não alfanum por espaço
    - colapsa espaços
    """
    if not isinstance(colname, str):
        colname = str(colname)
    s = _strip_accents(colname).lower().strip()
    s = re.sub(r"[^a-z0-9]+", " ", s)  # underscores, hífens, etc. viram espaço
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _find_column(df: pd.DataFrame, targets: set[str]) -> str | None:
    """
    Procura uma coluna cujo header normalizado (_header_key) caia em 'targets'
    Retorna o nome original da coluna (case-preserving) se achar.
    """
    keys = {c: _header_key(c) for c in df.columns}
    for original, key in keys.items():
        if key in targets:
            return original
    return None

# conjuntos-alvo aceitando variações
ORIG_TARGETS = {
    "subcat original", "subcategoria original", "subcat", "sub categoria original", "sub categoria",
    "subcat original", "sub cat original", "subcategoria", "sub cat"
}
NOVA_TARGETS = {
    "nova subcat", "nova subcategoria", "nova", "nova sub categoria", "nova sub cat"
}

def _extract_sheet_id(sheet_url: str) -> str:
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", sheet_url)
    if not m:
        raise ValueError("URL do Google Sheets inválida. Ex.: https://docs.google.com/spreadsheets/d/<ID>/edit")
    return m.group(1)

def _load_single_tab_csv(sheet_id: str, sheet_name: str) -> pd.DataFrame:
    url = GSHEETS_CSV_TPL.format(sid=sheet_id, sheet=urllib.parse.quote(sheet_name))
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    # Usa pandas para ler o CSV em memória
    df = pd.read_csv(pd.io.common.StringIO(r.text))
    df["_tab_name"] = sheet_name
    return df

def load_mapping_gsheets(sheet_url: str, tab_names: list[str]) -> pd.DataFrame:
    """
    Carrega múltiplas abas públicas do Google Sheets e consolida:
    - Espera colunas equivalentes a 'SubCat Original' e 'Nova SubCat'
    - Adiciona 'categoria_oficial' (nome da aba)
    - Cria chaves normalizadas: k_original, k_nova, k_categoria
    """
    sid = _extract_sheet_id(sheet_url)
    frames = []
    for tab in tab_names:
        df = _load_single_tab_csv(sid, tab)

        # localizar colunas com robustez
        col_orig = _find_column(df, ORIG_TARGETS)
        col_new  = _find_column(df, NOVA_TARGETS)

        if not col_orig or not col_new:
            # mensagem amigável com as colunas detectadas
            cols_seen = ", ".join(df.columns.astype(str).tolist())
            raise ValueError(
                f"Aba '{tab}' precisa ter colunas equivalentes a 'SubCat Original' e 'Nova SubCat'. "
                f"Colunas encontradas: [{cols_seen}]"
            )

        df = df.rename(columns={col_orig: "SubCat Original", col_new: "Nova SubCat"}).copy()
        df["categoria_oficial"] = tab
        frames.append(df[["SubCat Original","Nova SubCat","categoria_oficial"]])

    cat = pd.concat(frames, ignore_index=True)

    # chaves normalizadas (para joins e matching)
    cat["k_original"]  = cat["SubCat Original"].astype(str).map(norm_text)
    cat["k_nova"]      = cat["Nova SubCat"].astype(str).map(norm_text)
    cat["k_categoria"] = cat["categoria_oficial"].astype(str).map(norm_text)
    return cat


