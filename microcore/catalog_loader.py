import pandas as pd
from .utils import norm_text, safe_lower

def load_mapping_xlsx(path: str) -> pd.DataFrame:
    """
    Espera colunas: 'SubCat Original', 'Nova SubCat'
    Retorna DF com colunas auxiliares normalizadas.
    """
    df = pd.read_excel(path, engine="openpyxl")
    # normalizar cabeçalhos comuns
    cols = {c.lower().strip(): c for c in df.columns}
    # tentar mapear nomes prováveis
    col_orig = next((c for c in df.columns if safe_lower(c) in {"subcat original","subcat_original","subcat", "subcategoria original"}), None)
    col_new  = next((c for c in df.columns if safe_lower(c) in {"nova subcat","nova_subcat","nova subcategoria","nova_subcategoria","nova"}), None)

    if not col_orig or not col_new:
        raise ValueError("Planilha de mapeamento deve conter colunas 'SubCat Original' e 'Nova SubCat' (ou equivalentes).")

    df = df.rename(columns={col_orig: "SubCat Original", col_new: "Nova SubCat"}).copy()

    # criar chaves normalizadas
    df["k_original"] = df["SubCat Original"].astype(str).map(norm_text)
    df["k_nova"] = df["Nova SubCat"].astype(str).map(norm_text)
    return df[["SubCat Original","Nova SubCat","k_original","k_nova"]]
