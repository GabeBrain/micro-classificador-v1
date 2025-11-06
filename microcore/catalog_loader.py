
import urllib.parse
import pandas as pd
import requests
from .utils import norm_text

# URL base (substitua o ID pela sua planilha real)
GSHEETS_BASE = "https://docs.google.com/spreadsheets/d/1egrGImJrXqfvxa7U4QirKrePE7w8QtuOG8Jc_H_AsJE/edit?usp=sharing"

TABS_DEFAULT = [
    "Alimentação","Automotivo","Serviços","Decoração","Moda",
    "Educação","Inst. Financeira","Saúde e Bem Estar","Outros"
]

def load_mapping_gsheets() -> pd.DataFrame:
    """Lê todas as abas padrão do Google Sheets público e devolve DF consolidado."""
    frames = []
    for tab in TABS_DEFAULT:
        url = GSHEETS_BASE.format(sheet=urllib.parse.quote(tab))
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        df = pd.read_csv(pd.io.common.StringIO(r.text))
        df = df.rename(columns={
            "SubCat_Original": "SubCat Original",
            "Nova_SubCat": "Nova SubCat"
        })
        df["categoria_oficial"] = tab
        frames.append(df[["SubCat Original","Nova SubCat","categoria_oficial"]])

    cat = pd.concat(frames, ignore_index=True)
    cat["k_original"] = cat["SubCat Original"].astype(str).map(norm_text)
    cat["k_nova"] = cat["Nova SubCat"].astype(str).map(norm_text)
    cat["k_categoria"] = cat["categoria_oficial"].astype(str).map(norm_text)
    return cat


