
import urllib.parse
import pandas as pd
import requests
from .utils import norm_text


SHEET_ID = "1egrGImJrXqfvxa7U4QirKrePE7w8QtuOG8Jc_H_AsJE"

TABS_DEFAULT = [
    "Alimentação","Automotivo","Serviços","Decoração","Moda",
    "Educação","Inst. Financeira","Saúde e Bem Estar","Outros"
]



def load_mapping_gsheets() -> pd.DataFrame:
    frames = []
    for tab in TABS_DEFAULT:
        url = (
            f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq"
            f"?tqx=out:csv&sheet={urllib.parse.quote(tab)}"
        )
        r = requests.get(url, timeout=30)
        if r.status_code != 200:
            raise RuntimeError(f"Erro HTTP {r.status_code} ao carregar aba '{tab}'")
        text = r.text
        if text.strip().startswith("<!DOCTYPE html>"):
            raise RuntimeError(
                f"Aba '{tab}' retornou HTML em vez de CSV. "
                f"Verifique o nome exato da aba (acentos, espaços) e tente abrir manualmente:\n{url}"
            )
        df = pd.read_csv(pd.io.common.StringIO(text))
        df = df.rename(columns={"SubCat_Original": "SubCat Original", "Nova_SubCat": "Nova SubCat"})
        df["categoria_oficial"] = tab
        frames.append(df[["SubCat Original", "Nova SubCat", "categoria_oficial"]])

    cat = pd.concat(frames, ignore_index=True)
    cat["k_original"] = cat["SubCat Original"].astype(str).map(norm_text)
    cat["k_nova"] = cat["Nova SubCat"].astype(str).map(norm_text)
    cat["k_categoria"] = cat["categoria_oficial"].astype(str).map(norm_text)
    return cat
