
import urllib.parse
import pandas as pd
import requests
from .utils import norm_text

#  ID sheets (entre /d/ e /edit)
SHEET_ID = "1egrGImJrXqfvxa7U4QirKrePE7w8QtuOG8Jc_H_AsJE"

TABS_DEFAULT = [
    "Alimentação","Automotivo","Serviços","Decoração","Moda",
    "Educação","Inst. Financeira","Saúde e Bem Estar","Outros"
]

def _csv_url(sheet_name: str) -> str:
    return (
        "https://docs.google.com/spreadsheets/d/"
        f"{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={urllib.parse.quote(sheet_name)}"
    )

def load_mapping_gsheets() -> pd.DataFrame:
    frames = []
    for tab in TABS_DEFAULT:
        url = _csv_url(tab)
        r = requests.get(url, timeout=30)
        r.raise_for_status()

        # Se vier HTML, falha de permissão ou endpoint -> mensagem clara
        ctype = r.headers.get("Content-Type","")
        if "text/csv" not in ctype and not r.text.lstrip().startswith(("SubCat_Original","\"SubCat_Original")):
            snippet = r.text[:300].replace("\n"," ")
            raise RuntimeError(
                f"Aba '{tab}' não retornou CSV (tipo={ctype}). "
                "Verifique se a planilha está pública (Viewer). "
                f"Primeiros 300 chars: {snippet}"
            )

        df = pd.read_csv(pd.io.common.StringIO(r.text))
        df = df.rename(columns={"SubCat_Original":"SubCat Original","Nova_SubCat":"Nova SubCat"})
        df["categoria_oficial"] = tab
        frames.append(df[["SubCat Original","Nova SubCat","categoria_oficial"]])

    cat = pd.concat(frames, ignore_index=True)
    cat["k_original"]  = cat["SubCat Original"].astype(str).map(norm_text)
    cat["k_nova"]      = cat["Nova SubCat"].astype(str).map(norm_text)
    cat["k_categoria"] = cat["categoria_oficial"].astype(str).map(norm_text)
    return cat



