from typing import Tuple
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from rapidfuzz import fuzz
from .utils import norm_text

def _build_tfidf_index(target_terms: list[str]):
    vec = TfidfVectorizer(ngram_range=(1,2), min_df=1)
    X = vec.fit_transform(target_terms)
    return vec, X

def _semantic_match(query: str, vec, X, vocab_terms: list[str]) -> Tuple[str, float]:
    q = [query]
    qX = vec.transform(q)
    sims = cosine_similarity(qX, X).ravel()
    if sims.size == 0:
        return "", 0.0
    i = int(np.argmax(sims))
    return vocab_terms[i], float(sims[i])

def process_dataframe(df_in: pd.DataFrame,
                      mapping_df: pd.DataFrame,
                      hi_threshold: float = 0.90,
                      lo_threshold: float = 0.70,
                      text_columns: Tuple[str,...] = ("Sub-Categoria","Nome","Endereço")) -> Tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    df_in: DataFrame do arquivo original
    mapping_df: DF do catálogo (SubCat Original -> Nova SubCat), com colunas 'SubCat Original','Nova SubCat','k_original','k_nova'
    Retorna: (df_final_filtrado, df_baixa_confianca, métricas)
    """
    df = df_in.copy()

    # colunas essenciais protegidas
    for col in ["Categoria","Sub-Categoria","Nome","Endereço"]:
        if col not in df.columns:
            df[col] = ""

    # 1) matching determinístico por Sub-Categoria
    # chave normalizada da entrada
    df["_k_subcat"] = df["Sub-Categoria"].astype(str).map(norm_text)

    # left-join com mapeamento
    df = df.merge(
        mapping_df[["k_original","Nova SubCat"]],
        left_on="_k_subcat", right_on="k_original", how="left"
    )
    # se houver mapeamento determinístico, aplica
    det_mask = df["Nova SubCat"].notna()
    df.loc[det_mask, "acao"] = np.where(df.loc[det_mask, "Nova SubCat"].str.strip().str.lower()=="excluir", "Excluir", "Corrigir")
    df.loc[det_mask, "Sub-Categoria"] = df.loc[det_mask, "Nova SubCat"]
    df.loc[det_mask, "fonte"] = "catalogo"
    df.loc[det_mask, "confianca"] = 0.99

    # 2) para os não mapeados, tentar determinístico por contains no Nome/Endereço (usando chaves do mapeamento)
    # cria um índice simples de termos originais
    not_mapped = df["acao"].isna()
    # dicionário de contains: 'k_original' -> 'Nova SubCat'
    # (heurística: apenas entradas com 2+ caracteres)
    contains_pairs = [(row.k_original, row["Nova SubCat"]) for _, row in mapping_df.iterrows() if len(row.k_original) >= 2]

    def _contains_rule(row):
        if not_mapped[row.name]:
            hay = " ".join([str(row.get(c, "")) for c in ("Nome","Endereço")])
            hay_n = norm_text(hay)
            for k, nova in contains_pairs:
                if k and k in hay_n:
                    return nova
        return None

    df["_nova_contains"] = df.apply(_contains_rule, axis=1)
    cont_mask = not_mapped & df["_nova_contains"].notna()
    df.loc[cont_mask, "Sub-Categoria"] = df.loc[cont_mask, "_nova_contains"]
    df.loc[cont_mask, "acao"] = np.where(df.loc[cont_mask, "_nova_contains"].str.strip().str.lower()=="excluir", "Excluir", "Corrigir")
    df.loc[cont_mask, "fonte"] = "catalogo-contains"
    df.loc[cont_mask, "confianca"] = 0.92

    # 3) Semântico leve (TF-IDF) — somente para quem segue sem ação
    pending = df["acao"].isna()
    pending_idx = df.index[pending].tolist()

    # universo de destino = "Nova SubCat" únicas válidas (inclui "Excluir")
    target_terms = sorted(mapping_df["Nova SubCat"].astype(str).map(norm_text).unique().tolist())
    vec, X = _build_tfidf_index(target_terms)

    preds = []
    for i in pending_idx:
        # texto de consulta: Sub-Categoria (se veio) + Nome + Endereço
        bag = []
        for c in text_columns:
            bag.append(str(df.at[i, c]))
        q = norm_text(" ".join(bag))
        if not q.strip():
            preds.append(("", 0.0))
            continue

        # ajuda extra: fuzzy com o valor de Sub-Categoria original, se existir
        base_sc = norm_text(str(df.at[i, "Sub-Categoria"]))
        if base_sc:
            score_fz = fuzz.token_set_ratio(base_sc, " ".join(target_terms))/100.0
            # não usamos diretamente, apenas para casos extremados
        pred, sim = _semantic_match(q, vec, X, target_terms)
        preds.append((pred, sim))

    for (i, (pred, sim)) in zip(pending_idx, preds):
        if sim >= lo_threshold:
            human_pred = pred  # já está normalizado; vamos recuperar forma “bonita”
            # recuperar casing original a partir do mapping_df
            row_match = mapping_df.loc[mapping_df["k_nova"] == pred]
            if not row_match.empty:
                human_pred = row_match.iloc[0]["Nova SubCat"]

            df.at[i, "Sub-Categoria"] = human_pred
            df.at[i, "acao"] = "Inferir"
            df.at[i, "fonte"] = "semantico"
            df.at[i, "confianca"] = round(float(sim), 4)
        else:
            df.at[i, "acao"] = "Manter"
            df.at[i, "fonte"] = "nenhum"
            df.at[i, "confianca"] = round(float(sim), 4)

    # 4) “Excluir” -> tirar do resultado final (mas contamos nas métricas)
    excl_mask = df["Sub-Categoria"].astype(str).str.strip().str.lower().eq("excluir")
    df_excluidos = df[excl_mask].copy()
    df_final = df[~excl_mask].copy()

    # métricas simples
    metrics = {
        "total": int(len(df)),
        "catalogo": int((df["fonte"]=="catalogo").sum()),
        "catalogo_contains": int((df["fonte"]=="catalogo-contains").sum()),
        "inferido": int((df["fonte"]=="semantico").sum()),
        "manter": int((df["fonte"]=="nenhum").sum()),
        "excluidos": int(excl_mask.sum()),
        "baixa_confianca": int(((df["fonte"]=="semantico") & (df["confianca"]<hi_threshold)).sum()),
    }

    # tabela de baixa confiança (apenas inferidos com sim < hi)
    baixa_conf = df[(df["fonte"]=="semantico") & (df["confianca"]<hi_threshold)].copy()
    cols_show = ["ID","Nome","Endereço","Categoria","Sub-Categoria","acao","fonte","confianca"]
    baixa_conf = baixa_conf[[c for c in cols_show if c in baixa_conf.columns]]

    # limpeza auxiliares
    df_final = df_final.drop(columns=[col for col in ["_k_subcat","k_original","_nova_contains"] if col in df_final.columns], errors="ignore")
    df_excluidos = df_excluidos.drop(columns=[col for col in ["_k_subcat","k_original","_nova_contains"] if col in df_excluidos.columns], errors="ignore")

    return df_final, baixa_conf, metrics
