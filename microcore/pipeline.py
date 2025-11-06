from typing import Tuple
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from .utils import norm_text

def _build_tfidf_index(target_terms: list[str]):
    vec = TfidfVectorizer(ngram_range=(1,2), min_df=1)
    X = vec.fit_transform(target_terms)
    return vec, X

def _semantic_match(query: str, vec, X, vocab_terms: list[str]) -> Tuple[str, float]:
    qX = vec.transform([query])
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
    mapping_df precisa conter:
      ['SubCat Original','Nova SubCat','categoria_oficial','k_original','k_nova','k_categoria']
    Guard-rails:
      - 'Excluir' remove do resultado final
      - Sub-Categoria -> Categoria sempre ajustada para a 'categoria_oficial' do mapeamento
    """
    df = df_in.copy()

    # colunas essenciais
    for col in ["Categoria","Sub-Categoria","Nome","Endereço"]:
        if col not in df.columns:
            df[col] = ""

    # SALVAR ORIGINAIS para painel de reclassificação
    df["_Cat_Original"] = df["Categoria"].astype(str)
    df["_SubCat_Original"] = df["Sub-Categoria"].astype(str)

    # Dicionários rápidos para guard-rail
    #   k_nova -> (Nova SubCat bonitinha, categoria_oficial bonitinha)
    subcat_norm_to_pretty = dict(mapping_df[["k_nova","Nova SubCat"]].drop_duplicates().values)
    subcat_norm_to_cat    = dict(mapping_df[["k_nova","categoria_oficial"]].drop_duplicates().values)

    def _apply_guard_rail(row, k_nova_norm: str):
        # Ajusta Categoria de acordo com a categoria_oficial da subcategoria
        if not k_nova_norm:
            return row
        cat_oficial = subcat_norm_to_cat.get(k_nova_norm)
        if cat_oficial:
            row["Categoria"] = cat_oficial
        return row

    # 1) determinístico exato pela Sub-Categoria original
    df["_k_subcat_in"] = df["Sub-Categoria"].astype(str).map(norm_text)
    df = df.merge(
        mapping_df[["k_original","k_nova","Nova SubCat","categoria_oficial"]],
        left_on="_k_subcat_in", right_on="k_original", how="left"
    )

    det_mask = df["k_nova"].notna()
    if det_mask.any():
        df.loc[det_mask, "Sub-Categoria"] = df.loc[det_mask, "Nova SubCat"]
        df.loc[det_mask, "acao"] = np.where(
            df.loc[det_mask, "Nova SubCat"].str.strip().str.lower()=="excluir", "Excluir", "Corrigir"
        )
        df.loc[det_mask, "fonte"] = "catalogo"
        df.loc[det_mask, "confianca"] = 0.99
        # guard-rail de categoria
        for i in df.index[det_mask]:
            k_norm = norm_text(str(df.at[i, "Sub-Categoria"]))
            df.loc[i] = _apply_guard_rail(df.loc[i], k_norm)

    # 2) determinístico contains (Nome/Endereço) para não mapeados
    not_mapped = df["acao"].isna()  # noqa: F841
    contains_pairs = mapping_df[["k_original","Nova SubCat"]].drop_duplicates().values.tolist()

    def _contains_rule(row):
        if pd.isna(row.get("acao")):
            hay = " ".join(str(row.get(c, "")) for c in ("Nome","Endereço"))
            hay_n = norm_text(hay)
            for k_orig, nova in contains_pairs:
                if k_orig and k_orig in hay_n:
                    return nova
        return None

    df["_nova_contains"] = df.apply(_contains_rule, axis=1)
    cont_mask = df["acao"].isna() & df["_nova_contains"].notna()
    if cont_mask.any():
        df.loc[cont_mask, "Sub-Categoria"] = df.loc[cont_mask, "_nova_contains"]
        df.loc[cont_mask, "acao"] = np.where(
            df.loc[cont_mask, "_nova_contains"].str.strip().str.lower()=="excluir", "Excluir", "Corrigir"
        )
        df.loc[cont_mask, "fonte"] = "catalogo-contains"
        df.loc[cont_mask, "confianca"] = 0.92
        # guard-rail
        for i in df.index[cont_mask]:
            k_norm = norm_text(str(df.at[i, "Sub-Categoria"]))
            df.loc[i] = _apply_guard_rail(df.loc[i], k_norm)
    
    # 2.1) Validador semântico universal (aplica em todos os determinísticos)
    det_mask = df["fonte"].isin(["catalogo", "catalogo-contains"])
    idx_det = df.index[det_mask].tolist()

    if len(idx_det) > 0:
        target_terms_pretty = mapping_df["Nova SubCat"].astype(str).dropna().unique().tolist()
        target_terms_norm = [norm_text(t) for t in target_terms_pretty]
        vec_val, X_val = _build_tfidf_index(target_terms_norm)

        for i in idx_det:
            bag = " ".join(str(df.at[i, c]) for c in text_columns)
            q = norm_text(bag)
            if not q:
                continue
            pred_norm, sim = _semantic_match(q, vec_val, X_val, target_terms_norm)
            human_pred = mapping_df.loc[
                mapping_df["k_nova"] == pred_norm, "Nova SubCat"
            ].head(1).values

            atual = str(df.at[i, "Sub-Categoria"]).strip().lower()
            if len(human_pred) > 0:
                nova_pred = str(human_pred[0]).strip().lower()
                # se modelo diverge do determinístico e tem boa confiança, reclassifica
                if nova_pred != atual and sim >= 0.85:
                    df.at[i, "Sub-Categoria"] = human_pred[0]
                    df.at[i, "acao"] = "Corrigir"
                    df.at[i, "fonte"] = "semantico-validador"
                    df.at[i, "confianca"] = round(float(sim), 4)
                    # guard-rail de categoria
                    k_norm = norm_text(str(human_pred[0]))
                    cat_oficial = mapping_df.loc[
                        mapping_df["k_nova"] == k_norm, "categoria_oficial"
                    ].head(1).values
                    if len(cat_oficial) > 0:
                        df.at[i, "Categoria"] = cat_oficial[0]


    # 3) Semântico leve (TF-IDF) nos remanescentes
    pending = df["acao"].isna()
    pending_idx = df.index[pending].tolist()

    # universo alvo: todas as "Nova SubCat" (inclui "Excluir")
    target_terms_pretty = mapping_df["Nova SubCat"].astype(str).dropna().unique().tolist()
    target_terms_norm   = [norm_text(t) for t in target_terms_pretty]
    vec, X = _build_tfidf_index(target_terms_norm)

    for i in pending_idx:
        nome = str(df.at[i, "Nome"])
        endereco = str(df.at[i, "Endereço"])
        categoria = str(df.at[i, "Categoria"])
        subcat_original = str(df.at[i, "Sub-Categoria"]).strip()

        # 🧠 se subcategoria estiver vazia, dá mais peso ao nome
        if subcat_original in ["", "nan", "None"]:
            bag = f"{nome} {nome} {endereco} {categoria}"  # nome x2
        else:
            bag = f"{nome} {endereco} {categoria}"

        q = norm_text(bag)
        if not q:
            df.at[i, "acao"] = "Manter"
            df.at[i, "fonte"] = "nenhum"
            df.at[i, "confianca"] = 0.0
            continue

        pred_norm, sim = _semantic_match(q, vec, X, target_terms_norm)
        if sim >= lo_threshold:
            human_pred = mapping_df.loc[
                mapping_df["k_nova"] == pred_norm, "Nova SubCat"
            ].head(1).values
            if len(human_pred) > 0:
                df.at[i, "Sub-Categoria"] = human_pred[0]
                df.at[i, "acao"] = "Inferir"
                df.at[i, "fonte"] = "semantico"
                df.at[i, "confianca"] = round(float(sim), 4)
                # guard-rail
                cat_oficial = mapping_df.loc[
                    mapping_df["k_nova"] == pred_norm, "categoria_oficial"
                ].head(1).values
                if len(cat_oficial) > 0:
                    df.at[i, "Categoria"] = cat_oficial[0]
        else:
            df.at[i, "acao"] = "Manter"
            df.at[i, "fonte"] = "nenhum"
            df.at[i, "confianca"] = round(float(sim), 4)


    # 4) “Excluir” sai do final (mas conta nas métricas)
    excl_mask = df["Sub-Categoria"].astype(str).str.strip().str.lower().eq("excluir")
    df_excluidos = df[excl_mask].copy()
    df_final = df[~excl_mask].copy()

    # df_all = todas as linhas com ações (inclui Excluir)
    df_all = df.copy()

    # limpeza de colunas auxiliares (mas PRESERVAR _Cat_Original/_SubCat_Original)
    drop_cols = ["_k_subcat_in","k_original","k_nova","_nova_contains","categoria_oficial"]
    for _d in drop_cols:
        for _tgt in (df_final, df_excluidos, df_all):
            if _d in _tgt.columns:
                _tgt.drop(columns=[_d], inplace=True)

    # renomear originais para nomes “bonitos”
    for _tgt in (df_final, df_excluidos, df_all):
        if "_Cat_Original" in _tgt.columns:
            _tgt.rename(columns={"_Cat_Original":"Cat Original","_SubCat_Original":"SubCat Original"}, inplace=True)

    # Métricas
    metrics = {
        "total": int(len(df)),
        "catalogo": int((df["fonte"]=="catalogo").sum()),
        "catalogo_contains": int((df["fonte"]=="catalogo-contains").sum()),
        "inferido": int((df["fonte"]=="semantico").sum()),
        "manter": int((df["fonte"]=="nenhum").sum()),
        "excluidos": int(excl_mask.sum()),
        "baixa_confianca": int(((df["fonte"]=="semantico") & (df["confianca"]<hi_threshold)).sum()),
    }

    
    baixa_conf = df[(df["fonte"]=="semantico") & (df["confianca"]<hi_threshold)].copy()
    cols_show = ["ID","Nome","Endereço","Categoria","Sub-Categoria","acao","fonte","confianca"]
    baixa_conf = baixa_conf[[c for c in cols_show if c in baixa_conf.columns]]

    # limpeza
    drop_cols = ["_k_subcat_in","k_original","k_nova","_nova_contains","categoria_oficial"]
    df_final = df_final.drop(columns=[c for c in drop_cols if c in df_final.columns], errors="ignore")
    df_excluidos = df_excluidos.drop(columns=[c for c in drop_cols if c in df_excluidos.columns], errors="ignore")

    return df_final, baixa_conf, metrics, df_all
