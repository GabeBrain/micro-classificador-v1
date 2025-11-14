import re
from typing import Tuple
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from .utils import norm_text

_LOJA_PREFIX_RE = re.compile(
    r"^\s*(?:lojas?|postos?|est[úu]dios?|empresas?|serviços?)\s+(?:de|da|do|das|dos)?\s*",
    re.IGNORECASE,
)


def _strip_loja_prefix(value):
    if pd.isna(value):
        return value
    text = str(value).strip()
    if not text:
        return text
    cleaned = _LOJA_PREFIX_RE.sub("", text).strip()
    return cleaned or text

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
                      text_columns: Tuple[str,...] = ("Sub-Categoria","Nome","Endereço"),
                      progress_callback=None) -> Tuple[pd.DataFrame, pd.DataFrame, dict]:

    """
    mapping_df precisa conter:
      ['SubCat Original','Nova SubCat','categoria_oficial','k_original','k_nova','k_categoria']
    Guard-rails:
      - 'Excluir' permanece no resultado final porém sinalizado para revisão manual
      - Sub-Categoria -> Categoria sempre ajustada para a 'categoria_oficial' do mapeamento
    """
    df = df_in.copy()

    # helper interno para atualizar a barra
    def _update(frac, text):
        if progress_callback:
            try:
                progress_callback(frac, text)
            except Exception:
                pass

    _update(0.05, "Normalizando campos e preparando dados...")

    # colunas essenciais
    for col in ["Categoria","Sub-Categoria","Nome","Endereço"]:
        if col not in df.columns:
            df[col] = ""

    # SALVAR ORIGINAIS para painel de reclassificação
    df["_Cat_Original"] = df["Categoria"].astype(str)
    df["_SubCat_Original"] = df["Sub-Categoria"].astype(str)

    # remove prefixos como "Loja de" para melhorar o casamento semântico
    df["Sub-Categoria"] = df["Sub-Categoria"].apply(_strip_loja_prefix)

    _update(0.10, "Criando dicionários e guard-rails...")

    # Dicionários rápidos para guard-rail
    #   k_nova -> (Nova SubCat bonitinha, categoria_oficial bonitinha)
    subcat_norm_to_pretty = dict(mapping_df[["k_nova","Nova SubCat"]].drop_duplicates().values)
    subcat_norm_to_cat    = dict(mapping_df[["k_nova","categoria_oficial"]].drop_duplicates().values)
    orig_norm_to_new      = dict(mapping_df[["k_original","Nova SubCat"]].drop_duplicates().values)
    orig_norm_to_k_nova   = dict(mapping_df[["k_original","k_nova"]].drop_duplicates().values)

    def _apply_guard_rail(row, k_nova_norm: str):
        # Ajusta Categoria de acordo com a categoria_oficial da subcategoria
        if not k_nova_norm:
            return row
        cat_oficial = subcat_norm_to_cat.get(k_nova_norm)
        if cat_oficial:
            row["Categoria"] = cat_oficial
        return row
    
    _update(0.15, "Aplicando regras determinísticas (catálogo exato)...")

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

    _update(0.30, "Verificando matches 'contains' no catálogo...")

    # 2) determinístico contains (Nome/Endereço) para não mapeados
    not_mapped = df["acao"].isna()  # noqa: F841
    contains_pairs = mapping_df[["k_original","Nova SubCat"]].drop_duplicates().values.tolist()

    def _contains_rule(row):
        if pd.isna(row.get("acao")):
            hay = " ".join(str(row.get(c, "")) for c in ("Nome"))
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
        df.loc[cont_mask, "confianca"] = 0.90
        # guard-rail
        for i in df.index[cont_mask]:
            k_norm = norm_text(str(df.at[i, "Sub-Categoria"]))
            df.loc[i] = _apply_guard_rail(df.loc[i], k_norm)

    _update(0.45, "Executando validador semântico...")
    
    # 2.1) Validador semântico universal (somente Nome como entrada)
    det_mask = df["fonte"].isin(["catalogo", "catalogo-contains"])
    idx_det = df.index[det_mask].tolist()

    if len(idx_det) > 0:
        target_terms_pretty = mapping_df["SubCat Original"].astype(str).dropna().unique().tolist()
        target_terms_norm = [norm_text(t) for t in target_terms_pretty]
        vec_val, X_val = _build_tfidf_index(target_terms_norm)

        for i in idx_det:
            nome = str(df.at[i, "Nome"])
            if not nome or nome.strip().lower() in ["nan", "none", ""]:
                continue

            q = norm_text(nome)
            pred_norm, sim = _semantic_match(q, vec_val, X_val, target_terms_norm)
            nova_pred = orig_norm_to_new.get(pred_norm)
            if not nova_pred:
                continue

            atual = str(df.at[i, "Sub-Categoria"]).strip().lower()
            categoria_atual = atual
            is_problematic = categoria_atual in ["cooperativa de crédito", "operadora de telefonia"]

            nova_pred = str(nova_pred).strip()
            nova_pred_norm = nova_pred.lower()

            # regra geral
            #  - se divergir e sim >= lo_threshold => Corrigir
            #  - se for categoria problemática => Corrigir mesmo que sim >= 0.35
            #  - se for problemática e sim < 0.35 => marcar para Verificar
            if nova_pred_norm != categoria_atual:
                if sim >= lo_threshold or (is_problematic and sim >= 0.35):
                    df.at[i, "Sub-Categoria"] = nova_pred
                    df.at[i, "acao"] = "Corrigir"
                    df.at[i, "fonte"] = "semantico-validador"
                    df.at[i, "confianca"] = round(float(sim), 4)
                    # guard-rail de categoria
                    k_norm = orig_norm_to_k_nova.get(pred_norm) or norm_text(nova_pred)
                    cat_oficial = subcat_norm_to_cat.get(k_norm)
                    if cat_oficial:
                        df.at[i, "Categoria"] = cat_oficial

                elif is_problematic and sim < 0.35:
                    df.at[i, "acao"] = "Verificar"
                    df.at[i, "fonte"] = "semantico-validador"
                    df.at[i, "confianca"] = round(float(sim), 4)

    _update(0.70, "Rodando inferência semântica nos pendentes...")

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

    # 3.1) Regra fixa: endereço contendo palavras-alvo vira Excluir
    endereco_excluir_mask = (
        df["Endereço"]
        .astype(str)
        .str.contains(r"\b(shopping|loja|lj|ponto|conj|conjunto)\b", case=False, na=False)
    )
    if endereco_excluir_mask.any():
        df.loc[endereco_excluir_mask, "Sub-Categoria"] = "Excluir"
        df.loc[endereco_excluir_mask, "acao"] = "Excluir"
        df.loc[endereco_excluir_mask, "fonte"] = "regra-endereco"
        df.loc[endereco_excluir_mask, "confianca"] = 1.0

    # 3.2) Remover duplicados antes de calcular métricas/exportar
    dedupe_subset = [
        "ID",
        "Nome",
        "Endereço",
        "Categoria",
        "Sub-Categoria",
        "_Cat_Original",
        "_SubCat_Original",
        "SubCat_Intermediaria",
        "acao",
        "fonte",
        "confianca",
    ]
    dedupe_subset = [c for c in dedupe_subset if c in df.columns]
    if dedupe_subset:
        df = df.drop_duplicates(subset=dedupe_subset).reset_index(drop=True)
    else:
        df = df.drop_duplicates().reset_index(drop=True)

    _update(0.90, "Finalizando e aplicando filtros de exclusão...")

    # 4) “Excluir” permanece no dataset final, mas segue destacado nas métricas
    excl_mask = df["Sub-Categoria"].astype(str).str.strip().str.lower().eq("excluir")

    # registra subcategoria intermediária antes de virar "Excluir"
    df.loc[excl_mask, "SubCat_Intermediaria"] = df.loc[excl_mask, "_SubCat_Original"]

    # mantém cópias separadas para controles
    df_excluidos = df[excl_mask].copy()
    df_final = df.copy()

    # df_all = todas as linhas com ações (inclui Excluir)
    df_all = df.copy()

    # --- garantir que SubCat_Intermediaria esteja presente no df_all ---
    if "SubCat_Intermediaria" not in df_all.columns:
        df_all["SubCat_Intermediaria"] = None

    # se existirem exclusões, preenche também no df_all
    if "Sub-Categoria" in df_all.columns and "_SubCat_Original" in df_all.columns:
        excl_mask_all = df_all["Sub-Categoria"].astype(str).str.strip().str.lower().eq("excluir")
        df_all.loc[excl_mask_all, "SubCat_Intermediaria"] = df_all.loc[excl_mask_all, "_SubCat_Original"]


    # limpeza de colunas auxiliares (mas PRESERVAR _Cat_Original/_SubCat_Original)
    drop_cols = ["_k_subcat_in","k_original","k_nova","_nova_contains","categoria_oficial"]
    for _d in drop_cols:
        for _tgt in (df_final, df_excluidos, df_all):
            if _d in _tgt.columns:
                _tgt.drop(columns=[_d], inplace=True)

    # renomear originais para nomes “bonitos”
    for _tgt in (df_final, df_excluidos, df_all):
        rename_map = {}
        if "_Cat_Original" in _tgt.columns:
            rename_map["_Cat_Original"] = "Cat Original"
        if "_SubCat_Original" in _tgt.columns:
            rename_map["_SubCat_Original"] = "SubCat Original"
        if "Nova SubCat" in _tgt.columns:
            rename_map["Nova SubCat"] = "SubCat Catalogada"
        if rename_map:
            _tgt.rename(columns=rename_map, inplace=True)

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

    _update(1.0, "✅ Processamento concluído.")
    
    return df_final, baixa_conf, metrics, df_all
