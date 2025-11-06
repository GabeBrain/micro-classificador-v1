import io
import time
import pandas as pd
import numpy as np
import streamlit as st

from microcore.pipeline import process_dataframe
from microcore.catalog_loader import load_mapping_gsheets  



# ---------- Config da página ----------
st.set_page_config(
    layout="wide",
    page_title="Micro Classificador | MVP v1",
    page_icon="🧭"
)

# Logo (garante que assets/logoBrain.png exista)
try:
    st.logo("assets/logoBrain.png")
except Exception:
    pass  # se não existir, ignora sem quebrar o app

# ---------- Estilos custom (aproveita seu tema do config.toml) ----------
PRIMARY = "#006400"  # mesmo do config.toml
TEXT = "#31333F"

st.markdown(
    f"""
    <style>
        /* Títulos mais compactos */
        h1, h2, h3 {{
            color: {TEXT};
            letter-spacing: 0.2px;
        }}
        /* Container "card" reutilizável */
        .card {{
            background: #FFFFFF;
            border: 1px solid rgba(0,0,0,0.06);
            border-radius: 12px;
            padding: 16px 18px;
            box-shadow: 0 2px 12px rgba(0,0,0,0.04);
        }}
        /* Tabela mais legível */
        .stDataFrame, .stTable {{
            border-radius: 12px !important;
            overflow: hidden !important;
        }}
        /* Botão primário mais marcante */
        .stButton>button[kind="primary"] {{
            background: {PRIMARY} !important;
            color: white !important;
            border: 1px solid {PRIMARY} !important;
            border-radius: 10px !important;
            font-weight: 600 !important;
        }}
        /* Métricas “cardificadas” */
        div[data-testid="metric-container"] {{
            background: #FFFFFF;
            border: 1px solid rgba(0,0,0,0.06);
            border-radius: 12px;
            padding: 12px 12px;
            box-shadow: 0 2px 12px rgba(0,0,0,0.04);
        }}
        /* Sidebar com espaçamento */
        [data-testid="stSidebar"] {{
            padding-top: 10px;
        }}
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------- Header ----------
st.markdown("## 🧭 Micro Classificador — MVP v1")
st.caption(
    "Reclassificação por **catálogo determinístico** + **similaridade semântica (TF-IDF)**. "
    "Subcategorias **“Excluir”** são removidas do resultado final."
)

with st.sidebar:
    st.markdown("### ⚙️ Parâmetros")
    hi = st.slider("Limiar alta confiança (aplica direto)", 0.50, 0.99, 0.90, 0.01)
    lo = st.slider("Limiar baixa confiança (mínimo para aplicar)", 0.10, hi, 0.70, 0.01)
    st.caption("Scores < baixo permanecem como **Manter** (sem alteração).")
    st.markdown("---")
    st.markdown("#### Dicas")
    st.write(
        "- Deixe **0.90** e **0.70** para uma demo equilibrada.\n"
        "- Use um mapeamento enxuto para ver o semântico atuar."
    )

# ---------- Etapa 1: Catálogo (Mapeamento via Google Sheets FIXO) ----------
st.markdown("### 1) Catálogo (Google Sheets fixo)")
st.link_button("📄 Ver no Google Sheets", "1egrGImJrXqfvxa7U4QirKrePE7w8QtuOG8Jc_H_AsJE")

try:
    mapping_df = load_mapping_gsheets()
    # ---- resumo geral ----
    num_cats = mapping_df["categoria_oficial"].nunique()
    num_mapeamentos = len(mapping_df)
    num_subcats_orig = mapping_df["SubCat Original"].nunique()
    num_subcats_novas = mapping_df["Nova SubCat"].nunique()

    st.success(
        f"✅ Catálogo carregado com **{num_cats} categorias**, "
        f"**{num_subcats_orig} subcategorias originais** e "
        f"**{num_subcats_novas} novas subcategorias** "
        f"({num_mapeamentos} mapeamentos totais)."
    )

    # ---- tabela resumo por categoria ----
    resumo = (
        mapping_df.groupby("categoria_oficial")
        .agg(
            Subcats_Originais=("SubCat Original", "nunique"),
            Novas_Subcats=("Nova SubCat", "nunique"),
            Mapeamentos=("SubCat Original", "count"),
        )
        .sort_index()
        .reset_index()
    )


    # ---- expanders por categoria ----
    st.markdown("#### 🔍 Detalhamento por categoria")
    for cat in resumo["categoria_oficial"]:
        subset = mapping_df[mapping_df["categoria_oficial"] == cat].copy()
        total_mapeamentos = len(subset)
        total_novas = subset["Nova SubCat"].nunique()
        with st.expander(f"{cat} — {total_mapeamentos} mapeamentos, {total_novas} novas subcategorias"):
            # agrupamento interno: Nova SubCat -> quantas originais apontam
            sub_stats = (
                subset.groupby("Nova SubCat")
                .agg(Originais=("SubCat Original", "nunique"))
                .sort_values("Originais", ascending=False)
            )
            st.dataframe(sub_stats, use_container_width=True)
except Exception as e:
    st.error(f"Erro ao carregar catálogo do Google Sheets: {e}")
    st.stop()

# ---------- Etapa 2: Entrada ----------
st.markdown("### 2) Arquivo de entrada")
with st.container():
    data_file = st.file_uploader(
        "Envie o arquivo **XLSX** a ser processado",
        type=["xlsx"], key="data",
        help="Layout padrão com colunas como ID, Nome, Sub-Categoria, Categoria, Endereço, etc."
    )

if not data_file:
    st.info("Envie o arquivo de dados para liberar o processamento.")
    st.stop()

df_in = pd.read_excel(data_file, engine="openpyxl")
st.markdown('<div class="card">', unsafe_allow_html=True)
st.write("Prévia (30 primeiras linhas):")
st.dataframe(df_in.head(30), use_container_width=True)
st.markdown('</div>', unsafe_allow_html=True)

# ---------- Ação: Processar ----------
st.markdown("### 3) Processar")
run_col, _ = st.columns([1, 3])
with run_col:
    run = st.button("🚀 Processar agora", type="primary", use_container_width=True)

if not run:
    st.stop()

with st.spinner("Processando..."):
    start = time.time()
    df_final, baixa_conf, metrics = process_dataframe(
        df_in, mapping_df, hi_threshold=hi, lo_threshold=lo
    )
    elapsed = time.time() - start

st.success(f"Concluído em {elapsed:.2f}s")

# ---------- Métricas ----------
st.markdown("### 4) Métricas de execução")
m1, m2, m3, m4, m5, m6 = st.columns(6)
m1.metric("Total (inclui 'Excluir')", metrics["total"])
m2.metric("Catálogo (exato)", metrics["catalogo"])
m3.metric("Catálogo (contains)", metrics["catalogo_contains"])
m4.metric("Inferidos (semântico)", metrics["inferido"])
m5.metric("Mantidos", metrics["manter"])
m6.metric("Excluídos", metrics["excluidos"])

# ---------- Baixa confiança ----------
st.markdown("### 5) Inferências de baixa confiança")
st.caption("Inferências semânticas com **score < limiar alto** (ex.: 0.90).")
st.markdown('<div class="card">', unsafe_allow_html=True)
if len(baixa_conf) == 0:
    st.success("Nenhuma inferência de baixa confiança 🎉")
else:
    st.dataframe(baixa_conf, use_container_width=True)
st.markdown('</div>', unsafe_allow_html=True)

# ---------- Resultado final ----------
st.markdown("### 6) Resultado final (sem 'Excluir')")
st.markdown('<div class="card">', unsafe_allow_html=True)
st.dataframe(df_final.head(50), use_container_width=True)
st.markdown('</div>', unsafe_allow_html=True)

# ---------- Download ----------
st.markdown("### 7) Baixar resultado")
out_buf = io.BytesIO()
with pd.ExcelWriter(out_buf, engine="openpyxl") as writer:
    df_final.to_excel(writer, index=False, sheet_name="final")
    baixa_conf.to_excel(writer, index=False, sheet_name="baixa_confianca")

dl_cols = st.columns([1, 3, 1])
with dl_cols[0]:
    st.download_button(
        "📥 Baixar XLSX",
        data=out_buf.getvalue(),
        file_name="resultado_mvp_v1.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True
    )

st.caption("MVP v1 — pronto para demonstração e coleta de feedbacks.")
