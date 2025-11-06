import io
import time
import pandas as pd
import streamlit as st

from microcore.pipeline import process_dataframe
from microcore.catalog_loader import load_mapping_gsheets  



# ---------- Config da página ----------
st.set_page_config(
    layout="wide",
    page_title="Micro Classificador | MVP v1",
    page_icon="🧭"
)
st.rerun()

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
st.link_button("📄 Ver no Google Sheets", "https://docs.google.com/spreadsheets/d/1egrGImJrXqfvxa7U4QirKrePE7w8QtuOG8Jc_H_AsJE/edit?usp=sharing")

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
    df_final, baixa_conf, metrics, df_all = process_dataframe(
        df_in, mapping_df, hi_threshold=hi, lo_threshold=lo)
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

# ---------- Painel geral de reclassificações ----------
st.markdown("### 5) Resultados das reclassificações (antes × depois, com confiança)")

# construir tabela unificada a partir de df_all (todas as linhas)
panel = df_all.copy()

# garantir colunas “bonitas”
# Cat/SubCat novas já estão em 'Categoria' e 'Sub-Categoria'
panel = panel.rename(columns={
    "Categoria": "Cat Nova",
    "Sub-Categoria": "SubCat Nova"
})

# ordenar por ação para leitura executiva
if "acao" in panel.columns:
    panel["acao"] = pd.Categorical(panel["acao"], ["Corrigir","Inferir","Manter","Excluir"], ordered=True)
    panel = panel.sort_values(["acao","Cat Nova","SubCat Nova","Nome"], na_position="last")

cols_panel = [c for c in [
    "ID","Nome",
    "Cat Original","Cat Nova",
    "SubCat Original","SubCat Nova",
    "acao","fonte","confianca"
] if c in panel.columns]

st.dataframe(panel[cols_panel], use_container_width=True)

# ---------- Análise descritiva por categoria/subcategoria ----------
st.markdown("### 6) Análise descritiva das subcategorias (sem 'Excluir')")

if {"Cat Nova","SubCat Nova"}.issubset(panel.columns):
    # filtrar registros finais (sem 'Excluir')
    final_view = panel[~panel["SubCat Nova"].astype(str).str.strip().str.lower().eq("excluir")].copy()
    final_view.rename(columns={"Cat Nova":"Categoria", "SubCat Nova":"Sub-Categoria"}, inplace=True)

    if {"Categoria","Sub-Categoria"}.issubset(final_view.columns):
        # contagem por par (Categoria, Sub-Categoria)
        counts = (
            final_view
            .groupby(["Categoria","Sub-Categoria"])
            .size()
            .reset_index(name="Qtd")
        )
        # porcentagem dentro de cada categoria (EVITA reset_index em multiindex)
        counts["Percentual"] = (
            counts["Qtd"] / counts.groupby("Categoria")["Qtd"].transform("sum") * 100
        ).round(2)

        summary = counts[["Categoria","Sub-Categoria","Percentual"]]\
                    .sort_values(["Categoria","Percentual"], ascending=[True, False])

        st.caption("Participação (%) de cada Sub-Categoria dentro de sua Categoria (soma 100% por categoria).")
        st.dataframe(summary, use_container_width=True)

        # (Opcional) gráfico visual
        import altair as alt
        st.markdown("#### 📊 Distribuição visual")
        chart = (
            alt.Chart(summary)
            .mark_bar()
            .encode(
                y=alt.Y("Categoria:N", sort="-x", title="Categoria"),
                x=alt.X("Percentual:Q", title="% na categoria"),
                color=alt.Color("Sub-Categoria:N", legend=alt.Legend(title="Subcategoria")),
                tooltip=["Categoria","Sub-Categoria","Percentual"]
            )
            .properties(height=420)
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.warning("Colunas 'Categoria' e 'Sub-Categoria' não encontradas no resultado final.")
else:
    st.warning("Painel não possui colunas 'Cat Nova' / 'SubCat Nova'.")



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
