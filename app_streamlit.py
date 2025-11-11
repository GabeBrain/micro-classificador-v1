import io
import time
from datetime import datetime, timezone, timedelta
import pandas as pd
import streamlit as st
import altair as alt
from pathlib import Path

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
    pass  # ignora se não existir

# ---------- Estilos custom ----------
PRIMARY = "#006400"
TEXT = "#31333F"

CATEGORY_ICONS = {
    "Alimentação": "🍽️",
    "Saúde e Bem Estar": "🩺",
    "Serviços": "🛠️",
    "Moda": "👗",
    "Educação": "📚",
    "Outros": "🧩",
    "Inst. Financeira": "🏦",
    "Decoração": "🏠",
    "Automotivo": "🚗",
}
DEFAULT_CATEGORY_ICON = "📊"
FINAL_XLSX_COLUMNS = [
    "Nome",
    "Cat Original",
    "Categoria",
    "SubCat Original",
    "SubCat Catalogada",
    "Sub-Categoria",
    "fonte",
    "acao",
    "Endereço",
    "confianca",
    "SubCat_Intermediaria",
    "ID",
    "Ativo",
    "Em Shopping",
    "Porte",
    "Classe",
    "Longitude",
    "Latitude",
]

st.markdown(
    f"""
    <style>

        h1, h2, h3 {{
            color: {TEXT};
            letter-spacing: 0.2px;
        }}

        .card {{
            background: #FFFFFF;
            border: 1px solid rgba(0,0,0,0.06);
            border-radius: 12px;
            padding: 16px 18px;
            box-shadow: 0 2px 12px rgba(0,0,0,0.04);
        }}

        .stDataFrame, .stTable {{
            border-radius: 12px !important;
            overflow: hidden !important;
        }}

        .stButton>button[kind="primary"] {{
            background: {PRIMARY} !important;
            color: white !important;
            border: 1px solid {PRIMARY} !important;
            border-radius: 10px !important;
            font-weight: 600 !important;
        }}

        div[data-testid="metric-container"] {{
            background: #FFFFFF;
            border: 1px solid rgba(0,0,0,0.06);
            border-radius: 12px;
            padding: 12px 12px;
            box-shadow: 0 2px 12px rgba(0,0,0,0.04);
        }}

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
    

# ---------- Etapa 1: Catálogo (Google Sheets fixo) ----------
st.markdown("### 1) Catálogo (Google Sheets fixo)")
st.link_button(
    "📄 Ver no Google Sheets",
    "https://docs.google.com/spreadsheets/d/1egrGImJrXqfvxa7U4QirKrePE7w8QtuOG8Jc_H_AsJE/edit?usp=sharing",
)

try:
    mapping_df = load_mapping_gsheets()
    
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

    
    st.markdown("#### 📊 Detalhamento por categoria")
    category_cards = []
    for cat in resumo["categoria_oficial"]:
        subset = mapping_df[mapping_df["categoria_oficial"] == cat].copy()
        total_mapeamentos = len(subset)
        total_novas = subset["Nova SubCat"].nunique()
        category_cards.append((cat, subset, total_mapeamentos, total_novas))

    cols_per_row = 3
    for idx in range(0, len(category_cards), cols_per_row):
        cols = st.columns(cols_per_row)
        for col, card in zip(cols, category_cards[idx:idx + cols_per_row]):
            cat, subset, total_mapeamentos, total_novas = card
            icon = CATEGORY_ICONS.get(cat, DEFAULT_CATEGORY_ICON)
            label = f"{icon} {cat} — {total_mapeamentos} mapeamentos, {total_novas} novas subcategorias"
            with col:
                with st.expander(label):
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
        type=["xlsx"],
        key="data",
        help="Layout padrão com colunas como ID, Nome, Sub-Categoria, Categoria, Endereço, etc."
    )

if not data_file:
    st.info("Envie o arquivo de dados para liberar o processamento.")
    st.stop()

df_in = pd.read_excel(data_file, engine="openpyxl")
br_tz = timezone(timedelta(hours=-3))
timestamp_tag = datetime.now(br_tz).strftime("%d%m_%H%M")
uploaded_name = getattr(data_file, "name", "resultado_mvp_v1.xlsx")
upload_path = Path(uploaded_name)
download_base = upload_path.stem or "resultado_mvp_v1"
download_ext = upload_path.suffix or ".xlsx"
processed_filename = f"{download_base}_Processado_{timestamp_tag}{download_ext}"
st.markdown('<div class="card">', unsafe_allow_html=True)
st.write("Prévia (30 primeiras linhas):")
st.dataframe(df_in.head(30), use_container_width=True)
st.markdown('</div>', unsafe_allow_html=True)

# ---------- Etapa 3: Processar (com placeholder para evitar erro de sessão) ----------
st.markdown("### 3) Processar")
placeholder = st.empty()
run_col, _ = st.columns([1, 3])
with run_col:
    run = st.button("🚀 Processar agora", type="primary", use_container_width=True)

if not run:
    st.stop()

placeholder.info("⏳ Iniciando processamento...")

try:
    progress = st.progress(0)
    status = st.empty()

    def progress_callback(fraction, text):
        progress.progress(fraction)
        status.info(text)

    start = time.time()
    df_final, baixa_conf, metrics, df_all = process_dataframe(
        df_in,
        mapping_df,
        hi_threshold=hi,
        lo_threshold=lo,
        progress_callback=progress_callback
    )
    elapsed = time.time() - start

    progress.empty()
    status.success(f"✅ Concluído em {elapsed:.2f}s")

except Exception as e:
    placeholder.error(f"❌ Erro durante o processamento: {e}")
    st.stop()

# ---------- Métricas ----------
st.markdown("### 4) Métricas de execução")
m1, m2, m3, m4, m5, m6 = st.columns(6)
m1.metric("Total (inclui 'Excluir')", metrics["total"])
m2.metric("Catálogo (exato)", metrics["catalogo"])
m3.metric("Catálogo (contains)", metrics["catalogo_contains"])
m4.metric("Validador semântico", (df_all["fonte"] == "semantico-validador").sum())
m5.metric("Mantidos", metrics["manter"])
m6.metric("Excluídos", metrics["excluidos"])

# ---------- Painel geral de reclassificações ----------
st.markdown("### 5) Resultados das reclassificações (antes × depois, com confiança)")


panel = df_all.copy()

# renomear colunas pra manter padrão
panel = panel.rename(columns={
    "Categoria": "Cat Nova",
    "Sub-Categoria": "SubCat Nova"
})

cols_order = [
    "Nome",
    "SubCat Original", "SubCat Intermediaria", "SubCat Nova",
    "Cat Original", "Cat Nova",
    "acao", "confianca", "fonte"
]

# organizar fonte para ordenação visual
if "fonte" in panel.columns:
    fontes = panel["fonte"].dropna().unique().tolist()
    for fonte in fontes:
        subset = panel[panel["fonte"] == fonte].copy()
        st.markdown(f"#### 🔹 {fonte.upper()} ({len(subset)} registros)")
        if not subset.empty:
            df_show = subset.copy()
            # mostrar SubCat_Intermediaria se existir
            cols_display = [c for c in cols_order if c in df_show.columns]
            st.dataframe(df_show[cols_display], use_container_width=True)
        else:
            st.info(f"Sem registros para '{fonte}'.")

# ---------- Análise descritiva das subcategorias ----------
st.markdown("### 6) Análise descritiva das subcategorias (sem 'Excluir')")

if {"Cat Nova","SubCat Nova"}.issubset(panel.columns):
    final_view = panel[
        ~panel["SubCat Nova"].astype(str).str.strip().str.lower().eq("excluir")
    ].copy()
    final_view.rename(columns={"Cat Nova":"Categoria", "SubCat Nova":"Sub-Categoria"}, inplace=True)

    if {"Categoria","Sub-Categoria"}.issubset(final_view.columns):
        counts = (
            final_view
            .groupby(["Categoria","Sub-Categoria"])
            .size()
            .reset_index(name="Qtd")
        )
        
        counts["Percentual"] = (
            counts["Qtd"] / counts.groupby("Categoria")["Qtd"].transform("sum") * 100
        ).round(2)

        st.caption("Distribuição (%) de subcategorias dentro de cada categoria:")

        for cat in sorted(counts["Categoria"].unique()):
            subset = counts[counts["Categoria"] == cat].sort_values("Percentual", ascending=True)
            with st.expander(f"{cat} ({len(subset)} subcategorias)"):
                chart = (
                    alt.Chart(subset)
                    .mark_bar()
                    .encode(
                        x=alt.X("Percentual:Q", title="% dentro da categoria"),
                        y=alt.Y("Sub-Categoria:N", sort="-x", title="Subcategoria"),
                        tooltip=["Sub-Categoria","Percentual"]
                    )
                    .properties(height=max(300, 25 * len(subset)))
                )
                st.altair_chart(chart, use_container_width=True)
    else:
        st.warning("Colunas 'Categoria' e 'Sub-Categoria' não encontradas no resultado final.")
else:
    st.warning("Painel não possui colunas 'Cat Nova' / 'SubCat Nova'.")



# ---------- Download ----------
st.markdown("### 7) Baixar resultado")
out_buf = io.BytesIO()
final_cols = [col for col in FINAL_XLSX_COLUMNS if col in df_final.columns]
final_cols += [col for col in df_final.columns if col not in final_cols]
df_final_export = df_final[final_cols]
with pd.ExcelWriter(out_buf, engine="openpyxl") as writer:
    df_final_export.to_excel(writer, index=False, sheet_name="final")
    baixa_conf.to_excel(writer, index=False, sheet_name="baixa_confianca")

dl_cols = st.columns([1, 3, 1])
with dl_cols[0]:
    st.download_button(
        "📥 Baixar XLSX",
        data=out_buf.getvalue(),
        file_name=processed_filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True
    )

st.caption("MVP v1 — pronto para demonstração e coleta de feedbacks.")
