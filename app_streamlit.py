import io
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st
from rapidfuzz import fuzz, process as rf_process

try:
    import gspread
    from google.oauth2.service_account import Credentials
    from gspread.exceptions import WorksheetNotFound
except ImportError:
    gspread = None
    Credentials = None
    WorksheetNotFound = Exception

from microcore.pipeline import process_dataframe
from microcore.catalog_loader import load_mapping_gsheets, SHEET_ID
from microcore.utils import norm_text


# ---------- Config da página ----------
st.set_page_config(
    layout="wide",
    page_title="Micro Classificador | v2",
    page_icon=":compass:"
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
    "SubCat Original",
    "SubCat Catalogada",
    "Sub-Categoria",
    "Categoria",
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

GS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


@st.cache_resource(show_spinner=False)
def _get_gspread_client():
    if gspread is None or Credentials is None:
        raise RuntimeError("Instale 'gspread' e 'google-auth' para habilitar escrita no catálogo.")
    if "gcp_service_account" not in st.secrets:
        raise RuntimeError("Configure st.secrets['gcp_service_account'] com as credenciais do Google.")
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=GS_SCOPES,
    )
    return gspread.authorize(creds)


def _append_mapping_to_catalog(subcat_original: str, nova_subcat: str, categoria: str):
    client = _get_gspread_client()
    sh = client.open_by_key(SHEET_ID)
    try:
        worksheet = sh.worksheet(categoria)
    except WorksheetNotFound as exc:
        raise RuntimeError(f"Aba '{categoria}' não encontrada no Google Sheets.") from exc

    headers = worksheet.row_values(1)
    payload = []
    if headers:
        for header in headers:
            key = header.strip().lower()
            if key in {"subcat original", "subcat_original"}:
                payload.append(subcat_original)
            elif key in {"nova subcat", "nova_subcat"}:
                payload.append(nova_subcat)
            else:
                payload.append("")
    else:
        payload = [subcat_original, nova_subcat]

    worksheet.append_row(payload, value_input_option="USER_ENTERED")


def _catalog_suggestions(query: str, mapping_df: pd.DataFrame, limit: int = 5):
    if not query:
        return []
    catalog_terms = (
        mapping_df["Nova SubCat"]
        .dropna()
        .astype(str)
        .str.strip()
        .unique()
        .tolist()
    )
    if not catalog_terms:
        return []
    matches = rf_process.extract(
        query,
        catalog_terms,
        scorer=fuzz.WRatio,
        limit=limit,
    )
    suggestions = []
    for match, score, _ in matches:
        if match:
            suggestions.append({"label": match, "score": round(float(score), 1)})
    return suggestions


def _default_category_for_subcat(nova_subcat: str, mapping_df: pd.DataFrame):
    if not nova_subcat:
        return None
    mask = (
        mapping_df["Nova SubCat"]
        .astype(str)
        .str.strip()
        .str.lower()
        .eq(str(nova_subcat).strip().lower())
    )
    match = mapping_df.loc[mask, "categoria_oficial"].head(1)
    return match.iloc[0] if not match.empty else None


def _extend_catalog_with_session_entries(mapping_df: pd.DataFrame):
    new_entries = st.session_state.get("new_mappings", [])
    if not new_entries:
        return mapping_df
    additions = pd.DataFrame(new_entries)
    if additions.empty:
        return mapping_df
    additions["k_original"] = additions["SubCat Original"].astype(str).map(norm_text)
    additions["k_nova"] = additions["Nova SubCat"].astype(str).map(norm_text)
    additions["k_categoria"] = additions["categoria_oficial"].astype(str).map(norm_text)
    combined = pd.concat([mapping_df, additions], ignore_index=True)
    combined = combined.drop_duplicates(
        subset=["k_original", "k_nova", "k_categoria"], keep="last"
    )
    return combined

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
st.markdown("## 🧭 Micro Classificador — v2")
st.caption(
    "Reclassificação por **catálogo determinístico** + **similaridade semântica (TF-IDF)**. "
    "Subcategorias **“Excluir”** são removidas do resultado final."
)

if "process_result" not in st.session_state:
    st.session_state["process_result"] = None
if "should_process" not in st.session_state:
    st.session_state["should_process"] = False
if "new_mappings" not in st.session_state:
    st.session_state["new_mappings"] = []

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
    mapping_df = _extend_catalog_with_session_entries(mapping_df)
    
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
uploaded_name = getattr(data_file, "name", "resultado_v2.xlsx")
upload_path = Path(uploaded_name)
download_base = upload_path.stem or "resultado_v2"
download_ext = upload_path.suffix or ".xlsx"
processed_filename = f"{download_base}_Processado_{timestamp_tag}{download_ext}"
st.markdown('<div class="card">', unsafe_allow_html=True)
st.write("Prévia (30 primeiras linhas):")
st.dataframe(df_in.head(30), use_container_width=True)
st.markdown('</div>', unsafe_allow_html=True)

# ---------- Etapa 3: Processar ----------
st.markdown("### 3) Processar")
process_placeholder = st.empty()
process_cols = st.columns([1, 1, 2])
with process_cols[0]:
    if st.button("🚀 Processar agora", type="primary", use_container_width=True):
        st.session_state["should_process"] = True
with process_cols[1]:
    disabled_reprocess = st.session_state["process_result"] is None
    if st.button("🔁 Reprocessar catálogo atualizado", use_container_width=True, disabled=disabled_reprocess):
        st.session_state["should_process"] = True

if st.session_state.get("should_process"):
    process_placeholder.info("⏳ Iniciando processamento...")
    progress = st.progress(0)
    status = st.empty()

    def progress_callback(fraction, text):
        progress.progress(fraction)
        status.info(text)

    try:
        start_run = time.time()
        df_final, baixa_conf, metrics, df_all = process_dataframe(
            df_in,
            mapping_df,
            hi_threshold=hi,
            lo_threshold=lo,
            progress_callback=progress_callback
        )
        elapsed = time.time() - start_run
    except Exception as exc:
        progress.empty()
        process_placeholder.error(f"❌ Erro durante o processamento: {exc}")
        st.session_state["should_process"] = False
        st.session_state["process_result"] = None
        st.stop()
    else:
        st.session_state["process_result"] = {
            "df_final": df_final,
            "baixa_conf": baixa_conf,
            "metrics": metrics,
            "df_all": df_all,
            "processed_filename": processed_filename,
        }
        progress.empty()
        status.success(f"✅ Concluído em {elapsed:.2f}s")
        st.session_state["should_process"] = False

result_bundle = st.session_state.get("process_result")
if result_bundle is None:
    st.info("Envie o arquivo e clique em **Processar agora** para gerar os painéis.")
    st.stop()

df_final = result_bundle["df_final"]
baixa_conf = result_bundle["baixa_conf"]
metrics = result_bundle["metrics"]
df_all = result_bundle["df_all"]
processed_filename = result_bundle.get("processed_filename", processed_filename)


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
        header = f"🔹 {fonte.upper()} ({len(subset)} registros)"
        with st.expander(header, expanded=False):
            if not subset.empty:
                df_show = subset.copy()
                cols_display = [c for c in cols_order if c in df_show.columns]
                st.dataframe(df_show[cols_display], use_container_width=True)
            else:
                st.info(f"Sem registros para '{fonte}'.")

# ---------- Curadoria das subcategorias "Manter" ----------
st.markdown("### 6) Curadoria e mapeamento direto das subcategorias com ação 'Manter'")

panel["__acao_norm"] = panel["acao"].astype(str).str.strip().str.lower()
manter_df = panel[panel["__acao_norm"] == "manter"].copy()

if manter_df.empty:
    st.success("Nenhuma subcategoria pendente — todas receberam classificação.")
else:
    resumo_manter = (
        manter_df.groupby("SubCat Original")
        .agg(
            Registros=("SubCat Original", "size"),
            Conf_media=("confianca", "mean"),
            Conf_max=("confianca", "max"),
        )
        .reset_index()
        .sort_values("Registros", ascending=False)
    )
    resumo_manter["Conf_media"] = resumo_manter["Conf_media"].round(3)
    resumo_manter["Conf_max"] = resumo_manter["Conf_max"].round(3)

    st.caption(
        "Priorize as subcategorias com mais ocorrências para reduzir o backlog. "
        "Após salvar um novo vínculo, clique em **Reprocessar catálogo atualizado** para aplicar o aprendizado."
    )

    st.dataframe(resumo_manter, use_container_width=True)

    if "active_pending_subcat" not in st.session_state:
        st.session_state["active_pending_subcat"] = None
    if "mapping_mode_choice" not in st.session_state:
        st.session_state["mapping_mode_choice"] = "Usar sugestões automáticas"

    col_select, col_action = st.columns(2, gap="large")

    with col_select:
        st.markdown("#### 1) Selecionar subcategoria original")
        pendentes = resumo_manter["SubCat Original"].tolist()
        default_idx = 0
        if st.session_state["active_pending_subcat"] in pendentes:
            default_idx = pendentes.index(st.session_state["active_pending_subcat"])
        selected_pending = st.selectbox(
            "Subcategoria com maior impacto",
            pendentes,
            index=default_idx,
            key="pending_subcat_select",
        )
        selected_row = resumo_manter[resumo_manter["SubCat Original"] == selected_pending].iloc[0]
        met_cols = st.columns(2)
        met_cols[0].metric("Registros", int(selected_row["Registros"]))
        met_cols[1].metric("Conf. média", float(selected_row["Conf_media"]))

        if st.button("Confirmar esta subcategoria", use_container_width=True):
            st.session_state["active_pending_subcat"] = selected_pending
            st.session_state["mapping_mode_choice"] = "Usar sugestões automáticas"
            if "suggestion_choice" in st.session_state:
                del st.session_state["suggestion_choice"]

    active_pending = st.session_state.get("active_pending_subcat")

    if active_pending:
        with col_action:
            st.markdown("#### 2) Como deseja mapear?")
            active_row = resumo_manter[resumo_manter["SubCat Original"] == active_pending].iloc[0]
            st.caption(
                f"Subcategoria ativa: **{active_pending}** ({int(active_row['Registros'])} registros)"
            )

            suggestions = _catalog_suggestions(active_pending, mapping_df, limit=5)
            mode_options = [
                "Usar sugestões automáticas",
                "Buscar no catálogo",
                "Cadastrar nova subcategoria",
            ]
            current_mode = st.session_state.get("mapping_mode_choice", mode_options[0])
            mode_cols = st.columns(len(mode_options))
            for option, col in zip(mode_options, mode_cols):
                with col:
                    is_selected = current_mode == option
                    btn_type = "primary" if is_selected else "secondary"
                    key = f"mode_btn_{option.replace(' ', '_').lower()}"
                    if st.button(option, key=key, type=btn_type, use_container_width=True):
                        current_mode = option
                        st.session_state["mapping_mode_choice"] = option

            selected_nova = None
            if current_mode == mode_options[0]:
                if suggestions:
                    suggestion_labels = [
                        f"{item['label']} (score {item['score']:.0f})" for item in suggestions
                    ]
                    chosen = st.radio(
                        "Escolha uma das sugestões (ordenadas por afinidade)",
                        suggestion_labels,
                        key="suggestion_choice",
                    )
                    if chosen:
                        idx = suggestion_labels.index(chosen)
                        selected_nova = suggestions[idx]["label"]
                else:
                    st.info("Sem sugestões fortes — tente outra abordagem.")
            elif current_mode == mode_options[1]:
                catalog_options = sorted(
                    mapping_df["Nova SubCat"].dropna().astype(str).str.strip().unique().tolist()
                )
                selected_nova = st.selectbox(
                    "Busque pela subcategoria padronizada",
                    catalog_options,
                    key="catalog_search_select",
                )
            else:
                manual_value = st.text_input(
                    "Descreva a nova subcategoria",
                    key="new_subcat_manual",
                )
                selected_nova = manual_value.strip() if manual_value else None

            categorias = sorted(mapping_df["categoria_oficial"].dropna().unique().tolist())
            if selected_nova:
                selected_nova = selected_nova.strip()
            default_cat = _default_category_for_subcat(selected_nova, mapping_df)
            default_idx = categorias.index(default_cat) if default_cat in categorias else 0
            categoria_escolhida = st.selectbox(
                "Categoria oficial destino",
                categorias,
                index=default_idx,
                key="categoria_destino_select",
            )

            can_submit = bool(active_pending and selected_nova and categoria_escolhida)
            if st.button(
                "Salvar mapeamento no catálogo",
                use_container_width=True,
                disabled=not can_submit,
            ):
                try:
                    _append_mapping_to_catalog(active_pending, selected_nova, categoria_escolhida)
                except Exception as exc:
                    st.error(f"Não foi possível gravar no Google Sheets: {exc}")
                else:
                    registro = {
                        "SubCat Original": active_pending,
                        "Nova SubCat": selected_nova,
                        "categoria_oficial": categoria_escolhida,
                    }
                    st.session_state["new_mappings"].append(registro)
                    novo_df = pd.DataFrame([registro])
                    novo_df["k_original"] = novo_df["SubCat Original"].astype(str).map(norm_text)
                    novo_df["k_nova"] = novo_df["Nova SubCat"].astype(str).map(norm_text)
                    novo_df["k_categoria"] = novo_df["categoria_oficial"].astype(str).map(norm_text)
                    mapping_df = pd.concat([mapping_df, novo_df], ignore_index=True)
                    st.success("Mapeamento adicionado! Reprocessar aplicará o novo catálogo.")
    else:
        with col_action:
            st.info("Selecione e confirme uma subcategoria para liberar esta etapa.")

    if st.session_state["new_mappings"]:
        st.markdown("#### Mapeamentos adicionados nesta sessão")
        historico_df = pd.DataFrame(st.session_state["new_mappings"])
        st.dataframe(historico_df, use_container_width=True)


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

st.caption("v2 — pronto para demonstração e coleta de feedbacks.")
