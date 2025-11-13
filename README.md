# Micro Classificador — v2

Reclassificador por **catálogo determinístico** + **similaridade semântica leve (TF-IDF)** com curadoria interativa das subcategorias mantidas.
Subcategorias "Excluir" são removidas do resultado final, e novas equivalências podem ser registradas direto no app (com envio automático para o Google Sheets).

## Rodar local
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
streamlit run app_streamlit.py
