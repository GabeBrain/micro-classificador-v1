# Micro Classificador — MVP v1

MVP para reclassificar registros por **catálogo determinístico** + **similaridade semântica leve (TF-IDF)**. 
Subcategorias "Excluir" são removidas do resultado final.

## Rodar local
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
streamlit run app_streamlit.py
