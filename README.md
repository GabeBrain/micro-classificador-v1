# Micro Classificador - v2

Aplicativo em Streamlit para padronizar e reclassificar estabelecimentos em lote (planilhas XLSX), combinando:

- regras deterministicas baseadas em catalogo;
- validacao/inferencia semantica leve (TF-IDF);
- curadoria humana no proprio app para evoluir o catalogo.

## Objetivo

O objetivo do classificador e reduzir inconsistencias de `Categoria` e `Sub-Categoria` em bases operacionais, com foco em:

- aumentar cobertura de classificacao automatica;
- manter rastreabilidade da decisao (`fonte`, `acao`, `confianca`);
- permitir aprendizado continuo via inclusao de novos mapeamentos no Google Sheets.

## Metodologia de classificacao (logica detalhada)

O pipeline esta em `microcore/pipeline.py` e segue etapas sequenciais.

### 1) Normalizacao e preparacao

- Garante colunas minimas: `Categoria`, `Sub-Categoria`, `Nome`, `Endereco`.
- Preserva valores originais em colunas de auditoria (`Cat Original`, `SubCat Original`).
- Normaliza texto com:
  - minusculas;
  - remocao de acentos;
  - limpeza de caracteres especiais;
  - normalizacao de espacos.
- Remove prefixos comuns de descricao (`Loja de`, `Posto de`, `Servicos de`, etc.) para reduzir ruido.

### 2) Match deterministico exato (catalogo)

- Compara a `Sub-Categoria` de entrada normalizada com `SubCat Original` do catalogo.
- Quando encontra match:
  - atualiza `Sub-Categoria` para `Nova SubCat`;
  - define acao:
    - `Excluir` se a subcategoria destino for `Excluir`;
    - `Corrigir` nos demais casos;
  - define `fonte = catalogo`;
  - define `confianca = 0.99`.
- Aplica guard-rail: ajusta `Categoria` para a `categoria_oficial` associada a subcategoria final.

### 3) Match deterministico por contains

- Para registros ainda nao classificados, tenta encontrar termos do catalogo no conteudo do registro.
- Quando encontra:
  - aplica `Nova SubCat`;
  - define `fonte = catalogo-contains`;
  - define `confianca = 0.90`;
  - aplica o mesmo guard-rail de categoria.

### 4) Validador semantico em registros ja mapeados

- Reavalia registros classificados por catalogo (`catalogo` e `catalogo-contains`) usando similaridade TF-IDF entre `Nome` e o universo de `SubCat Original`.
- Se houver divergencia relevante entre classificacao atual e predicao semantica:
  - pode substituir para a classe prevista;
  - define `fonte = semantico-validador`;
  - define `acao = Corrigir` (ou `Verificar` em caso especifico).
- Regra especifica para categorias problematicas (`cooperativa de credito`, `operadora de telefonia`) com limiar mais permissivo para correcao e marcacao para revisao quando a similaridade e muito baixa.

### 5) Inferencia semantica para pendentes

- Para registros sem acao ate aqui, calcula similaridade TF-IDF contra o universo de `Nova SubCat`.
- Texto de consulta:
  - padrao: `Nome + Endereco + Categoria`;
  - se subcategoria estiver vazia, `Nome` recebe peso maior.
- Decisao por limiar baixo (`lo_threshold`):
  - `sim >= lo_threshold`: aplica predicao com `acao = Inferir`, `fonte = semantico`;
  - `sim < lo_threshold`: mantem original com `acao = Manter`, `fonte = nenhum`.

### 6) Regra fixa de exclusao por endereco

- Se `Endereco` contem padroes como `shopping`, `loja`, `lj`, `ponto`, `conj`, `conjunto`:
  - forca `Sub-Categoria = Excluir`;
  - define `acao = Excluir`;
  - define `fonte = regra-endereco`;
  - define `confianca = 1.0`.

### 7) Dedupe, metricas e saida

- Remove duplicatas por conjunto de colunas-chave.
- Gera metricas operacionais:
  - total processado;
  - volume por fonte;
  - mantidos;
  - excluidos;
  - baixa confianca.
- Exporta:
  - aba `final` (resultado consolidado);
  - aba `baixa_confianca` (casos semanticos abaixo do limiar alto).

## Limiar de confianca

No app existem dois limiares configuraveis:

- `hi_threshold` (alta confianca): usado para destacar baixa confianca no relatorio semantico.
- `lo_threshold` (minimo para aplicar): controla quando uma inferencia/correcao semantica e aplicada.

## Curadoria e aprendizado continuo

Pelo proprio app e possivel:

- listar subcategorias pendentes (`acao = Manter`);
- escolher sugestao automatica, buscar no catalogo ou cadastrar nova subcategoria;
- salvar novo vinculo no Google Sheets;
- reprocessar para aplicar o novo aprendizado na mesma sessao.

## Fonte de catalogo

O catalogo e carregado do Google Sheets definido em `microcore/catalog_loader.py`, em abas por categoria oficial:

- Alimentacao
- Automotivo
- Servicos
- Decoracao
- Moda
- Educacao
- Inst. Financeira
- Saude e Bem Estar
- Outros

## Estrutura do projeto

```text
.
|-- app_streamlit.py
|-- microcore/
|   |-- pipeline.py
|   |-- catalog_loader.py
|   `-- utils.py
|-- assets/
|-- requirements.txt
`-- .streamlit/config.toml
```

## Requisitos

- Python 3.11+ (recomendado)
- Dependencias em `requirements.txt`

## Execucao local

```bash
python -m venv .venv

# Linux/macOS
source .venv/bin/activate

# Windows PowerShell
# .venv\Scripts\Activate.ps1

pip install -r requirements.txt
streamlit run app_streamlit.py
```

## Formato de entrada esperado

Arquivo XLSX com colunas operacionais, especialmente:

- `ID`
- `Nome`
- `Categoria`
- `Sub-Categoria`
- `Endereco`

Se alguma coluna essencial nao existir, o pipeline cria coluna vazia para manter a execucao.

## Saida gerada

- Arquivo XLSX processado com classificacao consolidada.
- Campos de auditoria, incluindo:
  - `Cat Original`
  - `SubCat Original`
  - `SubCat Catalogada` (quando aplicavel)
  - `acao`
  - `fonte`
  - `confianca`

## Observacoes

- A escrita de novos mapeamentos no Google Sheets usa `gspread` + credenciais em `st.secrets["gcp_service_account"]`.
- Sem credenciais, o processamento funciona, mas a gravacao no catalogo fica indisponivel.
