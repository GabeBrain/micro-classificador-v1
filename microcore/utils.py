import re
import unicodedata
import pandas as pd

PT_STOP = {
    "de","da","do","das","dos","e","a","o","os","as","para","por","em","no","na","nos","nas",
    "com","sem","um","uma","uns","umas","loja","empresa","servicos","serviços","centro","shopping"
}

def strip_accents(text: str) -> str:
    if not isinstance(text, str):
        text = str(text) if pd.notna(text) else ""
    return "".join(
        ch for ch in unicodedata.normalize("NFKD", text)
        if not unicodedata.combining(ch)
    )

def norm_text(text: str) -> str:
    """normalização leve p/ matching: lower, sem acento, collapse espaços; preserva vírgulas fora daqui."""
    t = strip_accents(text).lower()
    t = re.sub(r"[^\w\s,.-]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def contains_any(hay: str, needles: list[str]) -> bool:
    hay_n = norm_text(hay)
    return any(n in hay_n for n in needles if n)

def safe_lower(s):
    return s.lower().strip() if isinstance(s, str) else s
