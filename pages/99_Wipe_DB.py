# pages/99_Wipe_DB.py
import streamlit as st
from pathlib import Path

st.set_page_config(page_title="Wipe DB", page_icon="🧹", layout="centered")
st.title("🧹 Apagar banco de dados local (bts_clients.db)")

# BASE_DIR = raiz do projeto (pai da pasta 'pages')
BASE_DIR = Path(__file__).resolve().parent.parent
db_path = BASE_DIR / "data" / "bts_clients.db"

st.write("Caminho do DB:")
st.code(str(db_path))

if db_path.exists():
    st.success("Status: arquivo encontrado ✅")
else:
    st.warning("Status: arquivo ainda não existe (nada para apagar).")

if st.button("APAGAR ARQUIVO data/bts_clients.db", type="primary"):
    if db_path.exists():
        db_path.unlink()
        st.success("Arquivo apagado com sucesso! Na próxima execução o db será recriado do zero.")
    else:
        st.warning("Arquivo não encontrado — talvez já tenha sido removido.")
