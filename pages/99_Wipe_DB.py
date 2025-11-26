# pages/99_Wipe_DB.py

import streamlit as st
from pathlib import Path

from db import restore_db_from_github, DB_PATH

st.set_page_config(page_title="Manutenção do DB", page_icon="🧹", layout="centered")
st.title("🧹 Manutenção do banco de dados (bts_clients.db)")

st.caption("Aqui você pode APAGAR o arquivo local ou RESTAURAR a partir do backup no GitHub.")

tab_apagar, tab_restaurar = st.tabs(["Apagar DB local", "Restaurar DB do GitHub"])

# BASE_DIR = raiz do projeto (pai da pasta 'pages')
BASE_DIR = Path(__file__).resolve().parent.parent
db_path = BASE_DIR / "data" / "bts_clients.db"

with tab_apagar:
    st.subheader("Apagar arquivo local")

    st.write("Caminho do DB:")
    st.code(str(db_path))

    if db_path.exists():
        st.success("Status: arquivo encontrado ✅")
    else:
        st.warning("Status: arquivo ainda não existe (nada para apagar).")

    if st.button("🧹 APAGAR ARQUIVO data/bts_clients.db", type="primary"):
        if db_path.exists():
            db_path.unlink()
            st.success(
                "Arquivo apagado com sucesso! Na próxima execução o DB será recriado do zero."
            )
        else:
            st.warning("Arquivo não encontrado — talvez já tenha sido removido.")

with tab_restaurar:
    st.subheader("Restaurar DB a partir do backup no GitHub")

    st.markdown(
        """
Esta opção **apaga o banco local atual** e recria o arquivo `bts_clients.db`
usando os CSVs de backup que estão no repositório:

- `backups/clients.csv`
- `backups/level_history.csv`
- `backups/daily_clients.csv`

Use isso quando o Streamlit Cloud recriar o ambiente e o histórico sumir.
"""
    )

    st.warning(
        "⚠️ Atenção: o arquivo local será substituído pelo conteúdo dos CSVs do GitHub."
    )

    if st.button("💾 Restaurar banco de dados do GitHub", type="primary"):
        with st.spinner("Restaurando banco de dados a partir dos CSVs do GitHub..."):
            try:
                total = restore_db_from_github()
            except Exception as e:
                st.error(f"Erro ao restaurar: {e}")
            else:
                st.success(
                    f"Banco restaurado com sucesso! {total} linhas importadas.\n\n"
                    f"Arquivo SQLite: `{DB_PATH}`"
                )
