# -*- coding: utf-8 -*-
# pages/98_Restaurar_DB_de_Backup.py

import streamlit as st
from db import restore_db_from_github, DB_PATH

st.set_page_config(
    page_title="Restaurar banco de dados",
    page_icon="💾",
    layout="centered",
)

st.title("💾 Restaurar banco de dados a partir do backup do GitHub")

st.markdown(
    """
Esta página serve para **reconstruir o banco de dados interno** (`bts_clients.db`)
a partir dos arquivos CSV de backup que estão no repositório do GitHub.

Ela é útil principalmente quando o Streamlit Cloud recria o ambiente e o arquivo
`data/bts_clients.db` some.

Os arquivos usados para restaurar são:

- `backups/clients.csv`
- `backups/level_history.csv`
- `backups/daily_clients.csv`

> ⚠️ **Atenção:** o banco atual será apagado antes da restauração.
"""
)

st.divider()

if st.button("🔁 Restaurar banco de dados a partir do GitHub", type="primary"):
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
else:
    st.info(
        "Clique no botão acima **apenas** quando perceber que o histórico de "
        "clientes/níveis sumiu (por exemplo, após um reset do ambiente no Streamlit Cloud)."
    )
