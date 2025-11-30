# -*- coding: utf-8 -*-
import streamlit as st
from db import restore_db_from_github, backup_db_to_github, DB_PATH

# Pode repetir set_page_config aqui sem problemas
st.set_page_config(
    page_title="Backup / Restore — Born to Ski",
    page_icon="💾",
    layout="wide",
)

st.title("💾 Backup e Restauração do Banco de Dados")

st.write(
    "Use esta página para restaurar o banco de dados a partir do backup no GitHub "
    "ou para gerar um novo backup manualmente."
)

# ─────────────────────────────────────────────────────────
# SEÇÃO 1 — RESTAURAR DO GITHUB
# ─────────────────────────────────────────────────────────
st.header("🔁 Restaurar banco de dados a partir do GitHub")

st.warning(
    "⚠️ Esta ação vai **apagar o arquivo local** de banco de dados "
    f"(`{DB_PATH}`) e recriá-lo com base nos arquivos de backup no GitHub.\n\n"
    "Use somente se você tiver certeza de que o backup está correto."
)

if st.button("🔁 Restaurar banco de dados do GitHub"):
    with st.spinner("Restaurando banco a partir do GitHub..."):
        try:
            restore_db_from_github()
        except Exception as e:
            st.error(f"Erro ao restaurar banco de dados: {e}")
        else:
            st.success(
                "Banco restaurado com sucesso a partir do GitHub! "
                "Recarregue as outras páginas para ver os dados atualizados."
            )

st.markdown("---")

# ─────────────────────────────────────────────────────────
# SEÇÃO 2 — BACKUP MANUAL PARA O GITHUB
# ─────────────────────────────────────────────────────────
st.header("📤 Gerar backup manual agora")

st.write(
    "Sempre que você fizer uma atualização importante na base de clientes ou na "
    "evolução de nível, clique abaixo para enviar um snapshot completo do banco "
    "de dados para o GitHub."
)

if st.button("📤 Gerar backup completo no GitHub"):
    with st.spinner("Gerando backup e enviando para o GitHub..."):
        try:
            backup_db_to_github()
        except Exception as e:
            st.error(f"Erro ao fazer backup: {e}")
        else:
            st.success(
                "Backup concluído e enviado para o GitHub com sucesso! "
                "Se precisar restaurar no futuro, use o botão de restauração acima."
            )

