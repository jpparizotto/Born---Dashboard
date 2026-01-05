# -*- coding: utf-8 -*-
import streamlit as st
from db import (
    restore_db_from_github,
    backup_db_to_github,
    DB_PATH,
    restore_acidentes_from_github,
    backup_acidentes_to_github,
)

st.set_page_config(
    page_title="Backup / Restore — Born to Ski",
    page_icon="💾",
    layout="wide",
)

st.title("💾 Backup e Restauração — Born to Ski")

st.write(
    "Use esta página para restaurar o banco de dados principal e o arquivo de "
    "acidentes a partir dos backups no GitHub, ou para gerar novos backups manualmente."
)

# ─────────────────────────────────────────────────────────
# SEÇÃO 1 — RESTAURAR DB PRINCIPAL
# ─────────────────────────────────────────────────────────
st.header("🔁 Restaurar banco de dados principal (clientes / níveis)")

st.warning(
    "⚠️ Esta ação vai **apagar o arquivo local** de banco de dados "
    f"(`{DB_PATH}`) e recriá-lo com base nos arquivos de backup no GitHub.\n\n"
    "Use somente se você tiver certeza de que o backup está correto."
)

if st.button("🔁 Restaurar banco de dados do GitHub"):
    with st.spinner("Restaurando banco a partir do GitHub..."):
        try:
            total = restore_db_from_github()
        except Exception as e:
            st.error(f"Erro ao restaurar banco de dados: {e}")
        else:
            st.success(
                f"Banco restaurado com sucesso a partir do GitHub! "
                f"({total} linhas importadas) "
                "Recarregue as outras páginas para ver os dados atualizados."
            )

# ─────────────────────────────────────────────────────────
# SEÇÃO 2 — BACKUP MANUAL DB PRINCIPAL
# ─────────────────────────────────────────────────────────
st.markdown("---")
st.header("📤 Gerar backup manual do banco principal (clientes / níveis)")

st.write(
    "Depois de atualizar a **Base de Clientes** e a **Evolução de Nível**, "
    "clique abaixo para enviar um snapshot completo do banco de dados para o GitHub."
)

if st.button("📤 Gerar backup completo do banco no GitHub"):
    with st.spinner("Gerando backup e enviando para o GitHub..."):
        try:
            res = backup_db_to_github()
        except Exception as e:
            st.error(f"Erro ao fazer backup do banco: {e}")
        else:
            st.success("Backup enviado para o GitHub com sucesso!")
            # mostra o commit sha do 1º arquivo (só pra comprovar)
            any_table = next(iter(res.keys()))
            sha = res[any_table].get("commit", {}).get("sha")
            if sha:
                st.caption(f"Commit: {sha}")

# ─────────────────────────────────────────────────────────
# SEÇÃO 3 — RESTAURAR ARQUIVO DE ACIDENTES
# ─────────────────────────────────────────────────────────
st.markdown("---")
st.header("🚑 Restaurar arquivo de acidentes")

st.write(
    "Se o relatório de acidentes sumir ou for zerado, você pode restaurar o "
    "arquivo `acidentes.csv` a partir do backup no GitHub."
)

if st.button("🚑 Restaurar arquivo de acidentes do GitHub"):
    with st.spinner("Restaurando arquivo de acidentes a partir do GitHub..."):
        try:
            n = restore_acidentes_from_github()
        except Exception as e:
            st.error(f"Erro ao restaurar arquivo de acidentes: {e}")
        else:
            st.success(
                f"Arquivo de acidentes restaurado com sucesso a partir do GitHub! "
                f"({n} linhas). Recarregue a página de reporte de acidentes para ver os dados."
            )

# ─────────────────────────────────────────────────────────
# SEÇÃO 4 — BACKUP MANUAL ARQUIVO DE ACIDENTES
# ─────────────────────────────────────────────────────────
st.markdown("---")
st.header("📤 Gerar backup manual do arquivo de acidentes")

st.write(
    "Além do backup automático ao registrar um acidente, você também pode forçar "
    "um backup manual do arquivo de acidentes atual."
)

if st.button("📤 Gerar backup manual de acidentes no GitHub"):
    with st.spinner("Enviando arquivo de acidentes para o GitHub..."):
        try:
            backup_acidentes_to_github()
        except Exception as e:
            st.error(f"Erro ao fazer backup de acidentes: {e}")
        else:
            st.success(
                "Backup do arquivo de acidentes enviado para o GitHub com sucesso!"
            )
