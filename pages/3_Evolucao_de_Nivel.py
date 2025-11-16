# -*- coding: utf-8 -*-
# pages/3_Evolucao_de_Nivel.py

import sqlite3

import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px

from db import get_connection, init_db_if_needed

st.set_page_config(page_title="Evolução de Nível por Cliente", page_icon="📈", layout="wide")
st.title("📈 Evolução de Nível por Cliente")

# Garante que as tabelas existam
init_db_if_needed()

# ---------------------------------------------------------------------------
# Carrega lista de clientes do banco
# ---------------------------------------------------------------------------
try:
    conn = get_connection()
    df_clients = pd.read_sql_query(
        """
        SELECT evo_id, nome_limpo, nome_bruto, nivel_atual, nivel_ordem
        FROM clients
        ORDER BY nome_limpo COLLATE NOCASE;
        """,
        conn,
    )
except sqlite3.Error as e:
    st.error("Erro ao ler clientes do banco interno.")
    st.exception(e)
    st.stop()
finally:
    conn.close()

if df_clients.empty:
    st.info("Nenhum cliente encontrado no banco. Vá primeiro em **'Base de Clientes'** → sincronize com o EVO e depois clique no botão **'Sincronizar clientes com banco interno'**.")
    st.stop()

# Nome amigável para o select
df_clients["label"] = df_clients.apply(
    lambda r: f"{r['nome_limpo']} ({r['nivel_atual'] or 'sem nível'})",
    axis=1,
)

# ---------------------------------------------------------------------------
# Seleção de cliente
# ---------------------------------------------------------------------------
sel_label = st.selectbox(
    "Escolha o cliente",
    df_clients["label"].tolist(),
)

sel_row = df_clients.loc[df_clients["label"] == sel_label].iloc[0]
sel_evo_id = sel_row["evo_id"]

st.markdown(f"**Cliente selecionado:** {sel_row['nome_limpo']}")

# ---------------------------------------------------------------------------
# Busca histórico de nível desse cliente
# ---------------------------------------------------------------------------
conn = get_connection()
try:
    df_hist = pd.read_sql_query(
        """
        SELECT
            data,
            nivel,
            nivel_ordem,
            origem,
            created_at
        FROM level_history
        WHERE evo_id = ?
        ORDER BY data, id;
        """,
        conn,
        params=[sel_evo_id],
    )
finally:
    conn.close()

# Métricas de topo
col1, col2 = st.columns(2)
with col1:
    nivel_atual = sel_row["nivel_atual"] or "Não definido"
    st.metric("Nível atual (último gravado)", nivel_atual)

with col2:
    total_mudancas = len(df_hist)
    st.metric("Total de mudanças de nível registradas", int(total_mudancas))

st.divider()

# ---------------------------------------------------------------------------
# Linha do tempo de níveis
# ---------------------------------------------------------------------------
st.subheader("Linha do tempo de níveis")

if df_hist.empty:
    st.info(
        "Ainda não há histórico de nível para este cliente.\n\n"
        "Dica: altere o nível dele no EVO e depois rode 'Atualizar clientes agora' "
        "na Base de Clientes."
    )
else:
    # Ordena por data e prepara para o gráfico
    df_hist_plot = df_hist.sort_values("data").copy()

    # Cria uma coluna datetime a partir de `data`
    df_hist_plot["data_dt"] = pd.to_datetime(
        df_hist_plot["data"], errors="coerce"
    )

    # Gráfico em degrau usando line + line_shape="hv"
    fig = px.line(
        df_hist_plot,
        x="data_dt",
        y="nivel_ordem",   # coluna que vem do banco
        title="Linha do tempo de níveis",
        markers=True,
        text="nivel",
    )

    fig.update_traces(line_shape="hv")  # deixa o gráfico com cara de degrau
    fig.update_layout(
        xaxis_title="Data",
        yaxis_title="Nível (ordem)",
    )

    st.plotly_chart(fig, use_container_width=True)

st.divider()

st.subheader("Histórico de níveis (level_history)")
if df_hist.empty:
    st.caption("Nenhum registro ainda.")
else:
    df_show = df_hist[["data", "nivel", "origem", "created_at"]].copy()
    st.dataframe(df_show, use_container_width=True, height=300)
