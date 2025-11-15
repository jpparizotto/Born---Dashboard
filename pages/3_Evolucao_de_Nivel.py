# -*- coding: utf-8 -*-
# pages/3_Evolucao_de_Nivel.py
#
# Visualização da evolução de nível por cliente,
# usando as tabelas do banco interno (clients, level_history, member_sessions)

import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime

from db import get_conn, LEVELS

st.set_page_config(
    page_title="Evolução de Nível — Born to Ski",
    page_icon="📈",
    layout="wide",
)

st.title("📈 Evolução de Nível por Cliente")

# ----------------------------------------------------------------------
# Carrega dados do banco
# ----------------------------------------------------------------------
conn = get_conn()

# Tenta carregar tabela de clientes
try:
    df_clients = pd.read_sql_query(
        "SELECT id, evo_id, nome_bruto, nome_limpo, nivel_atual, nivel_ordem, "
        "sexo, cidade, bairro, uf, email, telefone, criado_em, updated_at "
        "FROM clients ORDER BY nome_limpo COLLATE NOCASE",
        conn,
    )
except Exception:
    st.error("Não foi possível ler a tabela 'clients'. Já rodou a página de Base de Clientes ao menos uma vez?")
    st.stop()

if df_clients.empty:
    st.info("Nenhum cliente encontrado no banco. Vá primeiro em 'Base de Clientes' para sincronizar com o EVO.")
    st.stop()

# ----------------------------------------------------------------------
# Seleção de cliente
# ----------------------------------------------------------------------
st.sidebar.header("Seleção de Cliente")

# Campo de busca
busca = st.sidebar.text_input("Buscar por nome, email ou ID EVO", "")

df_list = df_clients.copy()

if busca:
    termo = busca.lower()
    cols_busca = ["evo_id", "nome_bruto", "nome_limpo", "email"]
    df_list = df_list[
        df_list[cols_busca].astype(str).apply(
            lambda s: s.str.lower().str.contains(termo, na=False)
        ).any(axis=1)
    ]

if df_list.empty:
    st.warning("Nenhum cliente encontrado com esse filtro. Ajuste a busca.")
    st.stop()

df_list["label"] = df_list.apply(
    lambda r: f"{r['nome_limpo'] or r['nome_bruto'] or 'Sem nome'} (ID {r['evo_id']})",
    axis=1,
)

selected_label = st.sidebar.selectbox(
    "Escolha o cliente",
    df_list["label"].tolist(),
)

sel_row = df_list.loc[df_list["label"] == selected_label].iloc[0]
sel_evo_id = str(sel_row["evo_id"])
sel_cli_id = int(sel_row["id"])

st.subheader(f"Cliente selecionado: {sel_row['nome_limpo'] or sel_row['nome_bruto']}")

# ----------------------------------------------------------------------
# Carrega histórico de nível e sessões para o cliente
# ----------------------------------------------------------------------
df_hist = pd.read_sql_query(
    """
    SELECT
        id,
        data_evento,
        nivel,
        nivel_ordem,
        origem,
        created_at
    FROM level_history
    WHERE evo_id = ?
    ORDER BY date(data_evento) ASC, id ASC
    """,
    conn,
    params=[sel_evo_id],
)

df_sessions = pd.read_sql_query(
    """
    SELECT
        id,
        data,
        start_time,
        end_time,
        activity_name,
        area_name,
        status_activity,
        status_client,
        is_replacement,
        origem,
        created_at,
        updated_at
    FROM member_sessions
    WHERE evo_id = ?
    ORDER BY date(data) ASC, start_time ASC
    """,
    conn,
    params=[sel_evo_id],
)

conn.close()

# ----------------------------------------------------------------------
# Cards de resumo
# ----------------------------------------------------------------------
col_info1, col_info2, col_info3 = st.columns(3)

with col_info1:
    st.metric(
        "Nível atual (último gravado)",
        df_hist["nivel"].dropna().iloc[-1] if not df_hist.empty else (sel_row["nivel_atual"] or "—"),
    )

with col_info2:
    st.metric(
        "Total de mudanças de nível registradas",
        len(df_hist) if not df_hist.empty else 0,
    )

with col_info3:
    st.metric(
        "Total de aulas registradas (member_sessions)",
        len(df_sessions),
    )

# ----------------------------------------------------------------------
# Gráfico de evolução de nível
# ----------------------------------------------------------------------
st.divider()
st.subheader("Linha do tempo de níveis")

if df_hist.empty:
    st.info("Ainda não há histórico de nível para este cliente. \n\n"
            "Dica: altere o nível dele no EVO, depois rode 'Atualizar clientes agora' na Base de Clientes.")
else:
    # Converte data_evento para datetime
    df_hist_plot = df_hist.copy()
    df_hist_plot["data_evento_dt"] = pd.to_datetime(df_hist_plot["data_evento"], errors="coerce")

    # Garante ordenação por data + ordem interna
    df_hist_plot = df_hist_plot.sort_values(["data_evento_dt", "id"])

    # Para o eixo Y, usamos nivel_ordem se existir, senão mapeamos
    df_hist_plot["nivel_ordem_plot"] = df_hist_plot["nivel_ordem"]

    # Cria um dicionário de tickvals/ticktext baseado em LEVELS
    level_to_ord = {lvl: i for i, lvl in enumerate(LEVELS)}
    ord_to_level = {v: k for k, v in level_to_ord.items()}

    # Se algum nivel_ordem estiver vazio, tenta calcular
    df_hist_plot["nivel_ordem_plot"] = df_hist_plot.apply(
        lambda r: r["nivel_ordem"]
        if pd.notna(r["nivel_ordem"])
        else level_to_ord.get(str(r["nivel"]).upper(), None),
        axis=1,
    )

    # Remove linhas sem ordem válida
    df_hist_plot = df_hist_plot[df_hist_plot["nivel_ordem_plot"].notna()]

    if df_hist_plot.empty:
        st.info("Há registros de nível, mas nenhum com código reconhecido (1A-4D).")
    else:
        # Define ticks só para os níveis que realmente aparecem
        ord_values = sorted(df_hist_plot["nivel_ordem_plot"].unique())
        tickvals = ord_values
        ticktext = [ord_to_level.get(int(v), f"?{v}") for v in ord_values]

        fig = px.line(
            df_hist_plot,
            x="data_evento_dt",
            y="nivel_ordem_plot",
            markers=True,
            title="Evolução de nível ao longo do tempo",
            hover_data={
                "nivel": True,
                "origem": True,
                "data_evento_dt": False,
                "nivel_ordem_plot": False,
            },
        )
        fig.update_layout(
            xaxis_title="Data do evento",
            yaxis_title="Nível",
            yaxis=dict(
                tickmode="array",
                tickvals=tickvals,
                ticktext=ticktext,
            ),
        )
        st.plotly_chart(fig, use_container_width=True)

# ----------------------------------------------------------------------
# Tabelas detalhadas
# ----------------------------------------------------------------------
col_tab1, col_tab2 = st.columns(2)

with col_tab1:
    st.markdown("#### Histórico de níveis (level_history)")
    if df_hist.empty:
        st.caption("Nenhum registro ainda.")
    else:
        df_hist_show = df_hist.copy()
        df_hist_show["data_evento"] = pd.to_datetime(df_hist_show["data_evento"], errors="coerce").dt.date
        df_hist_show["created_at"] = pd.to_datetime(df_hist_show["created_at"], errors="coerce")
        st.dataframe(
            df_hist_show[
                ["data_evento", "nivel", "nivel_ordem", "origem", "created_at"]
            ].sort_values(["data_evento", "created_at"]),
            use_container_width=True,
            height=350,
        )

with col_tab2:
    st.markdown("#### Aulas do cliente (member_sessions)")
    if df_sessions.empty:
        st.caption("Nenhuma aula registrada para este cliente na tabela member_sessions.")
    else:
        df_sess_show = df_sessions.copy()
        df_sess_show["data"] = pd.to_datetime(df_sess_show["data"], errors="coerce").dt.date
        st.dataframe(
            df_sess_show[
                [
                    "data",
                    "start_time",
                    "end_time",
                    "activity_name",
                    "area_name",
                    "status_activity",
                    "status_client",
                    "is_replacement",
                ]
            ],
            use_container_width=True,
            height=350,
        )
