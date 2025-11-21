# -*- coding: utf-8 -*-
# pages/3_Evolucao_de_Nivel.py

import sqlite3
from datetime import date, timedelta

import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px

from db import get_connection, init_db_if_needed

st.set_page_config(page_title="Evolução de Nível por Cliente", page_icon="📈", layout="wide")
st.title("📈 Evolução de Nível")

# Garante que as tabelas existam
init_db_if_needed()

# ---------------------------------------------------------------------------
# Constantes de níveis
# ---------------------------------------------------------------------------
LEVELS = [
    "1A", "1B", "1C", "1D",
    "2A", "2B", "2C", "2D",
    "3A", "3B", "3C", "3D",
    "4A", "4B", "4C", "4D",
]
LEVEL_INDEX = {lvl: i for i, lvl in enumerate(LEVELS)}  # 1A=0, 1B=1, ...

# ---------------------------------------------------------------------------
# Carrega lista de clientes do banco (usado na aba "Por cliente")
# ---------------------------------------------------------------------------
try:
    conn = get_connection()
    df_clients = pd.read_sql_query(
        """
        SELECT evo_id, nome_limpo, nome_bruto, sexo, nivel_atual, nivel_ordem
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
# Normalização: string vazia vira None
df_clients["nivel_atual"] = df_clients["nivel_atual"].replace("", None)

# Contagens corretas
total_sem_nivel = df_clients["nivel_atual"].isna().sum()
total_com_nivel = df_clients["nivel_atual"].notna().sum()
if df_clients.empty:
    st.info(
        "Nenhum cliente encontrado no banco.\n\n"
        "Vá primeiro em **'Base de Clientes'** → sincronize com o EVO e depois clique no botão "
        "**'Sincronizar clientes com banco interno'**."
    )
    st.stop()

# Nome amigável para o select na aba "Por cliente"
df_clients["label"] = df_clients.apply(
    lambda r: f"{r['nome_limpo']} ({r['nivel_atual'] or 'sem nível'})",
    axis=1,
)

# ---------------------------------------------------------------------------
# Abas: Visão geral  /  Por cliente
# ---------------------------------------------------------------------------
tab_visao, tab_cliente = st.tabs(["Visão geral", "Por cliente"])

# ===========================================================================
# ABA 1: VISÃO GERAL
# ===========================================================================
with tab_visao:
    st.subheader("📊 Distribuição de níveis da base de alunos")

    # Monta a distribuição de níveis a partir de df_clients
    df_dist = (
        df_clients
        .assign(nivel=df_clients["nivel_atual"])
        .groupby("nivel", dropna=False)
        .size()
        .reset_index(name="qtd")
    )

    if df_dist.empty:
        st.info("Nenhum cliente encontrado na base.")
    else:
        # Trata sem nível como "0" SOMENTE para o gráfico/tabela
        df_dist["nivel"] = df_dist["nivel"].fillna("0").astype(str)

        all_levels = ["0"] + LEVELS  # LEVELS = ["1A","1B",...,"4D"]

        # agrega por segurança
        df_dist = (
            df_dist.groupby("nivel", as_index=False)["qtd"]
                   .sum()
        )

        # garante que todos os níveis existam (mesmo com 0 clientes)
        df_dist = (
            df_dist.set_index("nivel")
                   .reindex(all_levels, fill_value=0)
                   .reset_index()
        )

        # ordenação categórica
        df_dist["nivel"] = pd.Categorical(
            df_dist["nivel"],
            categories=all_levels,
            ordered=True,
        )

        # KPIs usando as métricas oficiais calculadas lá em cima
        colm1, colm2 = st.columns(2)
        with colm1:
            st.metric(
                "Clientes com nível definido",
                f"{total_com_nivel:,}".replace(",", "."),
            )
        with colm2:
            st.metric(
                "Clientes sem nível",
                f"{total_sem_nivel:,}".replace(",", "."),
            )

        # Gráfico
        fig_dist = px.bar(
            df_dist,
            x="nivel",
            y="qtd",
            title="Distribuição de níveis na base de clientes",
            labels={"nivel": "Nível", "qtd": "Quantidade de clientes"},
        )
        
        fig_dist.update_traces(text=df_dist["qtd"], textposition="outside")
        fig_dist.update_layout(
            xaxis_title="Nível",
            yaxis_title="Clientes",
            uniformtext_minsize=8,
            uniformtext_mode="hide"
        )
        
        st.plotly_chart(fig_dist, use_container_width=True)

        # Tabela
        st.caption("Tabela de apoio")
        st.dataframe(
            df_dist.reset_index(drop=True),
            use_container_width=True,
            height=260,
        )
    
    # ─────────────────────────────────────────────────────────────
    # GRÁFICOS ADICIONAIS
    # ─────────────────────────────────────────────────────────────
    st.subheader("📊 Visões adicionais de distribuição de nível")

    # Considera apenas quem tem nível definido
    df_com_nivel = df_clients[df_clients["nivel_atual"].notna()].copy()

    if df_com_nivel.empty:
        st.info("Nenhum cliente com nível definido para gerar os gráficos adicionais.")
    else:
        # Normaliza coluna de nível como categórica ordenada
        df_com_nivel["nivel"] = pd.Categorical(
            df_com_nivel["nivel_atual"],
            categories=LEVELS,
            ordered=True,
        )

        # 1) Gráfico de pizza (todos os clientes com nível)
        df_pizza = (
            df_com_nivel.groupby("nivel", as_index=False)
                        .size()
                        .rename(columns={"size": "qtd"})
                        .sort_values("nivel")
        )

        col_pizza, _ = st.columns(2)
        with col_pizza:
            fig_pizza = px.pie(
                df_pizza,
                names="nivel",
                values="qtd",
                title="Distribuição de níveis (apenas quem tem nível)",
            )
            fig_pizza.update_traces(textposition="inside",
                                    textinfo="label+percent+value")
            st.plotly_chart(fig_pizza, use_container_width=True)

        # 2) Gráfico de barras - apenas homens
        cols_genero = st.columns(2)

        with cols_genero[0]:
            df_homem = df_com_nivel[df_com_nivel["sexo"] == "Masculino"].copy()
            if df_homem.empty:
                st.info("Nenhum cliente masculino com nível definido.")
            else:
                df_homem_grp = (
                    df_homem.groupby("nivel", as_index=False)
                            .size()
                            .rename(columns={"size": "qtd"})
                            .sort_values("nivel")
                )
                fig_homem = px.bar(
                    df_homem_grp,
                    x="nivel",
                    y="qtd",
                    title="Distribuição de níveis — Masculino",
                    labels={"nivel": "Nível", "qtd": "Clientes"},
                )
                
                fig_homem.update_traces(text=df_homem_grp["qtd"], textposition="outside")
                fig_homem.update_layout(
                    xaxis_title="Nível",
                    yaxis_title="Clientes",
                    uniformtext_minsize=8,
                    uniformtext_mode="hide",
                )
                
                st.plotly_chart(fig_homem, use_container_width=True)

        # 3) Gráfico de barras - apenas mulheres
        with cols_genero[1]:
            df_mulher = df_com_nivel[df_com_nivel["sexo"] == "Feminino"].copy()
            if df_mulher.empty:
                st.info("Nenhuma cliente feminina com nível definido.")
            else:
                df_mulher_grp = (
                    df_mulher.groupby("nivel", as_index=False)
                             .size()
                             .rename(columns={"size": "qtd"})
                             .sort_values("nivel")
                )
                fig_mulher = px.bar(
                    df_mulher_grp,
                    x="nivel",
                    y="qtd",
                    title="Distribuição de níveis — Feminino",
                    labels={"nivel": "Nível", "qtd": "Clientes"},
                )
                    
                fig_mulher.update_traces(text=df_mulher_grp["qtd"], textposition="outside")
                fig_mulher.update_layout(
                    xaxis_title="Nível",
                    yaxis_title="Clientes",
                    uniformtext_minsize=8,
                    uniformtext_mode="hide",
                )
                    
                st.plotly_chart(fig_mulher, use_container_width=True)

    st.divider()
    st.subheader("🕒 Log de mudanças de nível (últimos 10 dias)")

    
    dias = 10  # se quiser, dá pra virar input depois
    cutoff = (date.today() - timedelta(days=dias)).isoformat()
    
    # 1) Busca TODO o histórico (precisamos ver o nível anterior de cada aluno)
    try:
        conn = get_connection()
        df_all = pd.read_sql_query(
            """
            SELECT
                lh.id,
                lh.data,
                lh.nivel,
                lh.nivel_ordem,
                lh.origem,
                lh.evo_id,
                c.nome_limpo
            FROM level_history AS lh
            LEFT JOIN clients AS c
                   ON c.evo_id = lh.evo_id
            ORDER BY lh.evo_id, lh.data, lh.id;
            """,
            conn,
        )
    finally:
        conn.close()
    
    if df_all.empty:
        st.info("Ainda não há nenhum registro em level_history.")
    else:
        # 2) Converte data e garante ordenação
        df_all["data_dt"] = pd.to_datetime(df_all["data"], errors="coerce")
        df_all = df_all.sort_values(["evo_id", "data_dt", "id"])
    
        # 3) Compara com o nível anterior de cada aluno
        ZERO_ACTIVATION_DATE = "2025-11-17"  # ajuste se quiser mudar a data depois
        # 3) Compara com o nível anterior de cada aluno
        df_all["nivel_prev"] = df_all.groupby("evo_id")["nivel"].shift(1)
        # Mudança "normal": havia nível anterior e mudou
        mask_mudanca_normal = df_all["nivel_prev"].notna() & (df_all["nivel"] != df_all["nivel_prev"])
        # Mudança de "sem nível" -> algum nível,
        # mas APENAS a partir de ZERO_ACTIVATION_DATE
        mask_de_zero_para_nivel = (
            df_all["nivel_prev"].isna()
            & df_all["nivel"].notna()
            & (df_all["data"] >= ZERO_ACTIVATION_DATE)
        )
        df_all["is_change"] = mask_mudanca_normal | mask_de_zero_para_nivel 
                
        # 4) Mantém só mudanças reais nos últimos X dias
        df_changes = df_all[df_all["is_change"] & (df_all["data"] >= cutoff)]
    
        if df_changes.empty:
            st.info(f"Nenhuma mudança de nível registrada nos últimos {dias} dias.")
        else:
            st.caption(f"Mostrando apenas mudanças reais de nível a partir de {cutoff} (inclusive).")
    
            colg1, colg2 = st.columns(2)
            with colg1:
                st.metric("Total de mudanças no período", int(len(df_changes)))
            with colg2:
                st.metric("Clientes diferentes afetados", df_changes["evo_id"].nunique())
    
            # Ordena do mais recente pro mais antigo só para exibir
            df_changes = df_changes.sort_values(["data_dt", "evo_id"], ascending=[False, True])
    
            # Tabela enxuta
            df_show = df_changes[["data", "nome_limpo", "evo_id", "nivel", "nivel_prev", "origem"]].copy()
            df_show.rename(
                columns={
                    "data": "Data",
                    "nome_limpo": "Cliente",
                    "evo_id": "EVO ID",
                    "nivel": "Nível novo",
                    "nivel_prev": "Nível anterior",
                    "origem": "Origem",
                },
                inplace=True,
            )
            
            # trata quem não tinha nível como "0"
            df_show["Nível anterior"] = df_show["Nível anterior"].fillna("0")
            
            st.dataframe(df_show, use_container_width=True, height=400)

    if df_changes.empty:
        st.info(f"Nenhuma mudança de nível registrada nos últimos {dias} dias.")
    else:
        st.caption(f"Mostrando mudanças a partir de {cutoff} (inclusive).")

        # Converte data em datetime para ordenação/visualização
        df_changes["data_dt"] = pd.to_datetime(df_changes["data"], errors="coerce")

        colg1, colg2 = st.columns(2)
        with colg1:
            st.metric("Total de mudanças no período", int(len(df_changes)))
        with colg2:
            st.metric("Clientes diferentes afetados", df_changes["evo_id"].nunique())

        # Ordena apenas para visual (já vem ordenado, mas garantimos)
        df_changes = df_changes.sort_values(["data_dt", "evo_id"], ascending=[False, True])

        # Mostra tabela enxuta
        df_show = df_changes[["data", "nome_limpo", "evo_id", "nivel", "origem"]].copy()
        df_show.rename(
            columns={
                "data": "Data",
                "nome_limpo": "Cliente",
                "evo_id": "EVO ID",
                "nivel": "Nível",
                "origem": "Origem",
            },
            inplace=True,
        )
        st.dataframe(df_show, use_container_width=True, height=400)

# ===========================================================================
# ABA 2: POR CLIENTE (tela que você já tinha)
# ===========================================================================
with tab_cliente:
    st.subheader("🔍 Evolução por cliente")

    # -----------------------------------------------------------------------
    # Seleção de cliente
    # -----------------------------------------------------------------------
    sel_label = st.selectbox(
        "Escolha o cliente",
        df_clients["label"].tolist(),
    )

    sel_row = df_clients.loc[df_clients["label"] == sel_label].iloc[0]
    sel_evo_id = sel_row["evo_id"]

    st.markdown(f"**Cliente selecionado:** {sel_row['nome_limpo']}")

    # -----------------------------------------------------------------------
    # Busca histórico de nível desse cliente
    # -----------------------------------------------------------------------
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

    # -----------------------------------------------------------------------
    # Linha do tempo de níveis
    # -----------------------------------------------------------------------
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

        # Converte data em datetime
        df_hist_plot["data_dt"] = pd.to_datetime(
            df_hist_plot["data"], errors="coerce"
        )

        # Converte nível textual (1A, 3C, etc.) em índice 0..15
        df_hist_plot["nivel_idx"] = df_hist_plot["nivel"].map(LEVEL_INDEX)

        # Gráfico em degrau
        fig = px.line(
            df_hist_plot,
            x="data_dt",
            y="nivel_idx",
            title="Linha do tempo de níveis",
            markers=True,
            text="nivel",
        )

        fig.update_traces(line_shape="hv")

        # Só mostra ticks para os níveis que aparecem na série
        niveis_usados = [
            lvl for lvl in LEVELS
            if lvl in df_hist_plot["nivel"].dropna().unique()
        ]

        fig.update_layout(
            xaxis_title="Data",
            yaxis_title="Nível",
            yaxis=dict(
                tickmode="array",
                tickvals=[LEVEL_INDEX[lvl] for lvl in niveis_usados],
                ticktext=niveis_usados,
            ),
        )

        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    st.subheader("Histórico de níveis (level_history)")
    if df_hist.empty:
        st.caption("Nenhum registro ainda.")
    else:
        df_show_cli = df_hist[["data", "nivel", "origem", "created_at"]].copy()
        df_show_cli.rename(
            columns={
                "data": "Data",
                "nivel": "Nível",
                "origem": "Origem",
                "created_at": "Registrado em",
            },
            inplace=True,
        )
        st.dataframe(df_show_cli, use_container_width=True, height=300)
