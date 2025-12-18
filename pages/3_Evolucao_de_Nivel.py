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
        SELECT
            evo_id,
            nome_limpo,
            nome_bruto,
            sexo,
            nivel_atual,
            nivel_ordem,
            nivel_sk,
            nivel_sk_ordem,
            nivel_sb,
            nivel_sb_ordem,
            nivel_sem_designacao,
            nivel_sem_designacao_ordem
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
for col in [
    "nivel_atual",
    "nivel_sk",
    "nivel_sb",
    "nivel_sem_designacao",
]:
    if col in df_clients.columns:
        df_clients[col] = df_clients[col].replace("", None)

# Contagens (agora por modalidade)
mask_sk = df_clients["nivel_sk"].notna()
mask_sb = df_clients["nivel_sb"].notna()
mask_sd = df_clients["nivel_sem_designacao"].notna()
mask_any = mask_sk | mask_sb | mask_sd

total_ski = int(mask_sk.sum())
total_snow = int(mask_sb.sum())
total_sem_designacao = int(mask_sd.sum())
total_sem_nivel = int((~mask_any).sum())
if df_clients.empty:
    st.info(
        "Nenhum cliente encontrado no banco.\n\n"
        "Vá primeiro em **'Base de Clientes'** → sincronize com o EVO e depois clique no botão "
        "**'Sincronizar clientes com banco interno'**."
    )
    st.stop()

# Nome amigável para o select na aba "Por cliente"
def _mk_label(r):
    sk = r.get("nivel_sk")
    sb = r.get("nivel_sb")
    sd = r.get("nivel_sem_designacao")
    bits = []
    if pd.notna(sk):
        bits.append(f"SK {sk}")
    if pd.notna(sb):
        bits.append(f"SB {sb}")
    if pd.notna(sd):
        bits.append(f"ND {sd}")
    return f"{r['nome_limpo']} ({' | '.join(bits) if bits else 'sem nível'})"

df_clients["label"] = df_clients.apply(_mk_label, axis=1)

# ---------------------------------------------------------------------------
# Abas: Visão geral  /  Por cliente
# ---------------------------------------------------------------------------
tab_visao, tab_cliente = st.tabs(["Visão geral", "Por cliente"])

# ===========================================================================
# ABA 1: VISÃO GERAL
# ===========================================================================
with tab_visao:
    st.subheader("📊 Distribuição de níveis — agora separado por Ski/Snow")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Clientes com nível SK", f"{total_ski:,}".replace(",", "."))
    c2.metric("Clientes com nível SB", f"{total_snow:,}".replace(",", "."))
    c3.metric("Nível sem designação", f"{total_sem_designacao:,}".replace(",", "."))
    c4.metric("Sem nível", f"{total_sem_nivel:,}".replace(",", "."))

    def _plot_dist(df_src: pd.DataFrame, col_level: str, title: str):
        if df_src.empty:
            st.info("Nenhum cliente nesta seção.")
            return

        df_dist = (
            df_src
            .groupby(col_level, dropna=False)
            .size()
            .reset_index(name="qtd")
            .rename(columns={col_level: "nivel"})
        )

        df_dist["nivel"] = df_dist["nivel"].fillna("0").astype(str)
        all_levels = ["0"] + LEVELS
        df_dist = (
            df_dist.groupby("nivel", as_index=False)["qtd"].sum()
                   .set_index("nivel")
                   .reindex(all_levels, fill_value=0)
                   .reset_index()
        )
        df_dist["nivel"] = pd.Categorical(df_dist["nivel"], categories=all_levels, ordered=True)

        fig = px.bar(
            df_dist,
            x="nivel",
            y="qtd",
            title=title,
            labels={"nivel": "Nível", "qtd": "Quantidade de clientes"},
        )
        fig.update_traces(text=df_dist["qtd"], textposition="outside")
        fig.update_layout(xaxis_title="Nível", yaxis_title="Clientes", uniformtext_minsize=8, uniformtext_mode="hide")
        st.plotly_chart(fig, use_container_width=True)
        st.caption("Tabela de apoio")
        st.dataframe(df_dist.reset_index(drop=True), use_container_width=True, height=260)

    tsk, tsb, tnd, tnone = st.tabs(["Ski (SK)", "Snowboard (SB)", "Sem designação", "Sem nível"])

    with tsk:
        _plot_dist(df_clients[df_clients["nivel_sk"].notna()].copy(), "nivel_sk", "Distribuição de níveis — Ski (SK)")

    with tsb:
        _plot_dist(df_clients[df_clients["nivel_sb"].notna()].copy(), "nivel_sb", "Distribuição de níveis — Snowboard (SB)")

    with tnd:
        _plot_dist(
            df_clients[df_clients["nivel_sem_designacao"].notna()].copy(),
            "nivel_sem_designacao",
            "Distribuição de níveis — sem designação (não veio SK/SB no nome)",
        )

    with tnone:
        st.info(
            "Clientes que não têm nenhum nível identificado no nome (nem SK, nem SB, nem genérico)."
        )
        df_none = df_clients[~(df_clients["nivel_sk"].notna() | df_clients["nivel_sb"].notna() | df_clients["nivel_sem_designacao"].notna())].copy()
        st.dataframe(
            df_none[["evo_id", "nome_limpo", "nome_bruto", "sexo"]].reset_index(drop=True),
            use_container_width=True,
            height=380,
        )

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
                lh.modalidade,
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
        # Filtro de modalidade (quando existir)
        if "modalidade" in df_all.columns:
            mods = [m for m in sorted(df_all["modalidade"].dropna().unique().tolist()) if str(m).strip()]
            sel_mods = st.multiselect(
                "Filtrar modalidade",
                options=mods,
                default=mods,
                help="GERAL = melhor nível encontrado; SK = ski; SB = snowboard; SEM_DESIGNACAO = nível sem SK/SB",
            )
            if sel_mods:
                df_all = df_all[df_all["modalidade"].isin(sel_mods)].copy()

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
                modalidade,
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
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("SK (Ski)", sel_row.get("nivel_sk") or "—")
    with col2:
        st.metric("SB (Snow)", sel_row.get("nivel_sb") or "—")
    with col3:
        st.metric("Sem designação", sel_row.get("nivel_sem_designacao") or "—")
    with col4:
        total_mudancas = len(df_hist)
        st.metric("Registros no histórico", int(total_mudancas))

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
        # filtro de modalidade
        if "modalidade" in df_hist.columns:
            mods = [m for m in sorted(df_hist["modalidade"].dropna().unique().tolist()) if str(m).strip()]
            sel_mods = st.multiselect(
                "Modalidade",
                options=mods,
                default=mods,
                help="GERAL = melhor nível; SK = ski; SB = snowboard; SEM_DESIGNACAO = nível sem SK/SB",
            )
            if sel_mods:
                df_hist = df_hist[df_hist["modalidade"].isin(sel_mods)].copy()

        # Ordena por data e prepara para o gráfico
        df_hist_plot = df_hist.sort_values("data").copy()

        # Converte data em datetime
        df_hist_plot["data_dt"] = pd.to_datetime(
            df_hist_plot["data"], errors="coerce"
        )

        # Converte nível textual (1A, 3C, etc.) em índice 0..15
        df_hist_plot["nivel_idx"] = df_hist_plot["nivel"].map(LEVEL_INDEX)

        # Gráfico em degrau (com cor por modalidade)
        fig = px.line(
            df_hist_plot,
            x="data_dt",
            y="nivel_idx",
            color="modalidade" if "modalidade" in df_hist_plot.columns else None,
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
        cols_show = ["data", "modalidade", "nivel", "origem", "created_at"] if "modalidade" in df_hist.columns else ["data", "nivel", "origem", "created_at"]
        df_show_cli = df_hist[cols_show].copy()
        df_show_cli.rename(
            columns={
                "data": "Data",
                "modalidade": "Modalidade",
                "nivel": "Nível",
                "origem": "Origem",
                "created_at": "Registrado em",
            },
            inplace=True,
        )
        st.dataframe(df_show_cli, use_container_width=True, height=300)
