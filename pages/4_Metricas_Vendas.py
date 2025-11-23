# -*- coding: utf-8 -*-
# pages/4_Metricas_Vendas.py

import streamlit as st
import pandas as pd
import re
from datetime import datetime, date
import plotly.express as px

# ─────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Métricas de Vendas — Born To Ski",
    page_icon="💵",
    layout="wide"
)

st.title("📊 Métricas de Vendas — Born To Ski")
st.caption("Base: export do EVO (arquivo de vendas em Excel)")

# ─────────────────────────────────────────────────────────
# UPLOAD DO ARQUIVO
# ─────────────────────────────────────────────────────────
uploaded_file = st.file_uploader(
    "Envie o arquivo de vendas exportado do EVO (.xlsx)",
    type=["xlsx"]
)

if not uploaded_file:
    st.info(
        """
        ▶ Exporte do EVO o relatório de vendas em Excel  
        ▶ Suba aqui o arquivo `.xlsx` para ver os gráficos e métricas.
        """
    )
    st.stop()

# ─────────────────────────────────────────────────────────
# FUNÇÕES DE TRATAMENTO
# ─────────────────────────────────────────────────────────
def extract_slots(descricao: str) -> int:
    """Converte a descrição do produto em número de slots."""
    if pd.isna(descricao):
        return 0

    text = str(descricao)
    up = text.upper()

    # planos recorrentes
    if "SEMESTRAL" in up:
        return 24
    if "TRIMESTRAL" in up:
        return 12
    if "MENSAL" in up:
        return 4

    # pacotes: "(X sessões)"
    m = re.search(r"\((\d+)\s*sess", text, flags=re.IGNORECASE)
    if m:
        return int(m.group(1))

    # avulsa
    if "AVULSA" in up:
        return 1

    return 0


@st.cache_data
def carregar_e_processar(arquivo):
    """Lê o Excel bruto, limpa e retorna:
       - df_valid: todas as vendas válidas, linha a linha
       - daily_full: consolidação diária sem filtros
    """
    df = pd.read_excel(arquivo)

    # remove testes
    df = df[~df["Descrição"].astype(str).str.contains("TESTE", case=False, na=False)].copy()

    # numéricos
    df["Valor"] = pd.to_numeric(df["Valor"], errors="coerce")
    df["Quantidade"] = pd.to_numeric(df.get("Quantidade", 1), errors="coerce").fillna(1)

    # slots
    df["slots_por_venda"] = df["Descrição"].apply(extract_slots)
    df["slots_total"] = df["slots_por_venda"] * df["Quantidade"]

    # data
    df["Data"] = pd.to_datetime(df["Data da venda"], errors="coerce").dt.date

    df_valid = df.dropna(subset=["Data", "Valor"])

    # consolidação diária geral (sem filtros)
    daily_full = (
        df_valid
        .groupby("Data", as_index=False)
        .agg(
            total_vendas=("Valor", "sum"),
            total_slots=("slots_total", "sum"),
        )
    )
    daily_full["ticket_medio"] = daily_full["total_vendas"] / daily_full["total_slots"]
    daily_full = daily_full.sort_values("Data")
    daily_full["vendas_acumuladas"] = daily_full["total_vendas"].cumsum()
    daily_full["slots_acumulados"] = daily_full["total_slots"].cumsum()
    daily_full["ticket_medio_acumulado"] = (
        daily_full["vendas_acumuladas"] / daily_full["slots_acumulados"]
    )

    return df_valid, daily_full

# ─────────────────────────────────────────────────────────
# PROCESSAMENTO E FILTROS
# ─────────────────────────────────────────────────────────
try:
    df_base, daily_full = carregar_e_processar(uploaded_file)
except Exception as e:
    st.error(f"Erro ao processar o arquivo: {e}")
    st.stop()

# --- Filtros (sidebar) ---
st.sidebar.header("Filtros — Métricas de Vendas")

# intervalo de datas disponível
min_date = df_base["Data"].min()
max_date = df_base["Data"].max()

date_range = st.sidebar.date_input(
    "Período",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
    f_date_from, f_date_to = date_range
else:
    f_date_from, f_date_to = min_date, max_date

# filtro por produto (Descrição)
produtos_unicos = sorted(df_base["Descrição"].astype(str).unique())
sel_produtos = st.sidebar.multiselect(
    "Produtos (Descrição)",
    options=produtos_unicos,
    default=produtos_unicos,
)

# aplica filtros na base linha a linha
mask = (df_base["Data"] >= f_date_from) & (df_base["Data"] <= f_date_to)

if sel_produtos:
    mask &= df_base["Descrição"].astype(str).isin(sel_produtos)

df_filtrado = df_base[mask].copy()

if df_filtrado.empty:
    st.warning("Nenhuma venda encontrada com os filtros atuais.")
    st.stop()

# consolidação diária APÓS os filtros
daily = (
    df_filtrado
    .groupby("Data", as_index=False)
    .agg(
        total_vendas=("Valor", "sum"),
        total_slots=("slots_total", "sum"),
    )
)
daily["ticket_medio"] = daily["total_vendas"] / daily["total_slots"]
daily = daily.sort_values("Data")
daily["vendas_acumuladas"] = daily["total_vendas"].cumsum()
daily["slots_acumulados"] = daily["total_slots"].cumsum()
daily["ticket_medio_acumulado"] = (
    daily["vendas_acumuladas"] / daily["slots_acumulados"]
)

# ─────────────────────────────────────────────────────────
# MÉTRICAS RESUMO (já filtradas)
# ─────────────────────────────────────────────────────────
col1, col2, col3 = st.columns(3)

with col1:
    st.metric(
        "🎯 Ticket médio acumulado (R$/slot)",
        f"{daily['ticket_medio_acumulado'].iloc[-1]:.2f}"
    )

with col2:
    st.metric(
        "💰 Vendas totais (R$)",
        f"{daily['total_vendas'].sum():,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    )

with col3:
    st.metric(
        "🎿 Slots totais vendidos",
        int(daily["total_slots"].sum())
    )

st.markdown("---")

# ─────────────────────────────────────────────────────────
# GRÁFICOS (com números em cima)
# ─────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Vendas por dia",
    "Ticket médio (dia)",
    "Ticket médio acumulado",
    "Slots por dia",
    "Vendas acumuladas (R$)",
])

with tab1:
    st.subheader("💰 Vendas por dia (R$)")
    fig_vendas = px.line(
        daily,
        x="Data",
        y="total_vendas",
        markers=True,
        labels={"Data": "Data", "total_vendas": "Vendas (R$)"},
    )
    # números acima dos pontos, com separador de milhar como ponto
    textos_vendas = [
        f"{v:,.0f}".replace(",", ".")  # 1.000 em vez de 1,000
        for v in daily["total_vendas"]
    ]
    fig_vendas.update_traces(
        mode="lines+markers+text",
        text=textos_vendas,
        textposition="top center"
    )
    # eixo Y começando em 0
    fig_vendas.update_yaxes(rangemode="tozero")
    st.plotly_chart(fig_vendas, use_container_width=True)

with tab2:
    st.subheader("🎯 Ticket médio por dia (R$/slot)")
    fig_tm = px.line(
        daily,
        x="Data",
        y="ticket_medio",
        markers=True,
        labels={"Data": "Data", "ticket_medio": "Ticket médio (R$/slot)"},
    )
    # texto sem casas decimais
    textos_tm = [f"{v:.0f}" for v in daily["ticket_medio"]]
    fig_tm.update_traces(
        mode="lines+markers+text",
        text=textos_tm,
        textposition="top center"
    )
    st.plotly_chart(fig_tm, use_container_width=True)

with tab3:
    st.subheader("📈 Ticket médio acumulado (R$/slot)")
    fig_tm_acum = px.line(
        daily,
        x="Data",
        y="ticket_medio_acumulado",
        markers=True,
        labels={"Data": "Data", "ticket_medio_acumulado": "Ticket médio acumulado (R$/slot)"},
    )
    # texto sem casas decimais
    textos_tm_acum = [f"{v:.0f}" for v in daily["ticket_medio_acumulado"]]
    fig_tm_acum.update_traces(
        mode="lines+markers+text",
        text=textos_tm_acum,
        textposition="top center"
    )
    st.plotly_chart(fig_tm_acum, use_container_width=True)

with tab4:
    st.subheader("🎿 Slots vendidos por dia")
    fig_slots = px.bar(
        daily,
        x="Data",
        y="total_slots",
        labels={"Data": "Data", "total_slots": "Slots vendidos"},
        text="total_slots",
    )
    fig_slots.update_traces(
        textposition="outside"
    )
    st.plotly_chart(fig_slots, use_container_width=True)
    
with tab5:
    st.subheader("📈 Vendas acumuladas no período (R$)")

    fig_vendas_acum = px.line(
        daily,
        x="Data",
        y="vendas_acumuladas",
        markers=True,
        labels={"Data": "Data", "vendas_acumuladas": "Vendas acumuladas (R$)"},
    )

    # texto acima dos pontos — formatado com ponto como separador de milhar
    textos_vendas_acum = [
        f"{v:,.0f}".replace(",", ".")  # ex: 30.450
        for v in daily["vendas_acumuladas"]
    ]

    fig_vendas_acum.update_traces(
        mode="lines+markers+text",
        text=textos_vendas_acum,
        textposition="top center"
    )

    # eixo Y começando em zero
    fig_vendas_acum.update_yaxes(rangemode="tozero")

    st.plotly_chart(fig_vendas_acum, use_container_width=True)

# ─────────────────────────────────────────────────────────
# TABELA DIÁRIA CONSOLIDADA
# ─────────────────────────────────────────────────────────
st.markdown("### 📋 Tabela diária consolidada (após filtros)")
st.dataframe(daily, use_container_width=True)


