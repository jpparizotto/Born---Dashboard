# -*- coding: utf-8 -*-
# pages/4_Metricas_Vendas.py

import streamlit as st
import pandas as pd
import re
from datetime import datetime
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
        ▶ Exporta do EVO o relatório de vendas em Excel  
        ▶ Sobe aqui o arquivo `.xlsx` para ver os gráficos e métricas.
        """
    )
    st.stop()

# ─────────────────────────────────────────────────────────
# FUNÇÕES DE TRATAMENTO
# ─────────────────────────────────────────────────────────
def extract_slots(descricao: str) -> int:
    if pd.isna(descricao):
        return 0

    text = str(descricao)
    up = text.upper()

    if "SEMESTRAL" in up:
        return 24
    if "TRIMESTRAL" in up:
        return 12
    if "MENSAL" in up:
        return 4

    m = re.search(r"\((\d+)\s*sess", text, flags=re.IGNORECASE)
    if m:
        return int(m.group(1))

    if "AVULSA" in up:
        return 1

    return 0


@st.cache_data
def carregar_e_processar(arquivo) -> pd.DataFrame:
    df = pd.read_excel(arquivo)

    df = df[~df["Descrição"].astype(str).str.contains("TESTE", case=False, na=False)].copy()

    df["Valor"] = pd.to_numeric(df["Valor"], errors="coerce")
    df["Quantidade"] = pd.to_numeric(df.get("Quantidade", 1), errors="coerce").fillna(1)

    df["slots_por_venda"] = df["Descrição"].apply(extract_slots)
    df["slots_total"] = df["slots_por_venda"] * df["Quantidade"]

    df["Data"] = pd.to_datetime(df["Data da venda"], errors="coerce").dt.date

    df_valid = df.dropna(subset=["Data", "Valor"])

    daily = (
        df_valid
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
    daily["ticket_medio_acumulado"] = daily["vendas_acumuladas"] / daily["slots_acumulados"]

    return df_valid, daily

# ─────────────────────────────────────────────────────────
# PROCESSAMENTO
# ─────────────────────────────────────────────────────────
try:
    df_base, daily = carregar_e_processar(uploaded_file)
except Exception as e:
    st.error(f"Erro ao processar o arquivo: {e}")
    st.stop()

# ─────────────────────────────────────────────────────────
# MÉTRICAS RESUMO
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
# GRÁFICOS
# ─────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs([
    "Vendas por dia",
    "Ticket médio (dia)",
    "Ticket médio acumulado",
    "Slots por dia",
])

with tab1:
    st.subheader("💰 Vendas por dia (R$)")
    st.plotly_chart(
        px.line(daily, x="Data", y="total_vendas", markers=True),
        use_container_width=True
    )

with tab2:
    st.subheader("🎯 Ticket médio por dia (R$/slot)")
    st.plotly_chart(
        px.line(daily, x="Data", y="ticket_medio", markers=True),
        use_container_width=True
    )

with tab3:
    st.subheader("📈 Ticket médio acumulado (R$/slot)")
    st.plotly_chart(
        px.line(daily, x="Data", y="ticket_medio_acumulado", markers=True),
        use_container_width=True
    )

with tab4:
    st.subheader("🎿 Slots vendidos por dia")
    st.plotly_chart(
        px.bar(daily, x="Data", y="total_slots"),
        use_container_width=True
    )

# ─────────────────────────────────────────────────────────
# TABELA DETALHADA
# ─────────────────────────────────────────────────────────
st.markdown("### 📋 Tabela diária consolidada")
st.dataframe(daily, use_container_width=True)

