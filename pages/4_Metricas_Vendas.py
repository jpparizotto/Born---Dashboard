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
def detectar_coluna_cliente(df: pd.DataFrame) -> str | None:
    """Tenta descobrir automaticamente qual coluna é o nome do cliente."""
    candidatos = [
        "Cliente",
        "Aluno",
        "Aluno/Cliente",
        "Nome do aluno",
        "Nome do Aluno",
        "Nome do cliente",
        "Nome Cliente",
        "Pessoa",
    ]
    for c in candidatos:
        if c in df.columns:
            return c
    return None

def detectar_coluna_colaborador(df: pd.DataFrame) -> str | None:
    """
    Descobre automaticamente qual coluna é o colaborador / responsável.
    Usa busca por pedaços do nome (substring), não igualdade exata.
    """
    palavras_chave = [
        "comissão", "comissao",
        "colaborador",
        "responsável", "responsavel",
        "vendedor",
        "usuário", "usuario",
        "operador",
        "atendente",
        "consultor",
    ]

    for c in df.columns:
        nome_lower = c.lower()
        if any(p in nome_lower for p in palavras_chave):
            return c  # retorna o nome original da coluna

    return None

# ─────────────────────────────────────────────────────────
# PROCESSAMENTO E FILTROS
# ─────────────────────────────────────────────────────────
try:
    df_base, daily_full = carregar_e_processar(uploaded_file)

    # Criar coluna ClienteFull = Nome + Sobrenome
    cols = df_base.columns.str.lower()

    if ("nome" in cols) and ("sobrenome" in cols):
        col_nome = df_base.columns[cols == "nome"][0]
        col_sobrenome = df_base.columns[cols == "sobrenome"][0]

        df_base["ClienteFull"] = (
            df_base[col_nome].fillna("").astype(str).str.strip() + " " +
            df_base[col_sobrenome].fillna("").astype(str).str.strip()
        ).str.strip()

        df_base["ClienteFull"] = df_base["ClienteFull"].str.title()
    
    else:
        st.error("As colunas 'Nome' e 'Sobrenome' não foram encontradas no arquivo.")
        st.stop()
    
    # Detectar coluna de colaborador (vendedor / responsável)
    col_colab = detectar_coluna_colaborador(df_base)
    
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
# filtro por colaborador (se a coluna existir)
if col_colab:
    colaboradores_unicos = sorted(df_base[col_colab].astype(str).unique())
    sel_colaboradores = st.sidebar.multiselect(
        "Colaboradores (responsável pela venda)",
        options=colaboradores_unicos,
        default=colaboradores_unicos,
    )
else:
    sel_colaboradores = []

# aplica filtros na base linha a linha
mask = (df_base["Data"] >= f_date_from) & (df_base["Data"] <= f_date_to)

if sel_produtos:
    mask &= df_base["Descrição"].astype(str).isin(sel_produtos)

if col_colab and sel_colaboradores:
    mask &= df_base[col_colab].astype(str).isin(sel_colaboradores)

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
# JANELA MÓVEL: VENDAS NOS ÚLTIMOS 7 DIAS (terminando em cada data)
# ─────────────────────────────────────────────────────────
daily = daily.sort_values("Data").copy()

# Soma móvel de 7 dias (inclui o dia atual). min_periods=1 para início da série não ficar NaN
daily["vendas_ult_7d"] = daily["total_vendas"].rolling(window=7, min_periods=1).sum()
daily["vendas_ult_7d_mil"] = daily["vendas_ult_7d"] / 1000
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
    # converter valores para mil reais
    daily["total_vendas_mil"] = daily["total_vendas"] / 1000
    
    fig_vendas = px.line(
        daily,
        x="Data",
        y="total_vendas_mil",
        markers=True,
        labels={"Data": "Data", "total_vendas_mil": "Vendas (R$000)"},
    )
    
    # textos acima dos pontos — formatados em mil
    textos_vendas = [
        f"{v:,.1f}".replace(",", ".")
        for v in daily["total_vendas_mil"]
    ]
    
    fig_vendas.update_traces(
        mode="lines+markers+text",
        text=textos_vendas,
        textposition="top center"
    )
    
    # eixo Y começa em zero
    fig_vendas.update_yaxes(rangemode="tozero")
    # números acima dos pontos, com separador de milhar como ponto
    textos_vendas = [
        f"{v:,.1f}".replace(",", ".")
        for v in daily["total_vendas_mil"]
    ]
    fig_vendas.update_traces(
        mode="lines+markers+text",
        text=textos_vendas,
        textposition="top center"
    )
    # eixo Y começando em 0
    fig_vendas.update_yaxes(rangemode="tozero")
    st.plotly_chart(fig_vendas, use_container_width=True)
    st.markdown("#### 🗓️ Vendas nos últimos 7 dias (janela móvel) — R$")

    fig_vendas_ult7d = px.line(
        daily,
        x="Data",
        y="vendas_ult_7d_mil",
        markers=True,
        labels={"Data": "Data", "vendas_ult_7d_mil": "Vendas últimos 7 dias (R$000)"},
    )
    
    textos_ult7d = [
        f"{v:,.0f}".replace(",", ".")
        for v in daily["vendas_ult_7d_mil"]
    ]
    
    fig_vendas_ult7d.update_traces(
        mode="lines+markers+text",
        text=textos_ult7d,
        textposition="top center"
    )
    fig_vendas_ult7d.update_traces(
        textfont=dict(size=10)  # ajuste fino: 9, 10 ou 11 ficam bons
    )
    fig_vendas_ult7d.update_yaxes(rangemode="tozero")
    
    st.plotly_chart(fig_vendas_ult7d, use_container_width=True)

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

    # converter acumuladas para mil reais
    daily["vendas_acumuladas_mil"] = daily["vendas_acumuladas"] / 1000
    
    fig_vendas_acum = px.line(
        daily,
        x="Data",
        y="vendas_acumuladas_mil",
        markers=True,
        labels={"Data": "Data", "vendas_acumuladas_mil": "Vendas acumuladas (R$000)"},
    )
    
    # texto em cima dos pontos
    textos_vendas_acum = [
        f"{v:,.1f}".replace(",", ".")
        for v in daily["vendas_acumuladas_mil"]
    ]
    
    fig_vendas_acum.update_traces(
        mode="lines+markers+text",
        text=textos_vendas_acum,
        textposition="top center"
    )
    
    fig_vendas_acum.update_yaxes(rangemode="tozero")

    # texto acima dos pontos — formatado com ponto como separador de milhar
    textos_vendas_acum = [
        f"{v:,.1f}".replace(",", ".")
        for v in daily["vendas_acumuladas_mil"]
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
# GRÁFICO DE PIZZA – DISTRIBUIÇÃO DOS SLOTS POR PRODUTO
# ─────────────────────────────────────────────────────────
st.markdown("### 🍕 Distribuição dos Slots Vendidos por Produto")

def classificar_produto(desc: str) -> str:
    if pd.isna(desc):
        return "Indefinido"

    up = str(desc).upper()

    if "SEMESTRAL" in up:
        return "Plano Semestral (24 slots)"
    if "TRIMESTRAL" in up:
        return "Plano Trimestral (12 slots)"
    if "MENSAL" in up:
        return "Plano Mensal (4 slots)"
    if "AVULSA" in up:
        return "Avulsa (1 slot)"

    m = re.search(r"\((\d+)\s*sess", str(desc), flags=re.IGNORECASE)
    if m:
        n = int(m.group(1))
        return f"Pacote {n} sessões ({n} slots)"

    return "Outros"

# IMPORTANTÍSSIMO: usar df_filtrado, não df_base
df_filtrado["Produto"] = df_filtrado["Descrição"].apply(classificar_produto)

pizza = (
    df_filtrado
    .groupby("Produto", as_index=False)
    .agg(slots_totais=("slots_total", "sum"))
    .sort_values("slots_totais", ascending=False)
)

if pizza["slots_totais"].sum() == 0:
    st.info("Nenhum slot vendido no período/seleção atual para montar o gráfico de produtos.")
else:
    fig_pizza = px.pie(
        pizza,
        names="Produto",
        values="slots_totais",
        title="Participação dos Produtos nos Slots Vendidos (%)",
        hole=0.4,
    )
    fig_pizza.update_traces(textinfo="percent+label")

    # 👇 AQUI ENTRA A CORREÇÃO: key única
    st.plotly_chart(fig_pizza, use_container_width=True, key="pizza_slots_produto")

# ─────────────────────────────────────────────────────────
# DISTRIBUIÇÃO DE SLOTS POR CLIENTE
# ─────────────────────────────────────────────────────────
st.markdown("### 👥 Distribuição de slots por cliente")

if "ClienteFull" not in df_filtrado.columns:
    st.info("Nenhuma coluna de cliente disponível para esta análise.")
else:
    # 🔁 GARANTIR que os slots estejam calculados com a mesma regra usada no resto do app
    if "slots_por_venda" not in df_filtrado.columns or "slots_total" not in df_filtrado.columns:
        df_filtrado["slots_por_venda"] = df_filtrado["Descrição"].apply(extract_slots)
        df_filtrado["slots_total"] = df_filtrado["slots_por_venda"] * df_filtrado["Quantidade"]

    # total de slots / vendas / receita por cliente
    df_por_cliente = (
        df_filtrado
        .groupby("ClienteFull", as_index=False)
        .agg(
            total_slots=("slots_total", "sum"),   # ← soma dos slots (avulsa=1, mensal=4, tri=12, sem=24, pacotes etc.)
            total_vendas=("Valor", "sum"),       # R$ por cliente
            num_vendas=("Valor", "size"),        # quantidade de linhas (vendas) no relatório
        )
    )


    # distribuição: quantos clientes compraram 1, 2, 3... slots
    # totais gerais para percentuais
    total_clientes = len(df_por_cliente)
    total_slots_all = df_por_cliente["total_slots"].sum()

    # distribuição: quantos clientes compraram 1, 2, 3... slots
    # + quanto de slots e vendas cada grupo representa
    dist_slots = (
        df_por_cliente
        .groupby("total_slots", as_index=False)
        .agg(
            qtd_clientes=("ClienteFull", "count"),
            slots_grupo=("total_slots", "sum"),
            vendas_grupo=("total_vendas", "sum"),
        )
        .sort_values("total_slots")
    )

    # % de clientes de cada grupo em relação ao total de pagantes
    dist_slots["pct_clientes"] = dist_slots["qtd_clientes"] / total_clientes * 100

    # % de slots de cada grupo em relação ao total de slots vendidos
    dist_slots["pct_slots"] = dist_slots["slots_grupo"] / total_slots_all * 100


    if dist_slots.empty:
        st.info("Não há slots vendidos no período/seleção atual para montar a distribuição por cliente.")
    else:
        fig_dist = px.bar(
            dist_slots,
            x="total_slots",
            y="qtd_clientes",
            labels={
                "total_slots": "Slots comprados no período",
                "qtd_clientes": "Número de clientes",
            },
            text="qtd_clientes",
            title="Distribuição de clientes por quantidade de slots comprados",
        )
        fig_dist.update_traces(textposition="outside")
        fig_dist.update_layout(xaxis=dict(dtick=1))

        st.plotly_chart(fig_dist, use_container_width=True)

        st.markdown("#### 📋 Tabela de distribuição")
        st.dataframe(dist_slots, use_container_width=True)

        # ─────────────────────────────────────────────
        # MÉDIAS DE COMPRAS POR CLIENTE
        # ─────────────────────────────────────────────
        st.markdown("#### 📌 Resumo de comportamento de slots")
        
        # slots médios por cliente (todos)
        media_slots_todos = df_por_cliente["total_slots"].mean()
        
        # considera apenas clientes que têm mais de 1 slot no período
        df_multi_slots = df_por_cliente[df_por_cliente["total_slots"] > 1]
        media_slots_multi = (
            df_multi_slots["total_slots"].mean() if not df_multi_slots.empty else 0
        )
        
        col_a, col_b, col_c = st.columns(3)
        with col_a:
            st.metric("Clientes pagantes no período", total_clientes)
        with col_b:
            st.metric("Slots médios por cliente (todos)", f"{media_slots_todos:.2f}")
        with col_c:
            st.metric(
                "Slots médios por cliente (quem tem 2+ slots)",
                f"{media_slots_multi:.2f}",
            )

        import io
        
        st.markdown("#### ⬇️ Baixar resumo por cliente (Excel)")
        
        df_por_cliente_export = df_por_cliente.sort_values(
            "total_slots", ascending=False
        ).reset_index(drop=True)
        
        buffer_cli = io.BytesIO()
        with pd.ExcelWriter(buffer_cli, engine="xlsxwriter") as writer:
            df_por_cliente_export.to_excel(writer, index=False, sheet_name="Clientes")
            ws = writer.sheets["Clientes"]
            for i, col in enumerate(df_por_cliente_export.columns):
                max_len = max(df_por_cliente_export[col].astype(str).map(len).max(), len(col)) + 2
                ws.set_column(i, i, min(max_len, 40))
        
        buffer_cli.seek(0)
        
        st.download_button(
            label="📥 Baixar resumo por cliente (XLSX)",
            data=buffer_cli.getvalue(),
            file_name="clientes_slots_resumo.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="download_clientes_resumo",   # 👈 chave única
        )


# ─────────────────────────────────────────────────────────
# TABELA DIÁRIA CONSOLIDADA
# ─────────────────────────────────────────────────────────
st.markdown("### 📋 Tabela diária consolidada (após filtros)")
st.dataframe(daily, use_container_width=True)


