# -*- coding: utf-8 -*-
# pages/5_Reporte_de_Acidentes.py

import os
from datetime import date, datetime, time

import pandas as pd
import streamlit as st
import plotly.express as px

from db import backup_acidentes_to_github

# ─────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Reporte de Acidentes — Born to Ski",
    page_icon="⚠️",
    layout="wide"
)

st.title("⚠️ Reporte de Acidentes — Born to Ski")
st.caption("Módulo para registro e acompanhamento diário de incidentes em aula.")

DATA_PATH = "data"
CSV_PATH = os.path.join(DATA_PATH, "acidentes.csv")

# ─────────────────────────────────────────────────────────
# FUNÇÕES AUXILIARES
# ─────────────────────────────────────────────────────────
COLUMNS = [
    "data",
    "hora",
    "professor",
    "numero_aula_dia",
    "aluno",
    "pista",
    "inclinacao_pct",
    "velocidade_pct",
    "momento_aula",
    "gravidade",
    "parte_corpo",
    "encaminhamento",
    "descricao",
]

def load_acidentes_df() -> pd.DataFrame:
    os.makedirs(DATA_PATH, exist_ok=True)
    if os.path.exists(CSV_PATH):
        df = pd.read_csv(CSV_PATH, sep=";", dtype=str)
        # ajustes de tipos
        if "data" in df.columns:
            df["data"] = pd.to_datetime(df["data"]).dt.date
        if "hora" in df.columns:
            df["hora"] = pd.to_datetime(df["hora"], format="%H:%M:%S", errors="coerce").dt.time
        # numéricos
        for col in ["numero_aula_dia", "inclinacao_pct", "velocidade_pct"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df
    else:
        return pd.DataFrame(columns=COLUMNS)

def save_acidentes_df(df: pd.DataFrame) -> None:
    df_to_save = df.copy()
    # garantir tipos serializáveis
    df_to_save["data"] = df_to_save["data"].astype(str)
    df_to_save["hora"] = df_to_save["hora"].astype(str)
    df_to_save.to_csv(CSV_PATH, sep=";", index=False)

# ─────────────────────────────────────────────────────────
# CARREGAR BASE
# ─────────────────────────────────────────────────────────
df = load_acidentes_df()

with st.expander("ℹ️ Como usar este módulo", expanded=False):
    st.markdown(
        """
        **Registro de novos acidentes**
        - Preencha o formulário abaixo sempre que houver um incidente em aula.

        **Exportação / backup**
        - Todos os registros ficam salvos no arquivo `data/acidentes.csv` (separador `;`).
        - Você pode baixar a base completa mais abaixo na página.
        """
    )

# ─────────────────────────────────────────────────────────
# FORMULÁRIO PARA NOVO ACIDENTE
# ─────────────────────────────────────────────────────────
st.subheader("Novo reporte de acidente")

with st.form("form_novo_acidente", clear_on_submit=True):
    cols1 = st.columns(3)
    with cols1[0]:
        professor = st.text_input("Nome do professor*", "")
    with cols1[1]:
        numero_aula_dia = st.number_input("Nº da aula do dia*", min_value=1, step=1)
    with cols1[2]:
        aluno = st.text_input("Nome do aluno*", "")

    cols2 = st.columns(3)
    with cols2[0]:
        data_acc = st.date_input("Data*", value=date.today())
    with cols2[1]:
        hora_acc = st.time_input("Horário*", value=datetime.now().time())
    with cols2[2]:
        pista = st.selectbox("Pista*", ["A", "B"])

    cols3 = st.columns(3)
    with cols3[0]:
        inclinacao_pct = st.number_input("Inclinação (%)", min_value=0.0, max_value=100.0, step=1.0, value=14.0)
    with cols3[1]:
        velocidade_pct = st.number_input("Velocidade (%)", min_value=0.0, max_value=100.0, step=1.0, value=40.0)
    with cols3[2]:
        momento_aula = st.selectbox(
            "Momento da aula",
            ["Início", "Meio", "Final"]
        )

    cols4 = st.columns(3)
    with cols4[0]:
        gravidade = st.selectbox("Gravidade", ["Leve", "Moderada", "Grave"])
    with cols4[1]:
        parte_corpo = st.text_input("Parte do corpo afetada", placeholder="ex.: joelho esquerdo, mão direita")
    with cols4[2]:
        encaminhamento = st.selectbox(
            "Encaminhamento",
            [
                "Sem necessidade de atendimento",
                "Primeiros socorros na Born to Ski",
                "Encaminhado ao hospital",
            ],
        )

    descricao = st.text_area("Descrição do professor*", height=150)

    submitted = st.form_submit_button("Salvar reporte")

    if submitted:
        if not professor or not aluno or not descricao:
            st.error("Preencha pelo menos professor, aluno e descrição.")
        else:
            novo_registro = {
                "data": data_acc,
                "hora": hora_acc,
                "professor": professor.strip(),
                "numero_aula_dia": int(numero_aula_dia),
                "aluno": aluno.strip(),
                "pista": pista,
                "inclinacao_pct": float(inclinacao_pct),
                "velocidade_pct": float(velocidade_pct),
                "momento_aula": momento_aula,
                "gravidade": gravidade,
                "parte_corpo": parte_corpo.strip(),
                "encaminhamento": encaminhamento,
                "descricao": descricao.strip(),
            }
            df = pd.concat([df, pd.DataFrame([novo_registro])], ignore_index=True)
            save_acidentes_df(df)

            # Tenta enviar backup para o GitHub também
            try:
                backup_acidentes_to_github()
            except Exception as e:
                # Não quebra o fluxo se o backup falhar – só avisa
                st.warning(
                    "Reporte salvo localmente, mas houve erro ao enviar o backup para o GitHub."
                )
            else:
                st.success("Reporte salvo com sucesso e backup enviado para o GitHub!")

# ─────────────────────────────────────────────────────────
# FILTROS E VISUALIZAÇÃO DA BASE
# ─────────────────────────────────────────────────────────
st.subheader("Base de acidentes")

if df.empty:
    st.info("Nenhum acidente cadastrado ainda.")
else:
    # Filtros básicos
    col_f1, col_f2, col_f3, col_f4 = st.columns(4)

    with col_f1:
        data_min = df["data"].min()
        data_max = df["data"].max()
        intervalo_datas = st.date_input(
            "Período",
            value=(data_min, data_max),
            min_value=data_min,
            max_value=data_max
        )

    with col_f2:
        profs = ["Todos"] + sorted(df["professor"].dropna().unique().tolist())
        prof_sel = st.selectbox("Professor", profs)

    with col_f3:
        pistas = ["Todas"] + sorted(df["pista"].dropna().unique().tolist())
        pista_sel = st.selectbox("Pista", pistas)

    with col_f4:
        momentos = ["Todos"] + sorted(df["momento_aula"].dropna().unique().tolist())
        momento_sel = st.selectbox("Momento da aula", momentos)

    df_filtrado = df.copy()

    # filtro período
    if isinstance(intervalo_datas, (list, tuple)) and len(intervalo_datas) == 2:
        ini, fim = intervalo_datas
        df_filtrado = df_filtrado[(df_filtrado["data"] >= ini) & (df_filtrado["data"] <= fim)]

    if prof_sel != "Todos":
        df_filtrado = df_filtrado[df_filtrado["professor"] == prof_sel]

    if pista_sel != "Todas":
        df_filtrado = df_filtrado[df_filtrado["pista"] == pista_sel]

    if momento_sel != "Todos":
        df_filtrado = df_filtrado[df_filtrado["momento_aula"] == momento_sel]

    st.dataframe(
        df_filtrado.sort_values(["data", "hora"], ascending=[False, False]),
        use_container_width=True,
        hide_index=True,
    )

    st.download_button(
        "📥 Baixar base filtrada (CSV)",
        df_filtrado.to_csv(sep=";", index=False).encode("utf-8"),
        file_name="acidentes_filtrados.csv",
        mime="text/csv",
    )

    # ─────────────────────────────────────────────────────
    # MÉTRICAS E ESTATÍSTICAS
    # ─────────────────────────────────────────────────────
    st.subheader("Estatísticas")

    col_m1, col_m2, col_m3 = st.columns(3)
    with col_m1:
        st.metric("Total de acidentes (período)", len(df_filtrado))
    with col_m2:
        st.metric("Clientes afetados (únicos)", df_filtrado["aluno"].nunique())
    with col_m3:
        st.metric("Professores envolvidos", df_filtrado["professor"].nunique())

    # Acidentes por dia
    # Acidentes por dia (incluindo dias com 0 acidentes)
    if not df_filtrado.empty:
        # intervalo completo entre a menor e a maior data filtradas
        data_min = df_filtrado["data"].min()
        data_max = df_filtrado["data"].max()
        idx = pd.date_range(start=data_min, end=data_max, freq="D")
    
        # conta acidentes por dia e reindexa para todas as datas
        acidentes_por_dia = (
            df_filtrado.groupby("data")
            .size()
            .rename("qtd")
            .reindex(idx, fill_value=0)
            .reset_index()
            .rename(columns={"index": "data"})
        )
    
        fig_dia = px.bar(
            acidentes_por_dia,
            x="data",
            y="qtd",
            title="Acidentes por dia",
        )
        st.plotly_chart(fig_dia, use_container_width=True)


    cols_grafs = st.columns(3)

    # Acidentes por momento da aula
    with cols_grafs[0]:
        if df_filtrado["momento_aula"].notna().any():
            g_momento = (
                df_filtrado.groupby("momento_aula").size().reset_index(name="qtd")
            )
            fig_momento = px.bar(
                g_momento,
                x="momento_aula",
                y="qtd",
                title="Acidentes por momento da aula"
            )
            st.plotly_chart(fig_momento, use_container_width=True)

    # Acidentes por pista
    with cols_grafs[1]:
        if df_filtrado["pista"].notna().any():
            g_pista = df_filtrado.groupby("pista").size().reset_index(name="qtd")
            fig_pista = px.bar(g_pista, x="pista", y="qtd", title="Acidentes por pista")
            st.plotly_chart(fig_pista, use_container_width=True)

    # Acidentes por professor
    with cols_grafs[2]:
        if df_filtrado["professor"].notna().any():
            g_prof = (
                df_filtrado.groupby("professor").size().reset_index(name="qtd")
            )
            fig_prof = px.bar(
                g_prof,
                x="professor",
                y="qtd",
                title="Acidentes por professor"
            )
            st.plotly_chart(fig_prof, use_container_width=True)
