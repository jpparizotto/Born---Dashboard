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
    layout="wide",
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
            df["hora"] = pd.to_datetime(
                df["hora"], format="%H:%M:%S", errors="coerce"
            ).dt.time

        # numéricos
        for col in ["numero_aula_dia", "inclinacao_pct", "velocidade_pct"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df
    else:
        return pd.DataFrame(columns=COLUMNS)


def save_acidentes_df(df: pd.DataFrame) -> None:
    df_to_save = df.copy()
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
        - Há backup automático no GitHub a cada novo registro.
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
        inclinacao_pct = st.number_input(
            "Inclinação (%)",
            min_value=0.0,
            max_value=100.0,
            step=1.0,
            value=14.0,
        )
    with cols3[1]:
        velocidade_pct = st.number_input(
            "Velocidade (%)",
            min_value=0.0,
            max_value=100.0,
            step=1.0,
            value=40.0,
        )
    with cols3[2]:
        momento_aula = st.selectbox(
            "Momento da aula",
            ["Início", "Meio", "Final"],
        )

    cols4 = st.columns(3)
    with cols4[0]:
        gravidade = st.selectbox("Gravidade", ["Leve", "Moderada", "Grave"])
    with cols4[1]:
        parte_corpo = st.text_input(
            "Parte do corpo afetada",
            placeholder="ex.: joelho esquerdo, mão direita",
        )
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

            try:
                backup_acidentes_to_github()
            except Exception:
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
    col_f1, col_f2, col_f3, col_f4 = st.columns(4)

    with col_f1:
        data_min = df["data"].min()
        data_max = df["data"].max()
        intervalo_datas = st.date_input(
            "Período",
            value=(data_min, data_max),
            min_value=data_min,
            max_value=data_max,
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

    if isinstance(intervalo_datas, (list, tuple)) and len(intervalo_datas) == 2:
        ini, fim = intervalo_datas
        df_filtrado = df_filtrado[
            (df_filtrado["data"] >= ini) & (df_filtrado["data"] <= fim)
        ]

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

    st.write("DEBUG: bloco depois do download_button está rodando ✅")

    # ─────────────────────────────────────────────────────
    # EDITAR / DELETAR ACIDENTES
    # ─────────────────────────────────────────────────────
    with st.expander("✏️ Editar ou deletar acidentes", expanded=False):
        if df_filtrado.empty:
            st.info(
                "Nenhum acidente encontrado com os filtros atuais para editar ou deletar."
            )
        else:
            df_sorted = df_filtrado.sort_values(
                ["data", "hora"], ascending=[False, False]
            )
            row_ids = df_sorted.index.tolist()

            def format_option(row_id: int) -> str:
                row = df.loc[row_id]
                data_str = (
                    row["data"].strftime("%d/%m/%Y")
                    if isinstance(row["data"], date)
                    else str(row["data"])
                )
                hora_str = (
                    row["hora"].strftime("%H:%M")
                    if isinstance(row["hora"], time)
                    else str(row["hora"])
                )
                aluno_str = str(row.get("aluno", "") or "")
                prof_str = str(row.get("professor", "") or "")
                pista_str = str(row.get("pista", "") or "")
                return f"{data_str} {hora_str} — {aluno_str} (Prof: {prof_str}, Pista: {pista_str})"

            selected_row_id = st.selectbox(
                "Selecione um acidente para editar ou deletar",
                row_ids,
                format_func=format_option,
            )

            row_sel = df.loc[selected_row_id]

            st.markdown("#### ✏️ Editar acidente selecionado")

            with st.form("form_editar_acidente"):
                col_ed1, col_ed2 = st.columns(2)
                with col_ed1:
                    data_edit = st.date_input(
                        "Data",
                        value=row_sel["data"]
                        if isinstance(row_sel["data"], date)
                        else date.today(),
                        key="edit_data",
                    )
                with col_ed2:
                    hora_default = (
                        row_sel["hora"]
                        if isinstance(row_sel["hora"], time)
                        else time(0, 0)
                    )
                    hora_edit = st.time_input(
                        "Hora",
                        value=hora_default,
                        step=60,
                        key="edit_hora",
                    )

                col_ed3, col_ed4, col_ed5 = st.columns(3)
                with col_ed3:
                    professor_edit = st.text_input(
                        "Professor",
                        value=str(row_sel.get("professor", "") or ""),
                        key="edit_professor",
                    )
                with col_ed4:
                    numero_aula_edit = st.number_input(
                        "Nº da aula no dia",
                        min_value=1,
                        max_value=20,
                        value=int(row_sel.get("numero_aula_dia", 1) or 1),
                        key="edit_num_aula",
                    )
                with col_ed5:
                    aluno_edit = st.text_input(
                        "Aluno",
                        value=str(row_sel.get("aluno", "") or ""),
                        key="edit_aluno",
                    )

                col_ed6, col_ed7, col_ed8 = st.columns(3)
                with col_ed6:
                    pista_edit = st.selectbox(
                        "Pista",
                        ["A", "B"],
                        index=["A", "B"].index(
                            row_sel.get("pista")
                            if row_sel.get("pista") in ["A", "B"]
                            else "A"
                        ),
                        key="edit_pista",
                    )
                with col_ed7:
                    inclinacao_edit = st.number_input(
                        "Inclinação (%)",
                        min_value=0.0,
                        max_value=100.0,
                        step=1.0,
                        value=float(row_sel.get("inclinacao_pct", 0) or 0.0),
                        key="edit_inclinacao",
                    )
                with col_ed8:
                    velocidade_edit = st.number_input(
                        "Velocidade (%)",
                        min_value=0.0,
                        max_value=100.0,
                        step=1.0,
                        value=float(row_sel.get("velocidade_pct", 0) or 0.0),
                        key="edit_velocidade",
                    )

                col_ed9, col_ed10, col_ed11 = st.columns(3)
                with col_ed9:
                    momento_edit = st.selectbox(
                        "Momento da aula",
                        ["Início", "Meio", "Final"],
                        index=["Início", "Meio", "Final"].index(
                            row_sel.get("momento_aula")
                            if row_sel.get("momento_aula")
                            in ["Início", "Meio", "Final"]
                            else "Meio"
                        ),
                        key="edit_momento",
                    )
                with col_ed10:
                    gravidade_edit = st.selectbox(
                        "Gravidade",
                        ["Leve", "Moderada", "Grave"],
                        index=["Leve", "Moderada", "Grave"].index(
                            row_sel.get("gravidade")
                            if row_sel.get("gravidade")
                            in ["Leve", "Moderada", "Grave"]
                            else "Leve"
                        ),
                        key="edit_gravidade",
                    )
                with col_ed11:
                    parte_corpo_edit = st.text_input(
                        "Parte do corpo afetada",
                        value=str(row_sel.get("parte_corpo", "") or ""),
                        key="edit_parte_corpo",
                    )

                encaminhamento_edit = st.selectbox(
                    "Encaminhamento",
                    [
                        "Sem necessidade de atendimento",
                        "Primeiros socorros na Born to Ski",
                        "Encaminhado ao hospital",
                    ],
                    index=[
                        "Sem necessidade de atendimento",
                        "Primeiros socorros na Born to Ski",
                        "Encaminhado ao hospital",
                    ].index(
                        row_sel.get("encaminhamento")
                        if row_sel.get("encaminhamento")
                        in [
                            "Sem necessidade de atendimento",
                            "Primeiros socorros na Born to Ski",
                            "Encaminhado ao hospital",
                        ]
                        else "Sem necessidade de atendimento"
                    ),
                    key="edit_encaminhamento",
                )

                descricao_edit = st.text_area(
                    "Descrição do professor",
                    value=str(row_sel.get("descricao", "") or ""),
                    height=150,
                    key="edit_descricao",
                )

                salvar_edicao = st.form_submit_button("💾 Salvar alterações")

            if salvar_edicao:
                df.loc[selected_row_id, "data"] = data_edit
                df.loc[selected_row_id, "hora"] = hora_edit
                df.loc[selected_row_id, "professor"] = professor_edit.strip()
                df.loc[selected_row_id, "numero_aula_dia"] = int(numero_aula_edit)
                df.loc[selected_row_id, "aluno"] = aluno_edit.strip()
                df.loc[selected_row_id, "pista"] = pista_edit
                df.loc[selected_row_id, "inclinacao_pct"] = float(inclinacao_edit)
                df.loc[selected_row_id, "velocidade_pct"] = float(velocidade_edit)
                df.loc[selected_row_id, "momento_aula"] = momento_edit
                df.loc[selected_row_id, "gravidade"] = gravidade_edit
                df.loc[selected_row_id, "parte_corpo"] = parte_corpo_edit.strip()
                df.loc[selected_row_id, "encaminhamento"] = encaminhamento_edit
                df.loc[selected_row_id, "descricao"] = descricao_edit.strip()

                save_acidentes_df(df)
                try:
                    backup_acidentes_to_github()
                except Exception:
                    st.warning(
                        "Alterações salvas, mas houve erro ao enviar backup para o GitHub."
                    )
                else:
                    st.success(
                        "Acidente atualizado com sucesso e backup enviado para o GitHub!"
                    )
                st.experimental_rerun()

            st.markdown("#### 🗑️ Deletar acidente selecionado")

            col_del1, col_del2 = st.columns([1, 2])
            with col_del1:
                confirmar_delete = st.checkbox("Confirmar exclusão")

            with col_del2:
                if st.button("🗑️ Deletar acidente", disabled=not confirmar_delete):
                    df = df.drop(index=selected_row_id)
                    save_acidentes_df(df)
                    try:
                        backup_acidentes_to_github()
                    except Exception:
                        st.warning(
                            "Acidente deletado, mas houve erro ao enviar backup para o GitHub."
                        )
                    else:
                        st.success(
                            "Acidente deletado com sucesso e backup enviado para o GitHub!"
                        )
                    st.experimental_rerun()

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

    if not df_filtrado.empty:
        data_min = df_filtrado["data"].min()
        data_max = df_filtrado["data"].max()
        idx = pd.date_range(start=data_min, end=data_max, freq="D")

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

    with cols_grafs[0]:
        if df_filtrado["momento_aula"].notna().any():
            g_momento = (
                df_filtrado.groupby("momento_aula").size().reset_index(name="qtd")
            )
            fig_momento = px.bar(
                g_momento,
                x="momento_aula",
                y="qtd",
                title="Acidentes por momento da aula",
            )
            st.plotly_chart(fig_momento, use_container_width=True)

    with cols_grafs[1]:
        if df_filtrado["pista"].notna().any():
            g_pista = df_filtrado.groupby("pista").size().reset_index(name="qtd")
            fig_pista = px.bar(
                g_pista, x="pista", y="qtd", title="Acidentes por pista"
            )
            st.plotly_chart(fig_pista, use_container_width=True)

    with cols_grafs[2]:
        if df_filtrado["professor"].notna().any():
            g_prof = (
                df_filtrado.groupby("professor").size().reset_index(name="qtd")
            )
            fig_prof = px.bar(
                g_prof, x="professor", y="qtd", title="Acidentes por professor"
            )
            st.plotly_chart(fig_prof, use_container_width=True)
