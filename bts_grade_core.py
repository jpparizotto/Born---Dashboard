# bts_grade_core.py
# Funções reutilizáveis para gerar CSV de slots e grade XLSX

import os
import io
from datetime import date, timedelta

import pandas as pd

from dashboard_page import gerar_csv, _ensure_base_columns  # você já tem essas funções lá


DATA_DIR = "evo_ocupacao"


def get_slots_df_for_period(d0: date, d1: date) -> pd.DataFrame:
    """
    Usa a mesma função gerar_csv do dashboard para coletar slots do período [d0, d1]
    e devolve um DataFrame já normalizado.
    """
    path = gerar_csv(d0, d1)
    df = pd.read_csv(path, encoding="utf-8-sig")
    df = _ensure_base_columns(df)
    return df


def get_slots_df_for_day(target_day: date) -> pd.DataFrame:
    """
    Retorna apenas os slots do dia alvo.
    """
    df = get_slots_df_for_period(target_day, target_day)
    df_day = df[df["Data"] == target_day].copy()
    return df_day


def build_grade_xlsx_bytes(df: pd.DataFrame) -> bytes:
    """
    Gera o mesmo XLSX de grade que o dashboard gera, mas em memória.
    Retorna bytes do arquivo pronto pra anexar em e-mail.
    """
    # Mesma lógica do dashboard_page.py
    selected_cols = [
        "Pista", "Data", "Início", "Fim", "Atividade",
        "Capacidade", "Bookados", "Disponíveis",
        "Professor", "Aluno 1", "Aluno 2", "Aluno 3",
    ]

    sort_keys = [c for c in ["Data", "Horario", "Atividade"] if c in df.columns]
    df_sorted = df.sort_values(sort_keys) if sort_keys else df.copy()

    cols_existentes = [c for c in selected_cols if c in df_sorted.columns]
    df_excel = df_sorted[cols_existentes]

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        df_excel.to_excel(writer, index=False, sheet_name="Aulas")
        ws = writer.sheets["Aulas"]

        for i, col in enumerate(df_excel.columns):
            max_len = max(df_excel[col].astype(str).map(len).max(), len(col)) + 2
            ws.set_column(i, i, min(max_len, 40))

    buffer.seek(0)
    return buffer.getvalue()
