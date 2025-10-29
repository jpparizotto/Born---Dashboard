# -*- coding: utf-8 -*-
"""
Dashboard de Ocupação — Born to Ski (Streamlit)
- Lê os CSVs gerados pelo script (slots_*.csv) em ./evo_ocupacao
- Filtros: Data, Modalidade (Atividade), Período (Manhã/Tarde/Noite), Horário
- KPIs e gráficos (Plotly)
- Visualização extra: Calendário por mês (hover mostra Slots, Vagas, Ocupação %, Vagas sobrando)

Rode via:
  streamlit run dashboard.py
"""

import os
import io
import glob
import calendar as pycal
from datetime import datetime, date, timedelta
import subprocess, sys, shutil, tempfile, time, os

import numpy as np
import pandas as pd
from dateutil.parser import parse as parse_date
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

APP_TITLE = "Dashboard de Ocupação — Born to Ski"
DATA_DIR = "evo_ocupacao"  # pasta onde o script salva os CSVs de slots

# ──────────────────────────────────────────────────────────────────────────────
# Guard opcional: avisa caso rode como "python dashboard.py"
# (não impede execução; apenas orienta)
try:
    from streamlit.runtime.scriptrunner import get_script_run_ctx
    if get_script_run_ctx() is None:
        print("\n[Streamlit] Para abrir no navegador, use:\n\n  streamlit run dashboard.py\n")
except Exception:
    pass
# ──────────────────────────────────────────────────────────────────────────────

# ──────────────────────────────────────────────────────────────────────────────
# Utils de leitura e normalização
# ──────────────────────────────────────────────────────────────────────────────
def _read_csv_safely(path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(path, encoding="utf-8")
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="latin1")

def _find_latest_slots_csv():
    import glob
    pattern = os.path.join(DATA_DIR, "slots_*.csv")
    files = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)
    return files[0] if files else None

def _run_coletor(date_from: str|None, date_to: str|None) -> tuple[bool, str]:
    """
    Executa o script coletor que gera os CSVs.
    - Tenta passar as datas via argumentos --from/--to, se você aplicar o patch de argparse no coletor.
    - Retorna (ok, msg).
    """
    # caminho do script coletor (ajuste se o nome/caminho for outro):
    coletor = os.path.join(os.path.dirname(__file__), "Ocupação_próximos_dias.py")
    if not os.path.isfile(coletor):
        return False, f"Script coletor não encontrado: {coletor}"

    cmd = [sys.executable, coletor]
    if date_from and date_to:
        cmd += ["--from", date_from, "--to", date_to]

    # Passar credenciais como env (pegas de st.secrets se existir)
    env = os.environ.copy()
    try:
        import streamlit as st
        if "EVO_USER" in st.secrets:  env["EVO_USER"]  = str(st.secrets["EVO_USER"])
        if "EVO_TOKEN" in st.secrets: env["EVO_TOKEN"] = str(st.secrets["EVO_TOKEN"])
    except Exception:
        pass
    env["PYTHONIOENCODING"] = "utf-8"  # << força stdout/stderr em UTF-8

    try:
        proc = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=600)
        if proc.returncode != 0:
            return False, f"Coletor retornou {proc.returncode}.\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
        return True, proc.stdout[-2000:]  # um tail do log
    except subprocess.TimeoutExpired:
        return False, "Tempo esgotado executando o coletor (timeout)."
    except Exception as e:
        return False, f"Falha executando coletor: {e}"

def _ensure_base_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza nomes e tipos esperados vindos do seu script:
      Data, Horario, Periodo, Atividade, Início, Fim, Capacidade, Disponíveis, Bookados, ActivityId
    Também calcula Ocupacao% se não existir.
    """
    cols = {c: c.strip() for c in df.columns}
    df = df.rename(columns=cols)

    # mapear possíveis variações de nomes
    rename_map = {}
    for cand, target in [
        (["Data", "date", "Dia"], "Data"),
        (["Horario", "Hora", "hour", "time"], "Horario"),
        (["Periodo", "Período", "period"], "Periodo"),
        (["Atividade", "Modalidade", "Activity", "activity", "name", "description"], "Atividade"),
        (["Início", "Inicio", "start", "startTime"], "Início"),
        (["Fim", "End", "endTime"], "Fim"),
        (["Capacidade", "capacity", "VacanciesTotal", "maxCapacity", "Vagas"], "Capacidade"),
        (["Disponíveis", "available", "vacancies"], "Disponíveis"),
        (["Bookados", "booked", "occupied", "enrolled"], "Bookados"),
        (["ActivityId", "idActivity", "activityId", "ID", "Id"], "ActivityId"),
        (["Ocupacao%", "Ocupação%", "Occ%", "occ_pct"], "Ocupacao%"),
    ]:
        for c in cand:
            if c in df.columns:
                rename_map[c] = target
                break
    if rename_map:
        df = df.rename(columns=rename_map)

    # cria colunas ausentes
    required = ["Data", "Horario", "Periodo", "Atividade", "Início", "Fim", "Capacidade", "Disponíveis", "Bookados", "ActivityId"]
    for r in required:
        if r not in df.columns:
            df[r] = None

    # tipagem — Data
    def _to_date(x):
        if pd.isna(x):
            return None
        if isinstance(x, date):
            return x
        s = str(x)
        try:
            return parse_date(s).date()
        except Exception:
            try:
                return datetime.fromisoformat(s[:10]).date()
            except Exception:
                return None
    df["Data"] = df["Data"].apply(_to_date)

    # tipagem — Horario "HH:MM"
    def _to_hhmm(x):
        if pd.isna(x):
            return None
        s = str(x)
        if len(s) >= 5 and ":" in s[:5]:
            return s[:5]
        try:
            dtp = parse_date(s)
            return dtp.strftime("%H:%M")
        except Exception:
            return s
    df["Horario"] = df["Horario"].apply(_to_hhmm)

    # numéricos
    for col in ["Capacidade", "Disponíveis", "Bookados"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # Periodo (infere se vier vazio)
    def _infer_period(hhmm):
        if not hhmm or not isinstance(hhmm, str) or ":" not in hhmm:
            return "Indefinido"
        try:
            h, m = hhmm.split(":")
            total = int(h) * 60 + int(m)
        except Exception:
            return "Indefinido"
        noon = 12 * 60
        five_thirty = 17 * 60 + 30
        if total < noon:
            return "Manhã"
        elif noon <= total <= five_thirty:
            return "Tarde"
        else:
            return "Noite"

    if "Periodo" not in df.columns or df["Periodo"].isna().all():
        df["Periodo"] = df["Horario"].apply(_infer_period)
    else:
        df["Periodo"] = df["Periodo"].fillna("").replace({"Manha": "Manhã", "Tarde": "Tarde", "Noite": "Noite"})
        df.loc[df["Periodo"] == "", "Periodo"] = df.loc[df["Periodo"] == "", "Horario"].apply(_infer_period)

    # Ocupacao% (sem usar option_context deprecado)
    if "Ocupacao%" not in df.columns:
        df["Ocupacao%"] = (df["Bookados"] / df["Capacidade"] * 100)
        df["Ocupacao%"] = df["Ocupacao%"].replace([np.inf, -np.inf], np.nan).fillna(0).round(1)

    return df

def _load_data() -> pd.DataFrame:
    """Carrega o slots_*.csv mais recente de DATA_DIR. Se não existir, retorna df vazio."""
    pattern = os.path.join(DATA_DIR, "slots_*.csv")
    files = sorted(glob.glob(pattern))
    if not files:
        return pd.DataFrame()
    files = sorted(files, key=os.path.getmtime, reverse=True)
    df = _read_csv_safely(files[0])
    return _ensure_base_columns(df)

def _download_button_csv(df: pd.DataFrame, label: str, filename: str):
    csv_bytes = df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
    st.download_button(label, data=csv_bytes, file_name=filename, mime="text/csv")

def _kpi_block(label, value, help_text=None):
    st.metric(label=label, value=value, help=help_text)

# ──────────────────────────────────────────────────────────────────────────────
# Funções para o Calendário
# ──────────────────────────────────────────────────────────────────────────────
def _daily_agg(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega por dia: Slots, Vagas, Bookados, Ocupacao%, VagasSobrando."""
    g = df.groupby("Data", as_index=False).agg(
        Slots=("Horario", "count"),
        Vagas=("Capacidade", "sum"),
        Bookados=("Bookados", "sum"),
    )
    g["Ocupacao%"] = (g["Bookados"] / g["Vagas"] * 100).replace([np.inf, -np.inf], np.nan).fillna(0).round(1)
    g["VagasSobrando"] = (g["Vagas"] - g["Bookados"]).astype(int)
    return g

def _month_calendar_frame(daily: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    """
    Constrói um DF com posições de calendário (semana x dia da semana) para o mês.
    Columns: Data, Slots, Vagas, Bookados, Ocupacao%, VagasSobrando, weekday(0=Seg), week_index(0..5), day_num
    """
    # dias do mês
    n_days = pycal.monthrange(year, month)[1]
    first_wd = date(year, month, 1).weekday()  # Monday=0
    rows = []
    # mapa diário (para merge)
    daily_map = daily.set_index("Data").to_dict(orient="index")

    for d in range(1, n_days + 1):
        dt_i = date(year, month, d)
        offset = first_wd + (d - 1)
        week_idx = offset // 7
        wd = offset % 7  # 0..6
        rec = daily_map.get(dt_i, None)
        rows.append({
            "Data": dt_i,
            "day_num": d,
            "weekday": wd,
            "week_index": week_idx,
            "Slots": (rec or {}).get("Slots", 0),
            "Vagas": (rec or {}).get("Vagas", 0),
            "Bookados": (rec or {}).get("Bookados", 0),
            "Ocupacao%": (rec or {}).get("Ocupacao%", 0.0),
            "VagasSobrando": (rec or {}).get("VagasSobrando", 0),
        })
    return pd.DataFrame(rows)

def make_calendar_figure(daily_df: pd.DataFrame, year: int, month: int, color_metric: str, show_values_in_cell: bool = True) -> go.Figure:
    """
    Gera um calendário (heatmap) para o mês/ano.
    - Cor baseada em color_metric (Ocupacao%, Slots, Vagas, VagasSobrando)
    - Hover mostra detalhes
    - (opcional) Mostra os valores DENTRO de cada quadrinho via 'text'
    """
    cal = _month_calendar_frame(daily_df, year, month)

    # Eixos para o heatmap
    x_labels = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]
    max_week = cal["week_index"].max() if not cal.empty else 5
    n_weeks = int(max_week) + 1

    # Matrizes
    z = [[None for _ in range(7)] for __ in range(n_weeks)]
    text = [["" for _ in range(7)] for __ in range(n_weeks)]
    custom = [[None for _ in range(7)] for __ in range(n_weeks)]

    for _, r in cal.iterrows():
        wi = int(r["week_index"])
        wd = int(r["weekday"])
        slots = int(r["Slots"])
        vagas = int(r["Vagas"])
        book  = int(r["Bookados"])
        occ   = float(r["Ocupacao%"])
        sobr  = int(r["VagasSobrando"])

        # valor de cor
        if color_metric == "Ocupacao%":
            z_val = occ
        elif color_metric == "VagasSobrando":
            z_val = sobr
        elif color_metric == "Vagas":
            z_val = vagas
        else:  # Slots
            z_val = slots
        z[wi][wd] = float(z_val)

        # texto dentro do quadrinho
        if show_values_in_cell:
            # conciso e com quebras de linha
            text[wi][wd] = f"{int(r['day_num'])}\nS:{slots} V:{vagas}\nOcc:{occ:.0f}% Sob:{sobr}"
        else:
            text[wi][wd] = str(int(r["day_num"]))

        custom[wi][wd] = {
            "data": r["Data"],
            "slots": slots,
            "vagas": vagas,
            "book": book,
            "occ": occ,
            "sobr": sobr,
        }

    # Paleta e range
    if color_metric == "Ocupacao%":
        colorscale = "RdYlGn"
        zmin, zmax = 0, 100
        colorbar_title = "Ocupação %"
    elif color_metric == "VagasSobrando":
        colorscale = "Blues"
        zmin, zmax = 0, max(1, cal["VagasSobrando"].max())
        colorbar_title = "Vagas sobrando"
    elif color_metric == "Vagas":
        colorscale = "Greens"
        zmin, zmax = 0, max(1, cal["Vagas"].max())
        colorbar_title = "Vagas"
    else:
        colorscale = "Oranges"
        zmin, zmax = 0, max(1, cal["Slots"].max())
        colorbar_title = "Slots"

    # tamanho da fonte adaptativo (mais semanas -> fonte menor)
    base_font = 12
    font_size = max(9, base_font - max(0, n_weeks - 5) * 2)

    fig = go.Figure(
        data=go.Heatmap(
            z=z,
            x=list(range(7)),
            y=list(range(n_weeks)),
            colorscale=colorscale,
            zmin=zmin,
            zmax=zmax,
            showscale=True,
            colorbar=dict(title=colorbar_title),

            text=text,
            texttemplate="%{text}",
            textfont={"size": font_size},

            # hover com detalhes
            customdata=custom,
            hovertemplate=(
                "<b>%{customdata.data|%Y-%m-%d}</b><br>"
                "Slots: %{customdata.slots}<br>"
                "Vagas: %{customdata.vagas}<br>"
                "Ocupação: %{customdata.occ:.1f}%<br>"
                "Vagas sobrando: %{customdata.sobr}<extra></extra>"
            ),
        )
    )

    fig.update_xaxes(
        tickmode="array",
        tickvals=list(range(7)),
        ticktext=x_labels,
        side="top",
        showgrid=False
    )
    fig.update_yaxes(
        tickmode="array",
        tickvals=list(range(n_weeks)),
        ticktext=[f"Semana {i+1}" for i in range(n_weeks)],
        autorange="reversed",
        showgrid=False
    )

    fig.update_layout(
        title=f"Calendário — {pycal.month_name[month]} {year}",
        xaxis_title="",
        yaxis_title="",
        margin=dict(l=10, r=10, t=50, b=10),
        height=320 + 40 * n_weeks,
    )
    return fig

# ──────────────────────────────────────────────────────────────────────────────
# App
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title=APP_TITLE, page_icon="🎿", layout="wide")
st.title(APP_TITLE)

with st.sidebar:
    st.header("Fonte de dados")
    st.write(f"Lendo CSVs de: `{DATA_DIR}/`")
    df_slots = _load_data()
    if df_slots.empty:
        st.warning("Nenhum arquivo `slots_*.csv` encontrado. Faça upload de um CSV de slots para testar.")
        uploaded = st.file_uploader("Envie um CSV (slots)", type=["csv"])
        if uploaded is not None:
            df_slots = pd.read_csv(uploaded)
            df_slots = _ensure_base_columns(df_slots)
    else:
        st.success("Dados carregados.")
    st.caption("Dica: o script Python salva um `slots_YYYY-MM-DD_a_YYYY-MM-DD.csv` nessa pasta.")

if df_slots.empty:
    st.info("Carregue ou gere um CSV de slots para visualizar o dashboard.")
    st.stop()

# Filtros
st.sidebar.header("Filtros")
min_date = df_slots["Data"].dropna().min()
max_date = df_slots["Data"].dropna().max()
default_from = min_date if isinstance(min_date, date) else date.today()
default_to = max_date if isinstance(max_date, date) else date.today()

date_range = st.sidebar.date_input(
    "Período",
    value=(default_from, default_to),
    min_value=min_date if isinstance(min_date, date) else None,
    max_value=max_date if isinstance(max_date, date) else None
)
if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
    f_date_from, f_date_to = date_range
else:
    f_date_from, f_date_to = default_from, default_to

modalidades = sorted([m for m in df_slots["Atividade"].dropna().unique()])
periodos = ["Manhã", "Tarde", "Noite"]
sel_modalidades = st.sidebar.multiselect("Modalidades (Atividade)", modalidades, default=modalidades)
sel_periodos = st.sidebar.multiselect("Períodos", periodos, default=periodos)
horas = sorted([h for h in df_slots["Horario"].dropna().unique()])
sel_horas = st.sidebar.multiselect("Horários (opcional)", horas)

# Aplicando filtros
df = df_slots.copy()
mask = (
    (df["Data"] >= pd.to_datetime(f_date_from).date()) &
    (df["Data"] <= pd.to_datetime(f_date_to).date()) &
    (df["Atividade"].isin(sel_modalidades)) &
    (df["Periodo"].isin(sel_periodos))
)
if sel_horas:
    mask = mask & (df["Horario"].isin(sel_horas))
df = df[mask].copy()

if df.empty:
    st.warning("Nenhum dado com os filtros atuais.")
    st.stop()

# KPIs
total_slots = int(len(df))
total_capacity = int(df["Capacidade"].sum())
total_booked = int(df["Bookados"].sum())
occ_overall = (total_booked / total_capacity * 100) if total_capacity else 0.0

kpi1, kpi2, kpi3, kpi4 = st.columns(4)
with kpi1: _kpi_block("Ocupação média", f"{occ_overall:.1f}%")
with kpi2: _kpi_block("Vagas (capacidade)", f"{total_capacity}")
with kpi3: _kpi_block("Bookados", f"{total_booked}")
with kpi4: _kpi_block("Slots", f"{total_slots}")

st.divider()

# Gráfico — Ocupação por dia
grp_day = df.groupby("Data", as_index=False).agg(
    Vagas=("Capacidade", "sum"),
    Bookados=("Bookados", "sum"),
    Slots=("Horario", "count")
)
grp_day["Ocupacao%"] = (grp_day["Bookados"] / grp_day["Vagas"] * 100).replace([np.inf, -np.inf], np.nan).fillna(0).round(1)
fig1 = px.bar(
    grp_day.sort_values("Data"),
    x="Data", y="Ocupacao%",
    title="Ocupação por Dia",
    labels={"Ocupacao%": "Ocupação (%)", "Data": "Data"}
)
st.plotly_chart(fig1, width="stretch")

# Gráfico — Ocupação por modalidade
grp_mod = df.groupby("Atividade", as_index=False).agg(
    Vagas=("Capacidade", "sum"),
    Bookados=("Bookados", "sum"),
    Slots=("Horario", "count")
)
grp_mod["Ocupacao%"] = (grp_mod["Bookados"] / grp_mod["Vagas"] * 100).replace([np.inf, -np.inf], np.nan).fillna(0).round(1)
fig2 = px.bar(
    grp_mod.sort_values("Ocupacao%", ascending=False),
    x="Atividade", y="Ocupacao%",
    title="Ocupação por Modalidade",
    labels={"Ocupacao%": "Ocupação (%)", "Atividade": "Modalidade"}
)
st.plotly_chart(fig2, width="stretch")

# Gráfico — Ocupação por período
grp_per = df.groupby("Periodo", as_index=False).agg(
    Vagas=("Capacidade", "sum"),
    Bookados=("Bookados", "sum"),
    Slots=("Horario", "count")
)
grp_per["Ocupacao%"] = (grp_per["Bookados"] / grp_per["Vagas"] * 100).replace([np.inf, -np.inf], np.nan).fillna(0).round(1)
order_map = {"Manhã": 0, "Tarde": 1, "Noite": 2, "Indefinido": 3}
grp_per = grp_per.sort_values(by="Periodo", key=lambda s: s.map(order_map))
fig3 = px.bar(
    grp_per,
    x="Periodo", y="Ocupacao%",
    title="Ocupação por Período",
    labels={"Ocupacao%": "Ocupação (%)", "Periodo": "Período"}
)
st.plotly_chart(fig3, width="stretch")

# Heatmap — Data × Horário
grp_hh = df.groupby(["Data", "Horario"], as_index=False).agg(
    Vagas=("Capacidade", "sum"),
    Bookados=("Bookados", "sum")
)
grp_hh["Ocupacao%"] = (grp_hh["Bookados"] / grp_hh["Vagas"] * 100).replace([np.inf, -np.inf], np.nan).fillna(0).round(1)
def _hhmm_to_minutes(hhmm):
    try:
        h, m = str(hhmm)[:5].split(":")
        return int(h) * 60 + int(m)
    except Exception:
        return 0
grp_hh = grp_hh.sort_values(by="Horario", key=lambda s: s.map(_hhmm_to_minutes))
fig4 = px.density_heatmap(
    grp_hh, x="Data", y="Horario", z="Ocupacao%",
    color_continuous_scale="RdYlGn",
    title="Heatmap — Ocupação por Data × Horário",
    nbinsx=len(grp_hh["Data"].unique())
)
fig4.update_coloraxes(colorbar_title="Ocupação %", cmin=0, cmax=100)
st.plotly_chart(fig4, width="stretch")

st.divider()

# ──────────────────────────────────────────────────────────────────────────────
# Visualização extra: Calendário por mês
# ──────────────────────────────────────────────────────────────────────────────
st.subheader("Calendário (mensal)")
daily = _daily_agg(df)

# Meses disponíveis no recorte filtrado
if daily.empty:
    st.info("Sem dados para montar o calendário no período selecionado.")
else:
    min_m = daily["Data"].min().replace(day=1)
    max_m = daily["Data"].max().replace(day=1)
    months_list = []
    cur = min_m
    while cur <= max_m:
        months_list.append(cur)
        # avança 1 mês
        y, m = cur.year, cur.month
        if m == 12:
            cur = date(y + 1, 1, 1)
        else:
            cur = date(y, m + 1, 1)

    # seletor de mês
    month_labels = [f"{pycal.month_name[m.month]} {m.year}" for m in months_list]
    idx_default = len(months_list) - 1  # último mês por padrão
    sel = st.selectbox("Selecione o mês", options=list(range(len(months_list))), format_func=lambda i: month_labels[i], index=idx_default)

    # métrica de cor
    color_metric = st.radio(
        "Métrica (cor) do calendário",
        options=["Ocupacao%", "Slots", "Vagas", "VagasSobrando"],
        horizontal=True,
        index=0
    )
    # NOVO: mostrar números dentro do quadradinho
    show_values_in_cell = st.checkbox("Mostrar números no calendário", value=True)

    sel_month = months_list[sel]
    dmin = sel_month
    dmax = (sel_month.replace(day=pycal.monthrange(sel_month.year, sel_month.month)[1]))
    daily_month = daily[(daily["Data"] >= dmin) & (daily["Data"] <= dmax)].copy()

    fig_cal = make_calendar_figure(daily_month, sel_month.year, sel_month.month, color_metric)
    st.plotly_chart(fig_cal, width="stretch")

st.divider()

st.divider()
st.subheader("Atualização dos dados")

# Sugestão: usar as datas dos filtros atuais como "período desejado"
df_periodo_min = df["Data"].min()
df_periodo_max = df["Data"].max()
col_up1, col_up2 = st.columns([1,2])

with col_up1:
    if st.button("🔄 Atualizar agora", type="primary"):
        with st.spinner("Atualizando... isso pode levar alguns segundos/minutos."):
            ok, msg = _run_coletor(
                date_from=df_periodo_min.isoformat() if pd.notna(df_periodo_min) else None,
                date_to=df_periodo_max.isoformat() if pd.notna(df_periodo_max) else None
            )
        if ok:
            st.success("Atualizado com sucesso! Recarregando dados...")
            # recarrega o CSV mais novo
            latest = _find_latest_slots_csv()
            if latest:
                df_slots = _ensure_base_columns(_read_csv_safely(latest))
                st.rerun()  # redesenha a página (nova API Streamlit)
            else:
                st.warning("Coletor rodou, mas não encontrei novo slots_*.csv.")
        else:
            st.error("Falha ao atualizar 😕")
            with st.expander("Detalhes do erro"):
                st.code(msg, language="bash")

with col_up2:
    st.caption("Dica: o botão usa as datas dos filtros atuais como período alvo. "
               "Se o coletor ignorar argumentos, ele usará o padrão interno (próximos N dias).")


# Tabela & Downloads
st.subheader("Dados filtrados (detalhado)")
st.dataframe(
    df.sort_values(["Data", "Horario", "Atividade"]).reset_index(drop=True),
    use_container_width=True, height=420
)

col_a, col_b = st.columns(2)
with col_a:
    _download_button_csv(
        df.sort_values(["Data", "Horario", "Atividade"]),
        "⬇️ Baixar dados filtrados (CSV)",
        "dados_filtrados.csv"
    )
with col_b:
    _download_button_csv(
        grp_day.sort_values("Data"),
        "⬇️ Baixar ocupação por dia (CSV)",
        "ocupacao_por_dia.csv"
    )

st.caption("Atualize os CSVs com um novo processamento e clique em **Rerun** (Ctrl+R) no Streamlit).")
st.write("—")
st.caption("Feito com ❤️ em Streamlit + Plotly")

