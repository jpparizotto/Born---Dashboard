# -*- coding: utf-8 -*-
"""
Dashboard de Ocupação — Born to Ski (Streamlit Cloud-ready)
- Lê e também GERA CSVs de slots (rota EVO /activities/schedule)
- Botão "Atualizar agora" executa a coleta online (sem subprocess)
- Filtros: Data, Modalidade, Período, Horário
- Visuais Plotly + Calendário com números no quadrinho
"""

import os
import io
import glob
import calendar as pycal
from datetime import datetime, date, timedelta

import numpy as np
import pandas as pd
import requests
from dateutil.parser import parse as parse_date
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

# ──────────────────────────────────────────────────────────────────────────────
# CONFIG GERAL
# ──────────────────────────────────────────────────────────────────────────────
APP_TITLE = "Dashboard de Ocupação — Born to Ski"
DATA_DIR = "evo_ocupacao"
os.makedirs(DATA_DIR, exist_ok=True)

# Credenciais (preferir st.secrets; fallback env; último recurso: vazias)
EVO_USER = st.secrets.get("EVO_USER", os.environ.get("EVO_USER", ""))
EVO_TOKEN = st.secrets.get("EVO_TOKEN", os.environ.get("EVO_TOKEN", ""))
BASE_URL = "https://evo-integracao.w12app.com.br/api/v1"
VERIFY_SSL = True

# Defaults de período quando o usuário clicar em Atualizar (se não escolher outro)
DAYS_AHEAD_DEFAULT = 21

# Senha para liberar o botão (opcional)
DASH_PWD = st.secrets.get("DASHBOARD_PASSWORD", os.environ.get("DASHBOARD_PASSWORD", ""))

# ──────────────────────────────────────────────────────────────────────────────
# HELPERS GERAIS
# ──────────────────────────────────────────────────────────────────────────────
def _read_csv_safely(path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(path, encoding="utf-8")
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="latin1")

def _download_button_csv(df: pd.DataFrame, label: str, filename: str):
    csv_bytes = df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
    st.download_button(label, data=csv_bytes, file_name=filename, mime="text/csv")

def _kpi_block(label, value, help_text=None):
    st.metric(label=label, value=value, help=help_text)

def _hhmm_to_minutes(hhmm):
    try:
        h, m = str(hhmm)[:5].split(":")
        return int(h) * 60 + int(m)
    except Exception:
        return 0

def _find_latest_slots_csv():
    pattern = os.path.join(DATA_DIR, "slots_*.csv")
    files = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)
    return files[0] if files else None

# ──────────────────────────────────────────────────────────────────────────────
# NORMALIZAÇÃO DE DADOS
# ──────────────────────────────────────────────────────────────────────────────
def _ensure_base_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c: c.strip() for c in df.columns}
    df = df.rename(columns=cols)

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

    required = ["Data", "Horario", "Periodo", "Atividade", "Início", "Fim", "Capacidade", "Disponíveis", "Bookados", "ActivityId"]
    for r in required:
        if r not in df.columns:
            df[r] = None

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

    for col in ["Capacidade", "Disponíveis", "Bookados"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

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

    if "Ocupacao%" not in df.columns:
        df["Ocupacao%"] = (df["Bookados"] / df["Capacidade"] * 100)
        df["Ocupacao%"] = df["Ocupacao%"].replace([np.inf, -np.inf], np.nan).fillna(0).round(1)

    return df

def _load_data() -> pd.DataFrame:
    latest = _find_latest_slots_csv()
    if not latest:
        return pd.DataFrame()
    return _ensure_base_columns(_read_csv_safely(latest))

# ──────────────────────────────────────────────────────────────────────────────
# COLETOR EVO (EMBUTIDO)
# ──────────────────────────────────────────────────────────────────────────────
def _auth_headers():
    if not EVO_USER or not EVO_TOKEN:
        raise RuntimeError("Credenciais EVO ausentes. Defina EVO_USER e EVO_TOKEN em Secrets.")
    import base64
    auth_str = f"{EVO_USER}:{EVO_TOKEN}"
    b64 = base64.b64encode(auth_str.encode()).decode()
    return {
        "Authorization": f"Basic {b64}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

def _get_json(url, params=None):
    r = requests.get(url, headers=_auth_headers(), params=params or {}, verify=VERIFY_SSL, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"GET {url} -> {r.status_code} | {r.text[:300]}")
    try:
        return r.json()
    except Exception as e:
        raise RuntimeError(f"Falha ao interpretar JSON de {url}: {e}\nCorpo: {r.text[:500]}")

def _to_list(maybe, key="data"):
    if isinstance(maybe, list):
        return maybe
    if isinstance(maybe, dict):
        if key in maybe and isinstance(maybe[key], list):
            return maybe[key]
        for v in maybe.values():
            if isinstance(v, list):
                return v
    return []

def _first(obj, *keys, default=None):
    for k in keys:
        if isinstance(obj, dict) and k in obj and obj[k] not in (None, "", []):
            return obj[k]
    return default

def _normalize_date_only(s):
    if not s:
        return s
    if isinstance(s, str) and "T" in s:
        return s.split("T", 1)[0]
    return s

def _each_date_list(date_from_iso, date_to_iso):
    d0 = date.fromisoformat(date_from_iso)
    d1 = date.fromisoformat(date_to_iso)
    cur = d0
    out = []
    while cur <= d1:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out

def _listar_atividades():
    data = _get_json(f"{BASE_URL}/activities")
    atividades = _to_list(data, key="data")
    if not atividades:
        try:
            alt = _get_json(f"{BASE_URL}/service")
            atividades = _to_list(alt, key="data")
        except Exception:
            pass
    res = []
    for a in atividades:
        res.append({
            "name": _first(a, "name", "description", "title", default="(Sem nome)"),
            "id": _first(a, "id", "idActivity", "activityId", "ID", "Id"),
        })
    return res

def _listar_id_branch():
    try:
        cfg = _get_json(f"{BASE_URL}/configuration")
        lst = _to_list(cfg, key="data")
        for b in lst:
            bid = _first(b, "idBranch", "branchId", "id", "Id")
            if bid:
                return bid
    except Exception:
        pass
    return None

def _fetch_agenda_dia(d_iso, id_branch=None):
    params = {"date": f"{d_iso}T00:00:00", "showFullWeek": "false"}
    if id_branch:
        params["idBranch"] = id_branch
    data = _get_json(f"{BASE_URL}/activities/schedule", params=params)
    items = _to_list(data, key="data")
    for it in items:
        it["_requestedDate"] = d_iso  # carimbo do dia solicitado
    return items

def _fetch_agenda_periodo(date_from, date_to):
    id_branch = _listar_id_branch()
    all_items = []
    for d_iso in _each_date_list(date_from, date_to):
        try:
            items = _fetch_agenda_dia(d_iso, id_branch=id_branch)
            all_items.extend(items)
        except Exception as e:
            # log mínimo; sem emojis por compatibilidade
            print(f"Falha ao coletar {d_iso}: {e}")
    return all_items

def _safe_int(x):
    try:
        return int(x)
    except Exception:
        return None

def _materialize_rows(atividades, agenda_items):
    rows = []
    act_names = {a["name"].strip().lower(): a for a in atividades if a["name"]}
    for h in agenda_items:
        act_name_item = _first(h, "name", "activityDescription", "activityName", "description")
        if act_name_item:
            act_key = act_name_item.strip().lower()
            act_resolved = act_names.get(act_key)
            act_name_final = act_resolved["name"] if act_resolved else act_name_item
            act_id_final = act_resolved["id"] if act_resolved else _first(h, "idActivity", "activityId", "id", "Id")
        else:
            act_name_final = "(Sem atividade)"
            act_id_final = _first(h, "idActivity", "activityId", "id", "Id")

        date_val = _first(h, "_requestedDate") or _normalize_date_only(_first(h, "activityDate", "date", "classDate", "day", "scheduleDate"))

        hour_start = _first(h, "startTime", "hourStart", "timeStart", "startHour")
        hour_end = _first(h, "endTime", "hourEnd", "timeEnd", "endHour")

        schedule_id = _first(
            h,
            "idAtividadeSessao", "idConfiguration", "idGroupActivity",
            "idActivitySchedule", "scheduleId", "idSchedule",
            "idActivityScheduleClass", "idClassSchedule", "idScheduleClass",
            "idActivityScheduleTime", "activityScheduleId",
            "idClass", "idTime", "id", "Id"
        )

        capacity = _safe_int(_first(h, "capacity", "spots", "vacanciesTotal", "maxStudents", "maxCapacity"))
        filled   = _safe_int(_first(h, "ocupation", "spotsFilled", "occupied", "enrolled", "registrations"))
        available = _safe_int(_first(h, "available", "vacancies"))
        if available is None and capacity is not None and filled is not None:
            available = max(0, capacity - filled)

        if date_val:
            rows.append({
                "Data": date_val,
                "Atividade": act_name_final,
                "Início": hour_start,
                "Fim": hour_end,
                "Horario": hour_start if hour_start else None,
                "Capacidade": capacity or 0,
                "Disponíveis": (available or 0),
                "Bookados": (filled or (capacity or 0) - (available or 0) if capacity is not None and available is not None else 0),
                "ScheduleId": schedule_id,
                "ActivityId": act_id_final
            })

    rows.sort(key=lambda r: (r["Data"], r.get("Horario") or "", r["Atividade"]))
    return rows

def gerar_csv(date_from: str | None = None, date_to: str | None = None) -> str:
    """
    Coleta dias no período [date_from, date_to] (formato YYYY-MM-DD).
    Se None, usa hoje + DAYS_AHEAD_DEFAULT.
    Salva CSV em evo_ocupacao/ e retorna o caminho.
    """
    today = date.today()
    if not date_from or not date_to:
        d0 = today
        d1 = today + timedelta(days=DAYS_AHEAD_DEFAULT)
    else:
        d0 = date.fromisoformat(date_from)
        d1 = date.fromisoformat(date_to)
        if d1 < d0:
            raise ValueError("date_to não pode ser menor que date_from.")

    df_iso_from = d0.isoformat()
    df_iso_to = d1.isoformat()

    atividades = _listar_atividades()
    agenda_all = _fetch_agenda_periodo(df_iso_from, df_iso_to)
    rows = _materialize_rows(atividades, agenda_all)
    if not rows:
        raise RuntimeError("Nenhum slot retornado pela API no período solicitado.")

    df = pd.DataFrame(rows)
    # calcula Bookados se faltou; já foi feito no materialize_rows mas reforçamos:
    if "Bookados" not in df.columns or df["Bookados"].isna().all():
        df["Bookados"] = (df["Capacidade"].fillna(0) - df["Disponíveis"].fillna(0)).clip(lower=0).astype(int)

    # salva CSV
    fname = f"slots_{df_iso_from}_a_{df_iso_to}.csv"
    fpath = os.path.join(DATA_DIR, fname)
    df.to_csv(fpath, index=False, encoding="utf-8-sig")
    return fpath

# ──────────────────────────────────────────────────────────────────────────────
# CALENDÁRIO
# ──────────────────────────────────────────────────────────────────────────────
def _daily_agg(df: pd.DataFrame) -> pd.DataFrame:
    g = df.groupby("Data", as_index=False).agg(
        Slots=("Horario", "count"),
        Vagas=("Capacidade", "sum"),
        Bookados=("Bookados", "sum"),
    )
    g["Ocupacao%"] = (g["Bookados"] / g["Vagas"] * 100).replace([np.inf, -np.inf], np.nan).fillna(0).round(1)
    g["VagasSobrando"] = (g["Vagas"] - g["Bookados"]).astype(int)
    # garantir Data como date
    g["Data"] = pd.to_datetime(g["Data"]).dt.date
    return g

def _month_calendar_frame(daily: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    n_days = pycal.monthrange(year, month)[1]
    first_wd = date(year, month, 1).weekday()  # Monday=0
    rows = []
    daily_map = daily.set_index("Data").to_dict(orient="index")
    for d in range(1, n_days + 1):
        dt_i = date(year, month, d)
        offset = first_wd + (d - 1)
        week_idx = offset // 7
        wd = offset % 7
        rec = daily_map.get(dt_i, {})
        rows.append({
            "Data": dt_i,
            "day_num": d,
            "weekday": wd,
            "week_index": week_idx,
            "Slots": int(rec.get("Slots", 0) or 0),
            "Vagas": int(rec.get("Vagas", 0) or 0),
            "Bookados": int(rec.get("Bookados", 0) or 0),
            "Ocupacao%": float(rec.get("Ocupacao%", 0.0) or 0.0),
            "VagasSobrando": int(rec.get("VagasSobrando", 0) or 0),
        })
    return pd.DataFrame(rows)

def make_calendar_figure(daily_df: pd.DataFrame, year: int, month: int, color_metric: str, show_values_in_cell: bool = True) -> go.Figure:
    cal = _month_calendar_frame(daily_df, year, month)
    x_labels = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]
    max_week = cal["week_index"].max() if not cal.empty else 5
    n_weeks = int(max_week) + 1

    z = [[None for _ in range(7)] for __ in range(n_weeks)]
    text = [["" for _ in range(7)] for __ in range(n_weeks)]
    custom = [[None for _ in range(7)] for __ in range(n_weeks)]

    for _, r in cal.iterrows():
        wi = int(r["week_index"]); wd = int(r["weekday"])
        slots = int(r["Slots"]); vagas = int(r["Vagas"]); book = int(r["Bookados"])
        occ = float(r["Ocupacao%"]); sobr = int(r["VagasSobrando"])

        z_val = {"Ocupacao%": occ, "VagasSobrando": sobr, "Vagas": vagas}.get(color_metric, slots)
        z[wi][wd] = float(z_val)

        if show_values_in_cell:
            text[wi][wd] = f"{int(r['day_num'])}\nS:{slots} V:{vagas}\nOcc:{occ:.0f}% Sob:{sobr}"
        else:
            text[wi][wd] = str(int(r["day_num"]))

        custom[wi][wd] = {"data": r["Data"], "slots": slots, "vagas": vagas, "book": book, "occ": occ, "sobr": sobr}

    if color_metric == "Ocupacao%":
        colorscale = "RdYlGn"; zmin, zmax = 0, 100; ctitle = "Ocupação %"
    elif color_metric == "VagasSobrando":
        colorscale = "Blues"; zmin, zmax = 0, max(1, cal["VagasSobrando"].max()); ctitle = "Vagas sobrando"
    elif color_metric == "Vagas":
        colorscale = "Greens"; zmin, zmax = 0, max(1, cal["Vagas"].max()); ctitle = "Vagas"
    else:
        colorscale = "Oranges"; zmin, zmax = 0, max(1, cal["Slots"].max()); ctitle = "Slots"

    font_size = max(9, 12 - max(0, n_weeks - 5) * 2)

    fig = go.Figure(data=go.Heatmap(
        z=z, x=list(range(7)), y=list(range(n_weeks)),
        colorscale=colorscale, zmin=zmin, zmax=zmax,
        showscale=True, colorbar=dict(title=ctitle),
        text=text, texttemplate="%{text}", textfont={"size": font_size},
        customdata=custom,
        hovertemplate=(
            "<b>%{customdata.data|%Y-%m-%d}</b><br>"
            "Slots: %{customdata.slots}<br>"
            "Vagas: %{customdata.vagas}<br>"
            "Ocupação: %{customdata.occ:.1f}%<br>"
            "Vagas sobrando: %{customdata.sobr}<extra></extra>"
        ),
    ))

    fig.update_xaxes(tickmode="array", tickvals=list(range(7)), ticktext=x_labels, side="top", showgrid=False)
    fig.update_yaxes(tickmode="array", tickvals=list(range(n_weeks)), ticktext=[f"Semana {i+1}" for i in range(n_weeks)], autorange="reversed", showgrid=False)
    fig.update_layout(title=f"Calendário — {pycal.month_name[month]} {year}", xaxis_title="", yaxis_title="", margin=dict(l=10, r=10, t=50, b=10), height=320 + 40 * n_weeks)
    return fig

# ──────────────────────────────────────────────────────────────────────────────
# APP
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
    st.caption("Dica: o script interno gera `slots_YYYY-MM-DD_a_YYYY-MM-DD.csv` nessa pasta.")

if df_slots.empty:
    st.info("Carregue ou gere um CSV de slots para visualizar o dashboard.")
    # Ainda assim mostramos o bloco de atualização para gerar pela nuvem:
else:
    pass

# Filtros
st.sidebar.header("Filtros")
if not df_slots.empty:
    min_date = df_slots["Data"].dropna().min()
    max_date = df_slots["Data"].dropna().max()
else:
    # sem CSV ainda, defina defaults
    min_date = date.today()
    max_date = date.today() + timedelta(days=DAYS_AHEAD_DEFAULT)

st.sidebar.header("Coleta (Atualizar)")
coleta_mode = st.sidebar.radio(
    "Período da coleta",
    ["Usar filtros atuais", f"Hoje + {DAYS_AHEAD_DEFAULT} dias", "Hoje + dias personalizados"],
    index=0
)
dias_custom = None
if coleta_mode == "Hoje + dias personalizados":
    dias_custom = st.sidebar.number_input(
        "Quantidade de dias", 
        min_value=1, max_value=60, 
        value=DAYS_AHEAD_DEFAULT, step=1
    )

default_from = min_date if isinstance(min_date, date) else date.today()
default_to = max_date if isinstance(max_date, date) else date.today()

# permite escolher datas além do CSV atual (ex.: até +60 dias)
picker_max = max(
    default_to,
    date.today() + timedelta(days=60)  # ajuste se quiser
)

date_range = st.sidebar.date_input(
    "Período",
    value=(default_from, default_to),
    min_value=default_from,   # pode deixar como está
    max_value=picker_max      # <<< novo teto do seletor
)

if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
    f_date_from, f_date_to = date_range
else:
    f_date_from, f_date_to = default_from, default_to

if not df_slots.empty:
    modalidades = sorted([m for m in df_slots["Atividade"].dropna().unique()])
    periodos = ["Manhã", "Tarde", "Noite"]
    sel_modalidades = st.sidebar.multiselect("Modalidades (Atividade)", modalidades, default=modalidades)
    sel_periodos = st.sidebar.multiselect("Períodos", periodos, default=periodos)
    horas = sorted([h for h in df_slots["Horario"].dropna().unique()])
    sel_horas = st.sidebar.multiselect("Horários (opcional)", horas)
else:
    sel_modalidades = []; sel_periodos = []; sel_horas = []

# ──────────────────────────────────────────────────────────────────────────────
# BLOCO: ATUALIZAÇÃO ONLINE
# ──────────────────────────────────────────────────────────────────────────────
st.divider()
st.subheader("Atualização dos dados")

col_up_a, col_up_b = st.columns([1, 2])
with col_up_a:
    pwd_ok = True
    if DASH_PWD:
        senha = st.text_input("Senha (admin)", type="password", placeholder="Digite a senha para liberar")
        pwd_ok = (senha == DASH_PWD)
        if not pwd_ok:
            st.caption("Dica: defina `DASHBOARD_PASSWORD` nos Secrets. Sem senha, o botão fica bloqueado.")

    btn = st.button("🔄 Atualizar agora", type="primary", disabled=not pwd_ok)

    if btn:
    try:
        with st.spinner("Coletando dados do EVO e gerando CSV..."):
            if coleta_mode == "Usar filtros atuais":
                start = f_date_from
                end = f_date_to
            elif coleta_mode.startswith("Hoje +"):
                today = date.today()
                n = DAYS_AHEAD_DEFAULT if dias_custom is None else dias_custom
                start = today
                end = today + timedelta(days=int(n))

            path = gerar_csv(start.isoformat(), end.isoformat())
        st.success(f"Atualizado com sucesso!\nArquivo: {os.path.basename(path)}")
        st.rerun()
    except Exception as e:
        st.error("Falha ao atualizar os dados.")
        with st.expander("Detalhes"):
            st.code(str(e))

with col_up_b:
    st.caption("O botão gera um novo CSV no servidor (pasta `evo_ocupacao/`) e recarrega o painel.")

# Se ainda não há dados, paramos aqui (depois do botão o usuário pode gerar)
if df_slots.empty:
    st.stop()

# ──────────────────────────────────────────────────────────────────────────────
# Aplicar filtros aos dados carregados
# ──────────────────────────────────────────────────────────────────────────────
df = df_slots.copy()
mask = (df["Data"] >= pd.to_datetime(f_date_from).date()) & (df["Data"] <= pd.to_datetime(f_date_to).date())
if sel_modalidades:
    mask &= df["Atividade"].isin(sel_modalidades)
if sel_periodos:
    mask &= df["Periodo"].isin(sel_periodos)
if sel_horas:
    mask &= df["Horario"].isin(sel_horas)
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
grp_day = df.groupby("Data", as_index=False).agg(Vagas=("Capacidade", "sum"), Bookados=("Bookados", "sum"), Slots=("Horario", "count"))
grp_day["Ocupacao%"] = (grp_day["Bookados"] / grp_day["Vagas"] * 100).replace([np.inf, -np.inf], np.nan).fillna(0).round(1)
fig1 = px.bar(grp_day.sort_values("Data"), x="Data", y="Ocupacao%", title="Ocupação por Dia", labels={"Ocupacao%": "Ocupação (%)", "Data": "Data"})
st.plotly_chart(fig1, width="stretch")

# Gráfico — Ocupação por modalidade
grp_mod = df.groupby("Atividade", as_index=False).agg(Vagas=("Capacidade", "sum"), Bookados=("Bookados", "sum"), Slots=("Horario", "count"))
grp_mod["Ocupacao%"] = (grp_mod["Bookados"] / grp_mod["Vagas"] * 100).replace([np.inf, -np.inf], np.nan).fillna(0).round(1)
fig2 = px.bar(grp_mod.sort_values("Ocupacao%", ascending=False), x="Atividade", y="Ocupacao%", title="Ocupação por Modalidade", labels={"Ocupacao%": "Ocupação (%)", "Atividade": "Modalidade"})
st.plotly_chart(fig2, width="stretch")

# Gráfico — Ocupação por período
grp_per = df.groupby("Periodo", as_index=False).agg(Vagas=("Capacidade", "sum"), Bookados=("Bookados", "sum"), Slots=("Horario", "count"))
grp_per["Ocupacao%"] = (grp_per["Bookados"] / grp_per["Vagas"] * 100).replace([np.inf, -np.inf], np.nan).fillna(0).round(1)
order_map = {"Manhã": 0, "Tarde": 1, "Noite": 2, "Indefinido": 3}
grp_per = grp_per.sort_values(by="Periodo", key=lambda s: s.map(order_map))
fig3 = px.bar(grp_per, x="Periodo", y="Ocupacao%", title="Ocupação por Período", labels={"Ocupacao%": "Ocupação (%)", "Periodo": "Período"})
st.plotly_chart(fig3, width="stretch")

# Heatmap — Data × Horário
grp_hh = df.groupby(["Data", "Horario"], as_index=False).agg(Vagas=("Capacidade", "sum"), Bookados=("Bookados", "sum"))
grp_hh["Ocupacao%"] = (grp_hh["Bookados"] / grp_hh["Vagas"] * 100).replace([np.inf, -np.inf], np.nan).fillna(0).round(1)
grp_hh = grp_hh.sort_values(by="Horario", key=lambda s: s.map(_hhmm_to_minutes))
fig4 = px.density_heatmap(grp_hh, x="Data", y="Horario", z="Ocupacao%", color_continuous_scale="RdYlGn", title="Heatmap — Ocupação por Data × Horário", nbinsx=len(grp_hh["Data"].unique()))
fig4.update_coloraxes(colorbar_title="Ocupação %", cmin=0, cmax=100)
st.plotly_chart(fig4, width="stretch")

st.divider()

# Calendário
st.subheader("Calendário (mensal)")
daily = _daily_agg(df)
if daily.empty:
    st.info("Sem dados para montar o calendário no período selecionado.")
else:
    min_m = daily["Data"].min().replace(day=1)
    max_m = daily["Data"].max().replace(day=1)
    months_list = []
    cur = min_m
    while cur <= max_m:
        months_list.append(cur)
        y, m = cur.year, cur.month
        cur = date(y + 1, 1, 1) if m == 12 else date(y, m + 1, 1)

    month_labels = [f"{pycal.month_name[m.month]} {m.year}" for m in months_list]
    idx_default = len(months_list) - 1
    sel = st.selectbox("Selecione o mês", options=list(range(len(months_list))), format_func=lambda i: month_labels[i], index=idx_default)
    color_metric = st.radio("Métrica (cor) do calendário", options=["Ocupacao%", "Slots", "Vagas", "VagasSobrando"], horizontal=True, index=0)
    show_values_in_cell = st.checkbox("Mostrar números no calendário", value=True)

    sel_month = months_list[sel]
    dmin = sel_month
    dmax = sel_month.replace(day=pycal.monthrange(sel_month.year, sel_month.month)[1])
    daily_month = daily[(daily["Data"] >= dmin) & (daily["Data"] <= dmax)].copy()
    fig_cal = make_calendar_figure(daily_month, sel_month.year, sel_month.month, color_metric, show_values_in_cell=show_values_in_cell)
    st.plotly_chart(fig_cal, width="stretch")

st.divider()

# Tabela & Downloads
st.subheader("Dados filtrados (detalhado)")
st.dataframe(df.sort_values(["Data", "Horario", "Atividade"]).reset_index(drop=True), use_container_width=True, height=420)

col_a, col_b = st.columns(2)
with col_a:
    _download_button_csv(df.sort_values(["Data", "Horario", "Atividade"]), "⬇️ Baixar dados filtrados (CSV)", "dados_filtrados.csv")
with col_b:
    _download_button_csv(grp_day.sort_values("Data"), "⬇️ Baixar ocupação por dia (CSV)", "ocupacao_por_dia.csv")

st.caption("Feito com ❤️ em Streamlit + Plotly — coleta online via EVO")




