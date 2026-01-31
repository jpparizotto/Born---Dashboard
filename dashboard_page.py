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

# ──────────────────────────────────────────────────────────────────────────────
# NÍVEIS (DICIONÁRIO VIA CSV NO GITHUB)
# - Enquanto a EVO não libera o endpoint oficial, carregamos um CSV público (raw do GitHub)
# - O CSV deve ter pelo menos: idCliente ; niveis
#   (niveis pode conter histórico separado por vírgula, ex: "1ASK,1CSK,2ASK,3ASB")
# ──────────────────────────────────────────────────────────────────────────────
LEVELS_CSV_URL = st.secrets.get("LEVELS_CSV_URL", os.environ.get("LEVELS_CSV_URL", ""))  # raw github (opcional)
LEVELS_CSV_LOCAL = "data/clientes_niveis.csv"  # fallback local (opcional)

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

import re

def _normalize_level_code(code: str) -> str:
    c = (code or "").strip().upper()
    if not c:
        return ""
    # typos comuns (mantenha SKK! é válido)
    c = c.replace("SBB", "SB")
    # remove caracteres estranhos
    c = re.sub(r"[^0-9A-Z]", "", c)

    # normalização defensiva (opcional, mas ajuda se vier duplicado)
    if c.endswith("SKKK"):
        c = c[:-4] + "SKK"  # ex: 2ASKKK -> 2ASKK

    return c

def _parse_levels_history(niveis_raw: str | None) -> dict:
    """Retorna {"ski": "<ultimo SK/SKK/KC>", "snow": "<ultimo SB>"}."""
    out = {"ski": "", "snow": ""}

    if niveis_raw is None or (isinstance(niveis_raw, float) and pd.isna(niveis_raw)):
        return out

    s = str(niveis_raw).strip()
    if not s:
        return out

    # histórico separado por vírgulas (às vezes vem com ; | /)
    parts = [p.strip() for p in re.split(r"[,\|;/]+", s) if p.strip()]
    parts = [_normalize_level_code(p) for p in parts if p.strip()]

    # pega o mais da direita
    for p in reversed(parts):
        if not out["ski"] and (p.endswith("SK") or p.endswith("SKK") or p.endswith("KC")):
            out["ski"] = p
        if not out["snow"] and (p.endswith("SB") or p.endswith("SBK")):
            out["snow"] = p
        if out["ski"] and out["snow"]:
            break

    return out

@st.cache_data(show_spinner=False, ttl=6 * 3600)
def _load_levels_dict() -> dict[int, dict]:
    """Carrega o dicionário {idCliente: {ski, snow}} a partir do CSV (GitHub raw ou arquivo local)."""
    def _read_levels_csv(source: str) -> pd.DataFrame:
        attempts = [
            dict(sep=";", encoding="utf-8-sig"),
            dict(sep=";", encoding="utf-8"),
            dict(sep=";", encoding="latin1"),
            dict(sep=",", encoding="utf-8-sig"),
        ]
        last_err = None
        for kw in attempts:
            try:
                return pd.read_csv(
                    source,
                    dtype=str,
                    keep_default_na=False,
                    na_filter=False,
                    engine="python",
                    on_bad_lines="skip",
                    **kw,
                )
            except Exception as e:
                last_err = e

        raise RuntimeError(
            "Falha ao ler o CSV de níveis. Verifique se a URL é RAW do GitHub e se o arquivo está bem formatado.\n"
            f"Fonte: {source}\n"
            f"Erro: {last_err}"
        )

    if LEVELS_CSV_URL:
        df_lv = _read_levels_csv(LEVELS_CSV_URL)
    else:
        if os.path.exists(LEVELS_CSV_LOCAL):
            df_lv = _read_levels_csv(LEVELS_CSV_LOCAL)
        else:
            return {}

    df_lv = df_lv.rename(columns={c: str(c).strip() for c in df_lv.columns})

    if "idCliente" not in df_lv.columns:
        return {}

    col_niveis = "niveis" if "niveis" in df_lv.columns else ("Niveis" if "Niveis" in df_lv.columns else None)
    if not col_niveis:
        return {}

    out: dict[int, dict] = {}
    for _, row in df_lv.iterrows():
        try:
            idc = int(str(row.get("idCliente", "")).strip())
        except Exception:
            continue
        lv = _parse_levels_history(row.get(col_niveis))
        out[idc] = lv

    return out

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

    if "Professor" not in df.columns:
        df["Professor"] = None

    for col in ["Professor", "Aluno 1", "Aluno 2", "Aluno 3"]:
        if col not in df.columns:
            df[col] = None

    for cand in (["Pista", "Track", "Lane", "Belt", "Treadmill", "Machine", "Device"]):
        if cand in df.columns:
            df = df.rename(columns={cand: "Pista"})
            break

    if "Pista" not in df.columns:
        df["Pista"] = None

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
        it["_requestedDate"] = d_iso
    return items

def _fetch_agenda_periodo(date_from, date_to):
    id_branch = _listar_id_branch()
    all_items = []
    for d_iso in _each_date_list(date_from, date_to):
        try:
            items = _fetch_agenda_dia(d_iso, id_branch=id_branch)
            all_items.extend(items)
        except Exception as e:
            print(f"Falha ao coletar {d_iso}: {e}")
    return all_items

def _safe_int(x):
    try:
        return int(x)
    except Exception:
        return None

_DETAIL_CACHE = {}

def _get_schedule_detail(config_id: int | None, activity_date_iso: str | None, id_activity_session: int | None = None):
    if not config_id and not id_activity_session:
        return {}
    key = (config_id or 0, activity_date_iso or "", id_activity_session or 0)
    if key in _DETAIL_CACHE:
        return _DETAIL_CACHE[key]
    params = {}
    if config_id and activity_date_iso:
        params["idConfiguration"] = int(config_id)
        params["activityDate"] = activity_date_iso  # "YYYY-MM-DD"
    if id_activity_session:
        params["idActivitySession"] = int(id_activity_session)
    try:
        detail = _get_json(f"{BASE_URL}/activities/schedule/detail", params=params) or {}
        if isinstance(detail, dict) and "data" in detail and isinstance(detail["data"], dict):
            detail = detail["data"]
        _DETAIL_CACHE[key] = detail
        return detail
    except Exception:
        return {}

def _extract_alunos(detail: dict, target_start: str | None = None, slot_date: str | date | None = None) -> list[dict]:
    """
    Estratégia (opção A):
    - Para a grade operacional (amanhã/forward), NÃO filtrar por `status` (porque `status` tende a ser presença, não reserva).
    - Para datas passadas, se quiser, filtrar por `status==0` (presente).
    - Sempre excluir removed/suspended; e dar preferência ao replacement quando houver duplicidade por slotNumber.
    """
    if not isinstance(detail, dict):
        return []

    # Normaliza slot_date -> date
    sd: date | None = None
    if slot_date:
        try:
            if isinstance(slot_date, date):
                sd = slot_date
            else:
                sd = date.fromisoformat(str(slot_date)[:10])
        except Exception:
            sd = None

    # Considera "futuro operacional" = amanhã ou depois
    # (você comentou que gera a grade pro dia seguinte)
    is_future_op = False
    if sd is not None:
        is_future_op = sd >= (date.today() + timedelta(days=1))

    if not target_start:
        target_start = str(_first(detail, "startTime", "hourStart", "timeStart", "startHour") or "").strip()

    enrollments = detail.get("enrollments")
    if isinstance(enrollments, list) and enrollments:
        def _to_bool(v):
            if isinstance(v, bool):
                return v
            if v is None:
                return False
            return str(v).strip().lower() in ("1", "true", "t", "yes", "y", "sim")

        def _safe_int_local(v):
            try:
                return int(v)
            except Exception:
                return None

        def _id_cliente(e):
            for k in ["idMember", "idClient", "idCliente", "idCustomer", "memberId", "clientId", "customerId"]:
                if isinstance(e, dict) and e.get(k) not in (None, "", []):
                    return _safe_int_local(e.get(k))
            return None

        def _name(e):
            for k in ["name", "fullName", "displayName", "customerName", "personName", "clientName", "description"]:
                if isinstance(e, dict) and e.get(k):
                    return str(e.get(k)).strip()
            return None

        by_slot = {}  # slotNumber -> melhor enrollment
        extras = []   # sem slotNumber

        for e in enrollments:
            if not isinstance(e, dict):
                continue

            nm = _name(e)
            if not nm:
                continue

            removed = _to_bool(e.get("removed"))
            suspended = _to_bool(e.get("suspended"))
            replacement = _to_bool(e.get("replacement"))
            justified_abs = _to_bool(e.get("justifiedAbsence"))

            if removed or suspended:
                continue

            # Para FUTURO (amanhã+): não filtra por status (reserva != presença)
            # Para PASSADO (ou sem data): pode filtrar por status==0 (presente),
            # e pode também excluir justifiedAbsence.
            if not is_future_op:
                status = _safe_int_local(e.get("status"))  # 0=presente | 1=falta | 2=falta justificada
                if status is not None and status != 0:
                    continue
                if justified_abs:
                    continue
            # Para futuro, NÃO exclui justifiedAbsence (às vezes vem inconsistente); a regra forte é removed/suspended.

            slot_num = _safe_int_local(e.get("slotNumber"))
            rec = {
                "name": nm,
                "idCliente": _id_cliente(e),
                "_slotNumber": slot_num,
                "_replacement": replacement,
            }

            if slot_num is None:
                extras.append(rec)
                continue

            prev = by_slot.get(slot_num)
            if prev is None:
                by_slot[slot_num] = rec
            else:
                # prioriza replacement=True
                if rec["_replacement"] and not prev.get("_replacement"):
                    by_slot[slot_num] = rec

        slotted = [by_slot[k] for k in sorted(by_slot.keys())]
        out = slotted + extras

        for r in out:
            r.pop("_slotNumber", None)
            r.pop("_replacement", None)

        return out

    # Fallback antigo (caso sua unidade não retorne enrollments)
    name_keys = ["name", "fullName", "displayName", "customerName", "personName", "clientName", "description"]
    id_keys   = ["idMember", "idClient", "idCliente", "idCustomer", "clientId", "customerId", "memberId"]
    list_keys = ["registrations", "enrollments", "students", "members", "customers", "clients", "participants", "users"]

    def _name(o):
        if isinstance(o, dict):
            for k in name_keys:
                if o.get(k):
                    return str(o[k]).strip()
        elif isinstance(o, str):
            return o.strip()
        return None

    def _id(o):
        if isinstance(o, dict):
            for k in id_keys:
                v = o.get(k)
                if v not in (None, "", []):
                    try:
                        return int(v)
                    except Exception:
                        pass
        return None

    def _pack(o):
        n = _name(o)
        if not n:
            return None
        return {"name": n, "idCliente": _id(o)}

    for sess_key in ["sessions", "classes", "scheduleItems"]:
        sess_list = detail.get(sess_key)
        if isinstance(sess_list, list) and sess_list:
            for sess in sess_list:
                if not isinstance(sess, dict):
                    continue
                sess_start = str(_first(sess, "startTime", "hourStart", "timeStart", "startHour") or "").strip()
                if target_start and sess_start and target_start != sess_start:
                    continue
                for lk in list_keys:
                    lst = sess.get(lk)
                    if isinstance(lst, list) and lst:
                        packed = []
                        for it in lst:
                            rec = _pack(it)
                            if rec:
                                packed.append(rec)
                        return packed

    packed = []
    for lk in list_keys:
        lst = detail.get(lk)
        if isinstance(lst, list):
            for it in lst:
                item_start = str(_first(it, "startTime", "hourStart", "timeStart") or "").strip()
                if item_start and target_start and item_start != target_start:
                    continue
                rec = _pack(it)
                if rec:
                    packed.append(rec)
            break
        elif isinstance(lst, dict):
            rec = _pack(lst)
            if rec:
                packed.append(rec)
            break

    return packed

def _extract_professor(item):
    for k in ["teacher", "teacherName", "instructor", "instructorName",
              "professional", "professionalName", "employee", "employeeName",
              "coach", "coachName"]:
        v = _first(item, k)
        if v:
            return str(v).strip()

    for list_key in ["professionals", "teachers", "employees", "instructors"]:
        lst = item.get(list_key)
        if isinstance(lst, list) and lst:
            for cand in lst:
                for name_k in ["name", "fullName", "displayName", "description"]:
                    if isinstance(cand, dict) and cand.get(name_k):
                        return str(cand[name_k]).strip()
            return str(lst[0]).strip()
    return None

def _extract_pista(container) -> str | None:
    if not isinstance(container, dict):
        return None

    keys = ["pista", "track", "trackName", "lane", "belt", "área", "area", "device", "deviceName"]

    for k in keys:
        v = container.get(k)
        if v not in (None, "", []):
            raw = str(v).strip()
            break
    else:
        loc = container.get("location")
        if isinstance(loc, dict):
            for k in keys:
                v = loc.get(k)
                if v not in (None, "", []):
                    raw = str(v).strip()
                    break
            else:
                return None
        else:
            return None

    s = raw.lower()
    map_exact = {
        "a": "A", "pista a": "A", "track a": "A", "lane a": "A",
        "b": "B", "pista b": "B", "track b": "B", "lane b": "B",
        "1": "A", "pista 1": "A", "track 1": "A", "lane 1": "A", "machine 1": "A", "esteira 1": "A",
        "2": "B", "pista 2": "B", "track 2": "B", "lane 2": "B", "machine 2": "B", "esteira 2": "B",
    }
    if s in map_exact:
        return map_exact[s]

    if any(ch.isalpha() for ch in s):
        first_alpha = next((ch for ch in s if ch.isalpha()), None)
        if first_alpha in ("a", "b"):
            return first_alpha.upper()

    digits = "".join(ch for ch in s if ch.isdigit())
    if digits == "1":
        return "A"
    if digits == "2":
        return "B"

    return None

def _materialize_rows(atividades, agenda_items, levels_dict):
    rows = []
    act_names = {a["name"].strip().lower(): a for a in atividades if a["name"]}

    for h in agenda_items:
        act_name_item = _first(h, "name", "activityDescription", "activityName", "description")
        if act_name_item:
            act_key = act_name_item.strip().lower()
            act_resolved = act_names.get(act_key)
            act_name_final = act_resolved["name"] if act_resolved else act_name_item
            act_id_final   = act_resolved["id"]   if act_resolved else _first(h, "idActivity", "activityId", "id", "Id")
        else:
            act_name_final = "(Sem atividade)"
            act_id_final   = _first(h, "idActivity", "activityId", "id", "Id")

        date_val   = _first(h, "_requestedDate") or _normalize_date_only(
                        _first(h, "activityDate", "date", "classDate", "day", "scheduleDate"))
        hour_start = _first(h, "startTime", "hourStart", "timeStart", "startHour")
        hour_end   = _first(h, "endTime",   "hourEnd",   "timeEnd",   "endHour")

        config_id = _first(h, "idConfiguration", "idActivitySchedule", "idGroupActivity", "idConfig", "configurationId")
        id_activity_session = _first(
            h,
            "idActivitySession", "idActivityScheduleClass", "idClassSchedule",
            "idScheduleClass", "idScheduleTime", "idTime", "idClass", "idSchedule"
        )

        detail = _get_schedule_detail(config_id, date_val, id_activity_session)

        prof_name = _extract_professor(h)
        if not prof_name:
            prof_name = _first(detail, "instructor", "teacher", "instructorName", "teacherName")
            if not prof_name and isinstance(detail.get("instructor"), dict):
                prof_name = _first(detail["instructor"], "name", "fullName", "displayName", "description")
            if not prof_name:
                for lk in ["teachers", "professionals", "employees", "instructors"]:
                    lst = detail.get(lk)
                    if isinstance(lst, list) and lst:
                        cand = lst[0]
                        prof_name = _first(cand, "name", "fullName", "displayName", "description") if isinstance(cand, dict) else str(cand)
                        break
        prof_name = (prof_name or "(Sem professor)")

        pista = _extract_pista(detail) or "(Sem pista)"

        schedule_id = _first(
            h,
            "idAtividadeSessao", "idConfiguration", "idGroupActivity",
            "idActivitySchedule", "scheduleId", "idSchedule",
            "idActivityScheduleClass", "idClassSchedule", "idScheduleClass",
            "idActivityScheduleTime", "activityScheduleId",
            "idClass", "idTime", "id", "Id"
        )

        capacity  = _safe_int(_first(h, "capacity", "spots", "vacanciesTotal", "maxStudents", "maxCapacity"))
        filled    = _safe_int(_first(h, "ocupation", "spotsFilled", "occupied", "enrolled", "registrations"))
        available = _safe_int(_first(h, "available", "vacancies"))
        if available is None and capacity is not None and filled is not None:
            available = max(0, capacity - filled)

        # ✅ aqui passamos slot_date=date_val para aplicar a regra "amanhã+ não filtra status"
        alunos = _extract_alunos(detail, target_start=(hour_start or None), slot_date=date_val) or []

        filled_calc = ((capacity or 0) - (available or 0)) if filled is None else filled
        filled_calc = max(0, filled_calc)
        expected    = min(filled_calc, (capacity or 0))
        alunos      = alunos[:expected]

        aluno_cols = ["vazio", "vazio", "vazio"]
        nivel_ski_cols = ["", "", ""]
        nivel_snow_cols = ["", "", ""]

        for i in range(min(3, len(alunos))):
            aluno_cols[i] = alunos[i].get("name") or "vazio"
            idc = alunos[i].get("idCliente")
            if idc:
                try:
                    idc_i = int(idc)
                except Exception:
                    idc_i = None
                if idc_i is not None:
                    lv = levels_dict.get(idc_i) or {}
                    nivel_ski_cols[i] = lv.get("ski", "") or ""
                    nivel_snow_cols[i] = lv.get("snow", "") or ""

        if capacity == 2:
            aluno_cols[2] = "N.A"
            nivel_ski_cols[2] = ""
            nivel_snow_cols[2] = ""

        if date_val:
            rows.append({
                "Data": date_val,
                "Atividade": act_name_final,
                "Pista": pista,
                "Início": hour_start,
                "Fim":   hour_end,
                "Horario": hour_start if hour_start else None,
                "Capacidade": capacity or 0,
                "Disponíveis": (available or 0),
                "Bookados": (filled or (capacity or 0) - (available or 0) if capacity is not None and available is not None else 0),
                "ScheduleId": schedule_id,
                "ActivityId": act_id_final,
                "Professor": prof_name,
                "Aluno 1": aluno_cols[0],
                "Aluno 2": aluno_cols[1],
                "Aluno 3": aluno_cols[2],
                "Aluno 1 - Nível Ski":  nivel_ski_cols[0],
                "Aluno 1 - Nível Snow": nivel_snow_cols[0],
                "Aluno 2 - Nível Ski":  nivel_ski_cols[1],
                "Aluno 2 - Nível Snow": nivel_snow_cols[1],
                "Aluno 3 - Nível Ski":  nivel_ski_cols[2],
                "Aluno 3 - Nível Snow": nivel_snow_cols[2],
            })

    rows.sort(key=lambda r: (r["Data"], r.get("Horario") or "", r["Atividade"]))
    alt_idx_by_date = {}
    for r in rows:
        if r.get("Pista") in (None, "", "(Sem pista)"):
            d = r["Data"]
            i = alt_idx_by_date.get(d, 0)
            r["Pista"] = "A" if (i % 2 == 0) else "B"
            alt_idx_by_date[d] = i + 1

    return rows

def gerar_csv(date_from: str | date | None = None, date_to: str | date | None = None) -> str:
    today = date.today()

    if not date_from or not date_to:
        d0 = today
        d1 = today + timedelta(days=DAYS_AHEAD_DEFAULT)
    else:
        if isinstance(date_from, date):
            d0 = date_from
        else:
            d0 = date.fromisoformat(str(date_from))

        if isinstance(date_to, date):
            d1 = date_to
        else:
            d1 = date.fromisoformat(str(date_to))

        if d1 < d0:
            raise ValueError("date_to não pode ser menor que date_from.")

    df_iso_from = d0.isoformat()
    df_iso_to = d1.isoformat()

    atividades = _listar_atividades()
    agenda_all = _fetch_agenda_periodo(df_iso_from, df_iso_to)
    levels_dict = _load_levels_dict()
    rows = _materialize_rows(atividades, agenda_all, levels_dict)
    if not rows:
        raise RuntimeError("Nenhum slot retornado pela API no período solicitado.")

    df = pd.DataFrame(rows)
    if "Bookados" not in df.columns or df["Bookados"].isna().all():
        df["Bookados"] = (df["Capacidade"].fillna(0) - df["Disponíveis"].fillna(0)).clip(lower=0).astype(int)

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
    g["Data"] = pd.to_datetime(g["Data"]).dt.date
    return g

def _month_calendar_frame(daily: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    n_days = pycal.monthrange(year, month)[1]
    first_wd = date(year, month, 1).weekday()
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

def make_calendar_figure(
    daily_df: pd.DataFrame,
    year: int,
    month: int,
    color_metric: str,
    show_values_in_cell: bool = True
) -> go.Figure:
    cal = _month_calendar_frame(daily_df, year, month)
    x_labels = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]
    max_week = cal["week_index"].max() if not cal.empty else 5
    n_weeks = int(max_week) + 1

    z = [[None for _ in range(7)] for __ in range(n_weeks)]
    custom = [[None for _ in range(7)] for __ in range(n_weeks)]

    for _, r in cal.iterrows():
        wi = int(r["week_index"])
        wd = int(r["weekday"])
        slots = int(r["Slots"])
        vagas = int(r["Vagas"])
        book = int(r["Bookados"])
        occ = float(r["Ocupacao%"])
        sobr = int(r["VagasSobrando"])

        if color_metric == "VagasSobrando":
            z_val = min(sobr, 10)
        elif color_metric == "Vagas":
            z_val = vagas
        elif color_metric == "Ocupacao%":
            z_val = occ
        else:
            z_val = slots

        z[wi][wd] = float(z_val)

        custom[wi][wd] = {
            "data": r["Data"],
            "slots": slots,
            "vagas": vagas,
            "book": book,
            "occ": occ,
            "sobr": sobr,
        }

    if color_metric == "Ocupacao%":
        colorscale = "RdYlGn"; zmin, zmax = 0, 100; ctitle = "Ocupação %"
    elif color_metric == "VagasSobrando":
        zmin, zmax = 0, 10
        ctitle = "Vagas sobrando"
        colorscale = [
            [0.0,   "#ffffff"],
            [0.3,   "#d6eaff"],
            [0.6,   "#83c5f7"],
            [1.0,   "#4ba3e6"],
        ]
    elif color_metric == "Vagas":
        colorscale = "Greens"; zmin, zmax = 0, max(1, cal["Vagas"].max()); ctitle = "Vagas"
    else:
        colorscale = "Oranges"; zmin, zmax = 0, max(1, cal["Slots"].max()); ctitle = "Slots"

    fig = go.Figure(
        data=go.Heatmap(
            z=z,
            x=list(range(7)),
            y=list(range(n_weeks)),
            colorscale=colorscale,
            zmin=zmin,
            zmax=zmax,
            showscale=True,
            colorbar=dict(title=ctitle),
            customdata=custom,
            hovertemplate=(
                "<b>%{customdata.data|%d/%m/%Y}</b><br>"
                "Bookados: %{customdata.book}<br>"
                "Vagas totais: %{customdata.vagas}<br>"
                "Ocupação: %{customdata.occ:.1f}%<br>"
                "Vagas sobrando: %{customdata.sobr}<extra></extra>"
            ),
        )
    )

    if show_values_in_cell:
        for _, r in cal.iterrows():
            wi = int(r["week_index"])
            wd = int(r["weekday"])
            sobr = int(r["VagasSobrando"])
            data_val = r["Data"]

            sobr_display = "+10" if sobr > 10 else str(sobr)

            if isinstance(data_val, (date, datetime)):
                date_str = data_val.strftime("%d/%m")
            else:
                date_str = str(data_val)

            is_dark = False
            if zmax > 0:
                is_dark = sobr > zmax * 0.6
            font_color = "white" if is_dark else "black"

            fig.add_annotation(
                x=wd,
                y=wi,
                showarrow=False,
                xanchor="center",
                yanchor="middle",
                align="center",
                font=dict(size=18, color=font_color),
                text=(
                    f"{sobr_display}"
                    f"<br><span style='font-size:12px'>{date_str}</span>"
                ),
            )

    fig.update_xaxes(
        tickmode="array",
        tickvals=list(range(7)),
        ticktext=[f"<b>{lbl}</b>" for lbl in x_labels],
        side="top",
        showgrid=False,
        tickfont=dict(size=16),
    )
    fig.update_yaxes(
        tickmode="array",
        tickvals=list(range(n_weeks)),
        ticktext=[""] * n_weeks,
        autorange="reversed",
        showgrid=False,
        showticklabels=False,
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
else:
    pass

st.sidebar.header("Filtros")
if not df_slots.empty:
    min_date = df_slots["Data"].dropna().min()
    max_date = df_slots["Data"].dropna().max()
else:
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

picker_max = max(
    default_to,
    date.today() + timedelta(days=60)
)

date_range = st.sidebar.date_input(
    "Período",
    value=(default_from, default_to),
    min_value=default_from,
    max_value=picker_max
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

st.divider()
st.subheader("Atualização dos dados")

col_up_a, col_up_b = st.columns([1, 2])
with col_up_a:
    btn = st.button("🔄 Atualizar agora", type="primary")

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
                    end = today + timedelta(days=n)
                else:
                    start = date.today()
                    end = date.today() + timedelta(days=DAYS_AHEAD_DEFAULT)

                path = gerar_csv(start, end)

            st.success(f"Atualizado com sucesso!\nArquivo: {os.path.basename(path)}")
            st.rerun()

        except Exception as e:
            st.error("Falha ao atualizar os dados.")
            with st.expander("Detalhes"):
                st.code(str(e))

with col_up_b:
    st.caption("O botão gera um novo CSV no servidor (pasta `evo_ocupacao/`) e recarrega o painel.")

if df_slots.empty:
    st.stop()

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

total_capacity = int(df["Capacidade"].sum())
total_booked = int(df["Bookados"].sum())
total_free = int(df["Disponíveis"].sum())
if pd.isna(total_free) or total_free < 0:
    total_free = int((df["Capacidade"] - df["Bookados"]).clip(lower=0).sum())

occ_overall = (total_booked / total_capacity * 100) if total_capacity else 0.0

kpi1, kpi2, kpi3, kpi4 = st.columns(4)
with kpi1: _kpi_block("Ocupação média", f"{occ_overall:.1f}%")
with kpi2: _kpi_block("Vagas (capacidade)", f"{total_capacity}")
with kpi3: _kpi_block("Bookados", f"{total_booked}")
with kpi4: _kpi_block("Vagas livres", f"{total_free}")

grp_day = df.groupby("Data", as_index=False).agg(
    Vagas=("Capacidade", "sum"),
    Bookados=("Bookados", "sum"),
)
grp_day["Ocupacao%"] = (grp_day["Bookados"] / grp_day["Vagas"] * 100).replace([np.inf, -np.inf], np.nan).fillna(0).round(1)

_weekdays_pt = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]
grp_day["DiaSemana"] = pd.to_datetime(grp_day["Data"]).dt.dayofweek.map(lambda i: _weekdays_pt[int(i)] if pd.notna(i) else "")

fig1 = px.bar(
    grp_day.sort_values("Data"),
    x="Data",
    y="Ocupacao%",
    title="Ocupação por Dia",
    labels={"Ocupacao%": "Ocupação (%)", "Data": "Data"},
    text="Ocupacao%",
)
fig1.update_traces(
    texttemplate="%{text:.1f}%",
    textposition="outside",
    hovertemplate="<b>%{x|%d/%m/%Y}</b><br>Dia: %{customdata[0]}<br>Ocupação: %{y:.1f}%<extra></extra>",
    customdata=np.stack([grp_day["DiaSemana"]], axis=-1),
)
fig1.update_layout(
    yaxis_title="Ocupação (%)",
    margin=dict(t=60, b=40),
    uniformtext_minsize=8,
    uniformtext_mode="hide",
)
st.plotly_chart(fig1, use_container_width=True)

fig1b = px.bar(
    grp_day.sort_values("Data"),
    x="Data",
    y="Bookados",
    title="Clientes bookados por dia",
    labels={"Bookados": "Clientes", "Data": "Data"},
    text="Bookados",
)
fig1b.update_traces(
    texttemplate="%{text:d}",
    textposition="outside",
    hovertemplate="<b>%{x|%d/%m/%Y}</b><br>Dia: %{customdata[0]}<br>Clientes: %{y:d}<extra></extra>",
    customdata=np.stack([grp_day["DiaSemana"]], axis=-1)
)
fig1b.update_layout(
    uniformtext_minsize=8,
    uniformtext_mode="hide",
    yaxis_title="Clientes (bookados)",
    margin=dict(t=60, b=40),
)
st.plotly_chart(fig1b, use_container_width=True)

grp_day["VagasSobrando"] = (grp_day["Vagas"] - grp_day["Bookados"]).clip(lower=0).astype(int)

fig1c = px.bar(
    grp_day.sort_values("Data"),
    x="Data",
    y="VagasSobrando",
    title="Vagas sobrando por dia",
    labels={"VagasSobrando": "Vagas sobrando", "Data": "Data"},
    text="VagasSobrando",
)
fig1c.update_traces(
    texttemplate="%{text:d}",
    textposition="outside",
    hovertemplate="<b>%{x|%d/%m/%Y}</b><br>Dia: %{customdata[0]}<br>Vagas sobrando: %{y:d}<extra></extra>",
    customdata=np.stack([grp_day["DiaSemana"]], axis=-1),
)
fig1c.update_layout(
    uniformtext_minsize=8,
    uniformtext_mode="hide",
    yaxis_title="Vagas sobrando",
    margin=dict(t=60, b=40),
)
st.plotly_chart(fig1c, use_container_width=True)

grp_mod = df.groupby("Atividade", as_index=False).agg(
    Vagas=("Capacidade", "sum"),
    Bookados=("Bookados", "sum"),
    Slots=("Horario", "count")
)
grp_mod["Ocupacao%"] = (grp_mod["Bookados"] / grp_mod["Vagas"] * 100).replace([np.inf, -np.inf], np.nan).fillna(0).round(1)

fig2 = px.bar(
    grp_mod.sort_values("Ocupacao%", ascending=False),
    x="Atividade",
    y="Ocupacao%",
    title="Ocupação por Modalidade",
    labels={"Ocupacao%": "Ocupação (%)", "Atividade": "Modalidade"},
    text="Ocupacao%",
)
fig2.update_traces(
    texttemplate="%{text:.1f}%",
    textposition="outside",
)
fig2.update_layout(
    yaxis_title="Ocupação (%)",
    margin=dict(t=60, b=80),
)
st.plotly_chart(fig2, width="stretch")

grp_per = df.groupby("Periodo", as_index=False).agg(Vagas=("Capacidade", "sum"), Bookados=("Bookados", "sum"), Slots=("Horario", "count"))
grp_per["Ocupacao%"] = (grp_per["Bookados"] / grp_per["Vagas"] * 100).replace([np.inf, -np.inf], np.nan).fillna(0).round(1)
order_map = {"Manhã": 0, "Tarde": 1, "Noite": 2, "Indefinido": 3}
grp_per = grp_per.sort_values(by="Periodo", key=lambda s: s.map(order_map))
fig3 = px.bar(grp_per, x="Periodo", y="Ocupacao%", title="Ocupação por Período", labels={"Ocupacao%": "Ocupação (%)", "Periodo": "Período"})
st.plotly_chart(fig3, width="stretch")

df_prof = df.copy()
if "Professor" not in df_prof.columns or df_prof["Professor"].isna().all():
    st.info("Ainda não há dados de professor neste arquivo/período. Gere um CSV novo em “🔄 Atualizar agora”.")
else:
    df_prof["Professor"] = df_prof["Professor"].fillna("(Sem professor)")
    grp_prof = df_prof.groupby("Professor", as_index=False).agg(
        Aulas=("Horario", "count"),
        Bookados=("Bookados", "sum"),
    ).sort_values("Aulas", ascending=False)

    fig_prof = px.bar(
        grp_prof, x="Professor", y="Aulas",
        title="Aulas por Professor (período selecionado)",
        labels={"Aulas": "Aulas (contagem)", "Professor": "Professor"},
        text="Aulas",
    )
    fig_prof.update_traces(
        texttemplate="%{text:d}",
        textposition="outside",
        hovertemplate="<b>%{x}</b><br>Aulas: %{y:d}<br>Bookados (soma): %{customdata[0]:d}<extra></extra>",
        customdata=np.stack([grp_prof["Bookados"]], axis=-1),
    )
    fig_prof.update_layout(xaxis_tickangle=-25, margin=dict(t=60, b=80))
    st.plotly_chart(fig_prof, use_container_width=True)

grp_hh = df.groupby(["Data", "Horario"], as_index=False).agg(Vagas=("Capacidade", "sum"), Bookados=("Bookados", "sum"))
grp_hh["Ocupacao%"] = (grp_hh["Bookados"] / grp_hh["Vagas"] * 100).replace([np.inf, -np.inf], np.nan).fillna(0).round(1)
grp_hh = grp_hh.sort_values(by="Horario", key=lambda s: s.map(_hhmm_to_minutes))
fig4 = px.density_heatmap(grp_hh, x="Data", y="Horario", z="Ocupacao%", color_continuous_scale="RdYlGn", title="Heatmap — Ocupação por Data × Horário", nbinsx=len(grp_hh["Data"].unique()))
fig4.update_coloraxes(colorbar_title="Ocupação %", cmin=0, cmax=100)
st.plotly_chart(fig4, width="stretch")

st.divider()

st.subheader("Calendário (mensal)")
daily = _daily_agg(df)
if daily.empty:
    st.info("Sem dados para montar o calendário no período selecionado.")
else:
    hoje = date.today()
    limite = hoje + timedelta(days=30)

    daily_fut = daily[(daily["Data"] >= hoje) & (daily["Data"] <= limite)].copy()
    if daily_fut.empty:
        daily_fut = daily.copy()

    min_m = daily_fut["Data"].min().replace(day=1)
    max_m = daily_fut["Data"].max().replace(day=1)
    months_list = []
    cur = min_m
    while cur <= max_m:
        months_list.append(cur)
        y, m = cur.year, cur.month
        cur = date(y + 1, 1, 1) if m == 12 else date(y, m + 1, 1)

    month_labels = [f"{pycal.month_name[m.month]} {m.year}" for m in months_list]
    idx_default = 0
    sel = st.selectbox(
        "Selecione o mês",
        options=list(range(len(months_list))),
        format_func=lambda i: month_labels[i],
        index=idx_default,
    )

    st.caption("Cada quadradinho mostra o número de vagas sobrando em cada dia (próximos 30 dias).")

    sel_month = months_list[sel]
    dmin = sel_month
    dmax = sel_month.replace(day=pycal.monthrange(sel_month.year, sel_month.month)[1])
    daily_month = daily_fut[(daily_fut["Data"] >= dmin) & (daily_fut["Data"] <= dmax)].copy()

    fig_cal = make_calendar_figure(
        daily_month,
        sel_month.year,
        sel_month.month,
        color_metric="VagasSobrando",
        show_values_in_cell=True,
    )
    st.plotly_chart(fig_cal, width="stretch")

st.divider()

df_break = df.copy()
df_break["DataDT"] = pd.to_datetime(df_break["Data"])
df_break["TipoDia"] = df_break["DataDT"].dt.dayofweek.apply(lambda x: "Semana" if x < 5 else "Fim de semana")

grp_break = df_break.groupby(["TipoDia", "Atividade"], as_index=False).agg(Slots=("Horario", "count"))
grp_break["TotalSlotsTipoDia"] = grp_break.groupby("TipoDia")["Slots"].transform("sum")
grp_break["PctSlots"] = (grp_break["Slots"] / grp_break["TotalSlotsTipoDia"] * 100).round(1)

st.subheader("Dados filtrados (detalhado)")
st.dataframe(df.sort_values(["Data", "Horario", "Atividade"]).reset_index(drop=True), use_container_width=True, height=420)

col_a, col_b, col_c, col_d = st.columns(4)

with col_a:
    _download_button_csv(
        df.sort_values(["Data", "Horario", "Atividade"]),
        "⬇️ Baixar dados filtrados (CSV)",
        "dados_filtrados.csv",
    )

with col_b:
    _download_button_csv(
        grp_day.sort_values("Data"),
        "⬇️ Baixar ocupação por dia (CSV)",
        "ocupacao_por_dia.csv",
    )

with col_c:
    selected_cols = [
        "Pista", "Data", "Início", "Fim", "Atividade",
        "Capacidade", "Bookados", "Disponíveis",
        "Professor",
        "Aluno 1", "Aluno 1 - Nível Ski", "Aluno 1 - Nível Snow",
        "Aluno 2", "Aluno 2 - Nível Ski", "Aluno 2 - Nível Snow",
        "Aluno 3", "Aluno 3 - Nível Ski", "Aluno 3 - Nível Snow",
    ]

    sort_keys = [c for c in ["Data", "Horario", "Atividade"] if c in df.columns]
    df_sorted = df.sort_values(sort_keys) if sort_keys else df.copy()
    cols_existentes = [c for c in selected_cols if c in df_sorted.columns]
    df_excel = df_sorted[cols_existentes]

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        df_excel.to_excel(writer, index=False, sheet_name="Aulas")
        worksheet = writer.sheets["Aulas"]
        for i, col in enumerate(df_excel.columns):
            max_len = max(df_excel[col].astype(str).map(len).max(), len(col)) + 2
            worksheet.set_column(i, i, min(max_len, 40))

    st.download_button(
        label="⬇️ Baixar Grade (XLSX)",
        data=buffer.getvalue(),
        file_name="Grade.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

with col_d:
    buffer_break = io.BytesIO()
    with pd.ExcelWriter(buffer_break, engine="xlsxwriter") as writer:
        df_semana = grp_break[grp_break["TipoDia"] == "Semana"].copy()
        if not df_semana.empty:
            df_semana = df_semana[["Atividade", "Slots", "TotalSlotsTipoDia", "PctSlots"]]
            df_semana = df_semana.sort_values("PctSlots", ascending=False)
            df_semana.rename(
                columns={
                    "Slots": "Slots (aulas)",
                    "TotalSlotsTipoDia": "Total slots (dias de semana)",
                    "PctSlots": "% dos slots (semana)",
                },
                inplace=True,
            )
            df_semana.to_excel(writer, index=False, sheet_name="Semana")
            ws = writer.sheets["Semana"]
            for i, col in enumerate(df_semana.columns):
                max_len = max(df_semana[col].astype(str).map(len).max(), len(col)) + 2
                ws.set_column(i, i, min(max_len, 40))

        df_fim = grp_break[grp_break["TipoDia"] == "Fim de semana"].copy()
        if not df_fim.empty:
            df_fim = df_fim[["Atividade", "Slots", "TotalSlotsTipoDia", "PctSlots"]]
            df_fim = df_fim.sort_values("PctSlots", ascending=False)
            df_fim.rename(
                columns={
                    "Slots": "Slots (aulas)",
                    "TotalSlotsTipoDia": "Total slots (finais de semana)",
                    "PctSlots": "% dos slots (fim de semana)",
                },
                inplace=True,
            )
            df_fim.to_excel(writer, index=False, sheet_name="FimSemana")
            ws = writer.sheets["FimSemana"]
            for i, col in enumerate(df_fim.columns):
                max_len = max(df_fim[col].astype(str).map(len).max(), len(col)) + 2
                ws.set_column(i, i, min(max_len, 40))

    buffer_break.seek(0)

    st.download_button(
        label="⬇️ Breakdown modalidades (XLSX)",
        data=buffer_break.getvalue(),
        file_name="breakdown_modalidades_semana_fimsemana.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

st.divider()
st.subheader("📋 Escala de Professores")

cols_prof = ["Data", "Início", "Fim", "Pista", "Atividade", "Professor"]
cols_existentes = [c for c in cols_prof if c in df.columns]
df_prof_escala = df[cols_existentes].sort_values(["Data", "Início", "Atividade"])

buffer_prof = io.BytesIO()
with pd.ExcelWriter(buffer_prof, engine="xlsxwriter") as writer:
    df_prof_escala.to_excel(writer, index=False, sheet_name="Escala")
    worksheet = writer.sheets["Escala"]
    for i, col in enumerate(df_prof_escala.columns):
        max_len = max(df_prof_escala[col].astype(str).map(len).max(), len(col)) + 2
        worksheet.set_column(i, i, min(max_len, 40))
buffer_prof.seek(0)

st.download_button(
    label="⬇️ Baixar Escala de Professores",
    data=buffer_prof.getvalue(),
    file_name="escala_professores.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)

st.caption("Feito com ❤️ em Streamlit + Plotly — coleta online via EVO")































































































