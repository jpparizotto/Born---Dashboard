# -*- coding: utf-8 -*-
"""
Dashboard de Ocupação — Born to Ski (Streamlit Cloud-ready)
- Lê e também GERA CSVs de slots (rota EVO /activities/schedule)
- Botão "Atualizar agora" executa a coleta online (sem subprocess)
- Filtros: Data, Modalidade, Período, Horário
- Visuais Plotly + Calendário com números no quadrinho

UPDATE (níveis):
- Tenta puxar nível via EVO API v2 (member profile: /api/v2/members/{idMember})
- Se falhar ou vier vazio, usa fallback CSV (GitHub raw/local)
"""

import os
import io
import glob
import calendar as pycal
import re
from datetime import datetime, date, timedelta

import numpy as np
import pandas as pd
import requests
from dateutil.parser import parse as parse_date
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

def _debug_set(key: str, value):
    st.session_state.setdefault("_debug", {})
    st.session_state["_debug"][key] = value

def _debug_get(key: str, default=None):
    return st.session_state.get("_debug", {}).get(key, default)

def _debug_clear():
    st.session_state["_debug"] = {}

LEVELS_CACHE_PATH = "data/member_levels_cache.csv"
os.makedirs("data", exist_ok=True)
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
BASE_URL_V2 = "https://evo-integracao-api.w12app.com.br/api/v2"  # member profile v2

VERIFY_SSL = True
DAYS_AHEAD_DEFAULT = 21

# ──────────────────────────────────────────────────────────────────────────────
# FALLBACK CSV (GitHub/raw ou local)
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

def _read_levels_cache() -> dict[int, dict]:
    if not os.path.exists(LEVELS_CACHE_PATH):
        return {}
    try:
        dfc = pd.read_csv(LEVELS_CACHE_PATH, dtype={"idMember": "int64", "ski": "string", "snow": "string"})
        out = {}
        for _, r in dfc.iterrows():
            mid = int(r["idMember"])
            out[mid] = {"ski": (r.get("ski") or "") or "", "snow": (r.get("snow") or "") or ""}
        return out
    except Exception:
        return {}

def _write_levels_cache(cache: dict[int, dict]):
    rows = []
    for mid, lv in cache.items():
        rows.append({"idMember": int(mid), "ski": lv.get("ski", "") or "", "snow": lv.get("snow", "") or ""})
    dfc = pd.DataFrame(rows).drop_duplicates(subset=["idMember"], keep="last")
    dfc.to_csv(LEVELS_CACHE_PATH, index=False, encoding="utf-8-sig")

# ──────────────────────────────────────────────────────────────────────────────
# AUTH + HTTP
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

def _get_json(url, params=None, timeout=60):
    r = requests.get(
        url,
        headers=_auth_headers(),
        params=params or {},
        verify=VERIFY_SSL,
        timeout=timeout,
    )
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

# ──────────────────────────────────────────────────────────────────────────────
# NÍVEIS: API v2 + fallback CSV
# ──────────────────────────────────────────────────────────────────────────────
def _normalize_level_code(code: str) -> str:
    c = (code or "").strip().upper()
    if not c:
        return ""
    c = c.replace("SBB", "SB")
    c = re.sub(r"[^0-9A-Z]", "", c)
    return c

def _parse_levels_history(niveis_raw: str | None) -> dict:
    out = {"ski": "", "snow": ""}

    if niveis_raw is None or (isinstance(niveis_raw, float) and pd.isna(niveis_raw)):
        return out

    s = str(niveis_raw).strip()
    if not s:
        return out

    parts = [p.strip() for p in re.split(r"[,\|;/]+", s) if p.strip()]
    parts = [_normalize_level_code(p) for p in parts if p.strip()]

    for p in reversed(parts):
        if not out["ski"] and (p.endswith("SK") or p.endswith("KC") or p.startswith("KC")):
            out["ski"] = p
        if not out["snow"] and p.endswith("SB"):
            out["snow"] = p
        if out["ski"] and out["snow"]:
            break

    return out

@st.cache_data(show_spinner=False, ttl=6 * 3600)
def _load_levels_dict_from_csv() -> dict[int, dict]:
    """Carrega {idCliente: {ski, snow}} do CSV (GitHub raw ou arquivo local)."""
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
        raise RuntimeError(f"Falha ao ler CSV de níveis. Fonte={source} | Erro={last_err}")

    if LEVELS_CSV_URL:
        df_lv = _read_levels_csv(LEVELS_CSV_URL)
    elif os.path.exists(LEVELS_CSV_LOCAL):
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
        out[idc] = _parse_levels_history(row.get(col_niveis))

    return out

@st.cache_data(show_spinner=False, ttl=12 * 3600)
def _get_member_profile_v2(id_member: int) -> dict:
    """
    GET /api/v2/members/{idMember}
    Retorna memberLevel: [{ levelGroupName, currentLevelName }, ...]
    """
    url = f"{BASE_URL_V2}/members/{int(id_member)}"
    data = _get_json(url, timeout=30) or {}
    if isinstance(data, dict) and isinstance(data.get("data"), dict):
        return data["data"]
    return data if isinstance(data, dict) else {}

def _route_levels_to_ski_snow(member_profile: dict) -> dict:
    out = {"ski": "", "snow": ""}

    ml = member_profile.get("memberLevel")
    if not isinstance(ml, list):
        return out

    for it in ml:
        if not isinstance(it, dict):
            continue

        lvl = str(it.get("currentLevelName") or "").strip().upper()
        if not lvl:
            continue

        # ✅ regra simples: contém SB -> snow; contém SK -> ski (SKK entra aqui)
        if "SB" in lvl:
            out["snow"] = lvl
        if "SK" in lvl:
            out["ski"] = lvl

    return out
    
import time
import random

def _is_rate_limit_error(err: Exception) -> bool:
    s = str(err)
    return (" -> 429" in s) or ("request limit" in s.lower()) or ("too many requests" in s.lower())

def _api_fetch_levels_for_ids(
    member_ids: tuple[int, ...],
    per_minute_limit: int = 40,
    max_retries_429: int = 6,
    max_total_seconds: int = 600,  # tempo máximo que você aceita ficar esperando nessa rodada
) -> tuple[dict[int, dict], dict[int, str]]:
    """
    Tenta buscar níveis via API v2 para TODOS os IDs (API-first).
    Faz throttle ~40/min e retry com backoff quando dá 429.
    Retorna: (levels_dict_api, errors)
    """
    start_ts = time.time()

    # intervalo base por request pra respeitar 40/min (~1.5s)
    base_sleep = max(60.0 / float(per_minute_limit), 1.0)

    levels_api: dict[int, dict] = {}
    errors: dict[int, str] = {}

    for idx, mid in enumerate(member_ids):
        # corta por tempo total (pra não travar o app pra sempre)
        if (time.time() - start_ts) > max_total_seconds:
            errors[mid] = "timeout_total_seconds"
            continue

        # throttle entre requests
        if idx > 0:
            # jitter pequeno ajuda a não “bater” no minuto certinho
            time.sleep(base_sleep + random.uniform(0.05, 0.25))

        # tenta com retry específico para 429
        attempt = 0
        while True:
            try:
                prof = _get_member_profile_v2(int(mid))  # pode estar cacheado por st.cache_data
                lv = _route_levels_to_ski_snow(prof)

                # se API veio “sem level”, ainda conta como resposta válida
                levels_api[int(mid)] = {"ski": lv.get("ski", "") or "", "snow": lv.get("snow", "") or ""}
                break

            except Exception as e:
                if _is_rate_limit_error(e) and attempt < max_retries_429:
                    # backoff exponencial + jitter
                    wait = min(60, (2 ** attempt) * 3) + random.uniform(0.2, 0.8)
                    time.sleep(wait)
                    attempt += 1
                    continue

                errors[int(mid)] = str(e)
                levels_api[int(mid)] = {"ski": "", "snow": ""}
                break

    return levels_api, errors

def _merge_levels_api_with_csv(
    levels_api: dict[int, dict],
    member_ids: tuple[int, ...],
    levels_csv: dict[int, dict] | None = None,
) -> dict[int, dict]:
    """
    Regra: API manda. CSV só preenche se API falhou ou veio vazio.
    """
    if levels_csv is None:
        levels_csv = _load_levels_dict_from_csv()

    out: dict[int, dict] = {}

    for mid in member_ids:
        a = levels_api.get(mid) or {"ski": "", "snow": ""}
        c = levels_csv.get(mid) or {"ski": "", "snow": ""}

        ski = (a.get("ski") or "").strip()
        snow = (a.get("snow") or "").strip()

        # se API não trouxe, tenta CSV
        if not ski:
            ski = (c.get("ski") or "").strip()
        if not snow:
            snow = (c.get("snow") or "").strip()

        out[mid] = {"ski": ski, "snow": snow}

    return out

def _load_levels_dict_from_api(member_ids: tuple[int, ...]) -> tuple[dict[int, dict], dict[int, str]]:
    out: dict[int, dict] = {}
    errors: dict[int, str] = {}

    for mid in member_ids:
        try:
            prof = _get_member_profile_v2(int(mid))  # este sim fica cacheado por idMember
            out[int(mid)] = _route_levels_to_ski_snow(prof)
        except Exception as e:
            out[int(mid)] = {"ski": "", "snow": ""}
            errors[int(mid)] = str(e)

    return out, errors

def _load_levels_dict_from_api(
    member_ids: tuple[int, ...],
    max_fetch_per_run: int = 35,
) -> tuple[dict[int, dict], dict[int, str]]:
    """
    Carrega níveis via v2, mas:
    - usa cache persistente em disco (data/member_levels_cache.csv)
    - busca no máximo `max_fetch_per_run` IDs por execução (evita 429 de 40/min)
    Retorna (levels_dict, errors).
    """
    cache = _read_levels_cache()
    errors: dict[int, str] = {}

    # ids que ainda não estão no cache local
    missing = [int(mid) for mid in member_ids if int(mid) not in cache]

    # busca só um lote por rodada (pra não estourar 40/min)
    to_fetch = missing[:max_fetch_per_run]

    for mid in to_fetch:
        try:
            prof = _get_member_profile_v2(mid)  # este sim pode ficar cacheado 12h por id
            cache[mid] = _route_levels_to_ski_snow(prof)
        except Exception as e:
            errors[mid] = str(e)

    _write_levels_cache(cache)

    # opcional: salvar no debug quantos faltam
    try:
        remaining = len([int(mid) for mid in member_ids if int(mid) not in cache])
        _debug_set("levels_remaining", remaining)
    except Exception:
        pass

    return cache, errors

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

_DETAIL_CACHE: dict[tuple, dict] = {}
DEBUG_SCHEDULE_IDS = {15942757, 15848980}

def _dbg_print_schedule(schedule_id, date_val, hour_start, capacity, available, filled, expected, detail):
    if schedule_id not in DEBUG_SCHEDULE_IDS:
        return

    print("\n" + "=" * 90)
    print(f"[DEBUG] ScheduleId={schedule_id} | date={date_val} | start={hour_start} | cap={capacity} | avail={available} | filled={filled} | expected={expected}")

    enrollments = detail.get("enrollments") if isinstance(detail, dict) else None
    if not isinstance(enrollments, list):
        print("[DEBUG] detail.enrollments NÃO é lista. Keys do detail:", list(detail.keys())[:40] if isinstance(detail, dict) else type(detail))
        return

    def b(v):
        if isinstance(v, bool):
            return v
        if v is None:
            return False
        return str(v).strip().lower() in ("1", "true", "t", "yes", "y", "sim")

    def si(v):
        try:
            return int(v)
        except Exception:
            return None

    rows = []
    for i, e in enumerate(enrollments):
        if not isinstance(e, dict):
            continue
        nm = None
        for k in ["name", "fullName", "displayName", "customerName", "personName", "clientName", "description", "memberName", "nome"]:
            if e.get(k):
                nm = str(e.get(k)).strip()
                break
        rows.append({
            "i": i,
            "name": nm,
            "idMember": e.get("idMember") or e.get("memberId") or e.get("idClient") or e.get("clientId"),
            "slotNumber_raw": e.get("slotNumber"),
            "slotNumber_int": si(e.get("slotNumber")),
            "status": si(e.get("status")),
            "removed": b(e.get("removed")),
            "suspended": b(e.get("suspended")),
            "replacement": b(e.get("replacement")),
            "justifiedAbsence": b(e.get("justifiedAbsence")),
        })

    print("[DEBUG] ENROLLMENTS (raw):")
    for r in rows:
        print("  ", r)

    slot_ints = [r["slotNumber_int"] for r in rows if r["name"]]
    print("[DEBUG] slotNumber_int list:", slot_ints)

    from collections import Counter
    c = Counter([x for x in slot_ints if x is not None])
    dups = {k: v for k, v in c.items() if v > 1}
    print("[DEBUG] slotNumber duplicados (k:count):", dups)

    zeros = [r for r in rows if r["slotNumber_int"] == 0]
    if zeros:
        print("[DEBUG] ATENÇÃO: slotNumber==0 aparece em:", [z["name"] for z in zeros])

    nones = [r for r in rows if r["slotNumber_int"] is None]
    if nones:
        print("[DEBUG] slotNumber==None aparece em:", [n["name"] for n in nones])

    print("=" * 90)

def _get_schedule_detail(config_id: int | None, activity_date_iso: str | None, id_activity_session: int | None = None):
    if not config_id and not id_activity_session:
        return {}
    key = (config_id or 0, activity_date_iso or "", id_activity_session or 0)
    if key in _DETAIL_CACHE:
        return _DETAIL_CACHE[key]
    params = {}
    if config_id and activity_date_iso:
        params["idConfiguration"] = int(config_id)
        params["activityDate"] = activity_date_iso
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

    sd: date | None = None
    if slot_date:
        try:
            if isinstance(slot_date, date):
                sd = slot_date
            else:
                sd = date.fromisoformat(str(slot_date)[:10])
        except Exception:
            sd = None

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

        by_slot = {}
        extras = []

        def _score(r):
            return (
                1 if r.get("_replacement") else 0,
                1 if r.get("_status") == 0 else 0,
                0 if r.get("_justAbs") else 1,
            )

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
            status = _safe_int_local(e.get("status"))

            if removed or suspended:
                continue

            if not is_future_op:
                if status is not None and status != 0:
                    continue
                if justified_abs:
                    continue

            slot_num = _safe_int_local(e.get("slotNumber"))

            rec = {
                "name": nm,
                "idCliente": _id_cliente(e),
                "_slotNumber": slot_num,
                "_replacement": replacement,
                "_status": status,
                "_justAbs": justified_abs,
            }

            if slot_num is None or slot_num == 0:
                extras.append(rec)
                continue

            prev = by_slot.get(slot_num)
            if prev is None:
                by_slot[slot_num] = rec
            else:
                if _score(rec) > _score(prev):
                    by_slot[slot_num] = rec
                    extras.append(prev)
                else:
                    extras.append(rec)

        slotted = [by_slot[k] for k in sorted(by_slot.keys())]
        out = slotted + extras

        out.sort(
            key=lambda r: (
                -(1 if r.get("_replacement") else 0),
                -(1 if r.get("_status") == 0 else 0),
                (1 if r.get("_justAbs") else 0),
                (r.get("_slotNumber") is None, r.get("_slotNumber") or 9999),
                r.get("name") or "",
            )
        )

        for r in out:
            r.pop("_slotNumber", None)
            r.pop("_replacement", None)
            r.pop("_status", None)
            r.pop("_justAbs", None)

        return out

    # fallback antigo
    name_keys = ["name", "fullName", "displayName", "customerName", "personName", "clientName", "description"]
    id_keys = ["idMember", "idClient", "idCliente", "idCustomer", "clientId", "customerId", "memberId"]
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
            act_id_final = act_resolved["id"] if act_resolved else _first(h, "idActivity", "activityId", "id", "Id")
        else:
            act_name_final = "(Sem atividade)"
            act_id_final = _first(h, "idActivity", "activityId", "id", "Id")

        date_val = _first(h, "_requestedDate") or _normalize_date_only(
            _first(h, "activityDate", "date", "classDate", "day", "scheduleDate")
        )
        hour_start = _first(h, "startTime", "hourStart", "timeStart", "startHour")
        hour_end = _first(h, "endTime", "hourEnd", "timeEnd", "endHour")

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

        capacity = _safe_int(_first(h, "capacity", "spots", "vacanciesTotal", "maxStudents", "maxCapacity"))
        filled = _safe_int(_first(h, "ocupation", "spotsFilled", "occupied", "enrolled", "registrations"))
        available = _safe_int(_first(h, "available", "vacancies"))
        if available is None and capacity is not None and filled is not None:
            available = max(0, capacity - filled)

        filled_calc = ((capacity or 0) - (available or 0)) if filled is None else filled
        filled_calc = max(0, filled_calc)
        expected = min(filled_calc, (capacity or 0))

        if schedule_id in DEBUG_SCHEDULE_IDS:
            _dbg_print_schedule(schedule_id, date_val, hour_start, capacity, available, filled, expected, detail)

        alunos = _extract_alunos(detail, target_start=(hour_start or None), slot_date=date_val) or []

        if schedule_id in DEBUG_SCHEDULE_IDS:
            print("[DEBUG] alunos_extraidos (antes do corte):", [a.get("name") for a in alunos])

        alunos = alunos[:expected]

        if schedule_id in DEBUG_SCHEDULE_IDS:
            print("[DEBUG] alunos_final (depois do corte expected):", [a.get("name") for a in alunos])

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
                "Fim": hour_end,
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
                "Aluno 1 - Nível Ski": nivel_ski_cols[0],
                "Aluno 1 - Nível Snow": nivel_snow_cols[0],
                "Aluno 2 - Nível Ski": nivel_ski_cols[1],
                "Aluno 2 - Nível Snow": nivel_snow_cols[1],
                "Aluno 3 - Nível Ski": nivel_ski_cols[2],
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

def _collect_member_ids_from_agenda(agenda_items) -> set[int]:
    """
    Passa pelos slots, pega detail, extrai alunos e junta ids únicos (idMember/idClient etc).
    Reaproveita _DETAIL_CACHE.
    """
    ids: set[int] = set()

    for h in agenda_items:
        date_val = _first(h, "_requestedDate") or _normalize_date_only(
            _first(h, "activityDate", "date", "classDate", "day", "scheduleDate")
        )
        hour_start = _first(h, "startTime", "hourStart", "timeStart", "startHour")

        config_id = _first(h, "idConfiguration", "idActivitySchedule", "idGroupActivity", "idConfig", "configurationId")
        id_activity_session = _first(
            h,
            "idActivitySession", "idActivityScheduleClass", "idClassSchedule",
            "idScheduleClass", "idScheduleTime", "idTime", "idClass", "idSchedule"
        )

        detail = _get_schedule_detail(config_id, date_val, id_activity_session)
        alunos = _extract_alunos(detail, target_start=(hour_start or None), slot_date=date_val) or []

        for a in alunos:
            v = a.get("idCliente")
            if v:
                try:
                    ids.add(int(v))
                except Exception:
                    pass

    return ids
    
def _merge_levels(api_levels: dict[int, dict], csv_levels: dict[int, dict]) -> dict[int, dict]:
    out = dict(api_levels or {})
    for mid, lv in (csv_levels or {}).items():
        cur = out.get(mid) or {"ski": "", "snow": ""}
        if not cur.get("ski") and lv.get("ski"):
            cur["ski"] = lv["ski"]
        if not cur.get("snow") and lv.get("snow"):
            cur["snow"] = lv["snow"]
        out[mid] = cur
    return out
    
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

    member_ids = _collect_member_ids_from_agenda(agenda_all)
    member_ids_t = tuple(sorted(member_ids))
    
    _debug_set("member_ids_t", member_ids_t)
    
    # 1) tenta buscar tudo via API (pode demorar, mas fica “API-first”)
    levels_api, level_errors = _api_fetch_levels_for_ids(
        member_ids_t,
        per_minute_limit=40,
        max_retries_429=6,
        max_total_seconds=600,  # aumente se quiser insistir mais
    )
    
    # 2) merge: API manda, CSV só cobre buracos
    levels_dict = _merge_levels_api_with_csv(levels_api, member_ids_t)

    _debug_set("level_errors", level_errors)
    _debug_set("levels_sample", {mid: levels_dict.get(mid) for mid in list(member_ids_t)[:10]})
    
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
with st.expander("🛠️ Debug níveis (API v2)"):
    member_ids_t = _debug_get("member_ids_t", ())
    level_errors = _debug_get("level_errors", {})
    levels_sample = _debug_get("levels_sample", {})

    st.write("Qtd IDs únicos encontrados na agenda:", len(member_ids_t))
    st.write("Amostra de IDs:", list(member_ids_t[:10]) if member_ids_t else [])

    if level_errors:
        st.error(f"{len(level_errors)} chamadas falharam. Exibindo até 5:")
        for mid, err in list(level_errors.items())[:5]:
            st.code(f"idMember={mid} -> {err}")

    if levels_sample:
        st.write("Amostra do resultado (levels_dict):")
        st.json(levels_sample)

    test_id_default = int(member_ids_t[0]) if member_ids_t else 0
    test_id = st.number_input("Testar idMember", min_value=0, value=test_id_default, step=1)
    if st.button("Testar chamada v2 agora"):
        try:
            prof = _get_member_profile_v2(int(test_id))
            st.json({
                "idMember": prof.get("idMember"),
                "memberLevel": prof.get("memberLevel"),
            })
        except Exception as e:
            st.error(str(e))

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










































































































