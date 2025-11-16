# -*- coding: utf-8 -*-
# pages/2_Base_de_Clientes.py

import os
from datetime import date, datetime, timedelta
from dateutil.parser import parse as parse_date

import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
import requests
import io
import base64
from time import sleep
import re
from datetime import datetime  # você já tem date, mas agora precisa de datetime também
from db import init_db, get_conn
import re

from db import (
    init_db,
    upsert_client,
    add_level_snapshot,
    upsert_session,
    get_client_by_evo,
    LEVEL_ORDER,
)
from db import sync_clients_from_df

# ──────────────────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Base de Clientes — Born to Ski", page_icon="👥", layout="wide")
st.title("👥 Base de Clientes — Born to Ski")

BASE_URL_V1 = "https://evo-integracao.w12app.com.br/api/v1"
BASE_URL_V2 = "https://evo-integracao-api.w12app.com.br/api/v2"
VERIFY_SSL = True

EVO_USER = st.secrets.get("EVO_USER", os.environ.get("EVO_USER", ""))
EVO_TOKEN = st.secrets.get("EVO_TOKEN", os.environ.get("EVO_TOKEN", ""))

if not EVO_USER or not EVO_TOKEN:
    st.error("Credenciais EVO auscentes. Configure EVO_USER e EVO_TOKEN em Secrets.")
    st.stop()

# Inicializa banco interno
init_db()

# ──────────────────────────────────────────────────────────────────────────────
# HELPERS API
# ──────────────────────────────────────────────────────────────────────────────

def _fix_mojibake(s: str) -> str:
    """Corrige 'SÃ£o Paulo' → 'São Paulo' quando vier com encoding errado."""
    if not isinstance(s, str) or not s:
        return s
    if "Ã" in s or "Õ" in s or "Â" in s:
        try:
            return s.encode("latin1").decode("utf-8")
        except Exception:
            return s
    return s


def _extract_address_any(c: dict):
    """
    Extrai endereço em vários formatos comuns do EVO.
    Retorna (street, number, complement, neighborhood, city, state, zip).
    """
    addr = c.get("addresses") or c.get("address") or []
    if isinstance(addr, dict):
        addr = [addr]

    cand = {}
    if isinstance(addr, list) and addr:
        main_list = [
            a for a in addr
            if isinstance(a, dict) and str(a.get("isMain", "")).lower() in ("true", "1")
        ]
        cand = (main_list[0] if main_list else addr[0]) or {}
    else:
        cand = {}

    flat = {
        "street": c.get("street") or c.get("streetName") or c.get("publicPlace") or c.get("logradouro"),
        "number": c.get("number") or c.get("streetNumber") or c.get("numero"),
        "complement": c.get("complement") or c.get("complemento"),
        "neighborhood": c.get("neighborhood") or c.get("bairro"),
        "city": c.get("city") or c.get("cidade"),
        "state": c.get("state") or c.get("uf") or c.get("stateCode") or c.get("stateInitials"),
        "zipCode": c.get("zipCode") or c.get("cep") or c.get("postalCode"),
    }

    def pick(d: dict, *keys):
        for k in keys:
            v = d.get(k)
            if v not in (None, "", []):
                return v
        return ""

    street = pick(cand, "street", "streetName", "publicPlace", "logradouro") or flat["street"] or ""
    number = pick(cand, "number", "streetNumber", "numero") or flat["number"] or ""
    compl  = pick(cand, "complement", "complemento") or flat["complement"] or ""
    neighb = pick(cand, "neighborhood", "bairro") or flat["neighborhood"] or ""
    city   = pick(cand, "city", "cidade") or flat["city"] or ""
    state  = pick(cand, "state", "uf", "stateCode", "stateInitials") or flat["state"] or ""
    zipc   = pick(cand, "zipCode", "cep", "postalCode") or flat["zipCode"] or ""

    street = _fix_mojibake(str(street).strip())
    number = str(number).strip()
    compl  = _fix_mojibake(str(compl).strip())
    neighb = _fix_mojibake(str(neighb).strip())
    city   = _fix_mojibake(str(city).strip())
    state  = _fix_mojibake(str(state).strip())
    zipc   = str(zipc).strip()

    return street, number, compl, neighb, city, state, zipc


def _auth_header_basic():
    auth_str = f"{EVO_USER}:{EVO_TOKEN}"
    b64 = base64.b64encode(auth_str.encode()).decode()
    return {"Authorization": f"Basic {b64}"}


def _get_json(url_base, path, params=None):
    """
    GET genérico com Basic Auth + backoff.
    """
    url = f"{url_base.rstrip('/')}/{path.lstrip('/')}"
    headers = {"Accept": "application/json", **_auth_header_basic()}
    params = params or {}
    backoff = 1.0

    for _ in range(6):
        try:
            r = requests.get(url, headers=headers, params=params, verify=VERIFY_SSL, timeout=60)
            if r.status_code in (200, 204):
                if r.status_code == 204:
                    return []
                try:
                    data = r.json()
                except Exception:
                    return []
                if isinstance(data, dict):
                    for k in ("data", "items", "results", "list", "rows"):
                        if k in data and isinstance(data[k], list):
                            return data[k]
                return data if isinstance(data, list) else []
            if r.status_code in (429, 500, 502, 503, 504):
                sleep(backoff)
                backoff = min(backoff * 2, 8)
                continue
            raise RuntimeError(f"GET {url} -> {r.status_code} | {r.text[:400]}")
        except requests.RequestException:
            sleep(backoff)
            backoff = min(backoff * 2, 8)

    raise RuntimeError(f"Falha ao acessar {url} após múltiplas tentativas.")


@st.cache_data(show_spinner=False, ttl=600)
def _cached_get_v2(path: str, params_tuple):
    params = dict(params_tuple)
    return _get_json(BASE_URL_V2, path, params=params)


def _get_json_v1(path, params=None):
    return _get_json(BASE_URL_V1, path, params=params)


def fetch_members_v2_all(take=100):
    """
    Pagina /members até acabar. Retorna a LISTA bruta (sem normalização).
    Usa cache por página.
    """
    take = min(max(1, int(take)), 100)
    skip = 0
    all_rows = []
    while True:
        params = {
            "take": take,
            "skip": skip,
            "showMemberships": "true",
            "includeAddress": "true",
            "includeContacts": "true",
        }
        batch = _cached_get_v2("members", tuple(params.items()))
        if not batch:
            break
        all_rows.extend(batch)
        skip += take
    return all_rows


def _excel_bytes(df, sheet_name="Sheet1"):
    buf = io.BytesIO()
    try:
        with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name=sheet_name)
            ws = writer.sheets[sheet_name]
            for i, col in enumerate(df.columns[:50]):
                ws.set_column(i, i, min(max(len(str(col)) + 2, 16), 40))
    except Exception:
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name=sheet_name)
    return buf.getvalue()

# ──────────────────────────────────────────────────────────────────────────────
# NOME + NÍVEL
# ──────────────────────────────────────────────────────────────────────────────

LEVEL_ORDER_MAP = LEVEL_ORDER  # reaproveita do db.py


def split_nome_e_nivel(nome: str):
    """
    Recebe algo como 'DANIEL BRUNS 1B' ou 'HENRIQUE 3A SB/2CSKI'
    e retorna (nome_limpo, nivel_atual, nivel_ordem).

    Regra:
    - procura todos os padrões [1-4][A-D]
    - escolhe o de MAIOR ordem (do pior pro melhor: 1A ... 4D)
    - remove todos esses códigos do texto do nome
    """
    if not nome:
        return "", None, None

    nome_str = str(nome).strip()
    matches = re.findall(r'([1-4][A-D])', nome_str.upper())
    if not matches:
        return nome_str, None, None

    best = max(matches, key=lambda x: LEVEL_ORDER_MAP.get(x, -1))
    nivel_atual = best
    nivel_ordem = LEVEL_ORDER_MAP.get(best)

    nome_limpo = nome_str
    for code in set(matches):
        nome_limpo = re.sub(r'\b' + re.escape(code) + r'\b', '', nome_limpo, flags=re.IGNORECASE)
    nome_limpo = re.sub(r'\s+', ' ', nome_limpo).strip()

    return nome_limpo, nivel_atual, nivel_ordem

# ──────────────────────────────────────────────────────────────────────────────
# NORMALIZAÇÃO DE CLIENTES
# ──────────────────────────────────────────────────────────────────────────────


def _normalize_members_basic(raw_list):
    def _clean_phone(v: str) -> str:
        if not v:
            return ""
        digits = re.sub(r"\D", "", str(v))
        if digits.startswith("55"):
            return "+" + digits
        if len(digits) in (10, 11):
            return "+55" + digits
        return "+" + digits if digits else ""

    def _clean_email(v: str) -> str:
        v = str(v or "").strip()
        return v if "@" in v and "." in v.split("@")[-1] else ""

    out = []
    seen = set()
    for c in raw_list:
        cid = c.get("idMember") or c.get("memberId") or c.get("id") or c.get("Id")
        if cid in seen:
            continue
        seen.add(cid)

        nome_bruto = (c.get("fullName") or c.get("name") or "").strip()
        if not nome_bruto:
            fn = (c.get("firstName") or "").strip()
            ln = (c.get("lastName") or "").strip()
            nome_bruto = (fn + " " + ln).strip()

        nome_limpo, nivel_atual, nivel_ordem = split_nome_e_nivel(nome_bruto)

        sx = c.get("gender") or c.get("sexo") or c.get("sex") or ""
        if isinstance(sx, dict):
            sx = sx.get("name") or sx.get("description") or ""
        sxn = str(sx).strip().lower()
        if sxn in ("m", "masc", "masculino", "male"):
            sexo_fmt = "Masculino"
        elif sxn in ("f", "fem", "feminino", "female"):
            sexo_fmt = "Feminino"
        elif sxn:
            sexo_fmt = sxn.capitalize()
        else:
            sexo_fmt = "Não informado"

        nascimento = None
        idade = None
        b = c.get("birthDate") or c.get("birthday") or c.get("dtBirth")
        if b:
            try:
                dtn = parse_date(str(b)).date()
                nascimento = dtn.isoformat()
                idade = int((date.today() - dtn).days // 365.25)
                if idade < 0 or idade > 120:
                    idade = None
            except Exception:
                pass

        email = _clean_email(c.get("email") or "")
        tel = _clean_phone(c.get("phone") or c.get("mobile") or c.get("cellphone") or "")

        for ct in (c.get("contacts") or []):
            t = str(ct.get("type") or "").upper()
            v = str(ct.get("value") or ct.get("description") or "").strip()
            if not v:
                continue
            if not email and t in ("EMAIL", "E-MAIL", "MAIL"):
                email = _clean_email(v)
            if not tel and t in ("MOBILE", "CELULAR", "CELLPHONE", "PHONE", "TELEFONE"):
                tel = _clean_phone(v)

        street, number, compl, bairro, cidade, uf, cep = _extract_address_any(c)

        criado = c.get("createdAt") or c.get("creationDate") or ""
        if criado:
            try:
                criado = parse_date(str(criado)).date().isoformat()
            except Exception:
                pass

        out.append({
            "IdCliente": str(cid) if cid is not None else "",
            "Nome": nome_bruto,
            "NomeLimpo": nome_limpo,
            "NivelAtual": nivel_atual,
            "NivelOrdem": nivel_ordem,
            "Sexo": sexo_fmt,
            "Nascimento": nascimento,
            "Idade": idade,
            "Rua": street,
            "Numero": number,
            "Complemento": compl,
            "Bairro": bairro,
            "Cidade": cidade,
            "UF": uf,
            "CEP": cep,
            "EnderecoLinha": " | ".join([x for x in [street, number, compl, bairro, cidade, uf, cep] if x]),
            "Email": email,
            "Telefone": tel,
            "CriadoEm": criado,
        })

    return pd.DataFrame(out)

# ──────────────────────────────────────────────────────────────────────────────
# HELPERS DE NÍVEL / DB
# ──────────────────────────────────────────────────────────────────────────────

def _extract_nome_e_nivel(nome: str):
    """
    Recebe algo como 'JOÃO PAULO PARIZOTTO 3C' e devolve:
      ( 'JOÃO PAULO PARIZOTTO', '3C', ordem )

    Se não achar nível, devolve (nome, None, None).
    """
    if not isinstance(nome, str) or not nome.strip():
        return nome, None, None

    txt = nome.strip()
    # procura todos os códigos tipo 1A,1B,...,4D
    matches = re.findall(r"\b([1-4][ABCD])\b", txt.upper())
    nivel = matches[-1] if matches else None

    nome_limpo = txt
    if nivel:
        # remove o nível do final do nome (ignorando maiúsc/minúsc)
        nome_limpo = re.sub(
            r"\s*\b" + re.escape(nivel) + r"\b\s*$",
            "",
            txt,
            flags=re.IGNORECASE
        ).strip()

    try:
        nivel_ordem = LEVELS.index(nivel) if nivel in LEVELS else None
    except Exception:
        nivel_ordem = None

    return nome_limpo, nivel, nivel_ordem


def _sync_clientes_para_db(dfc: pd.DataFrame):
    """
    Grava/atualiza clientes na tabela 'clients' e registra mudança de nível em 'level_history'.
    """
    init_db()
    conn = get_conn()
    now_iso = datetime.utcnow().isoformat()

    with conn:
        for _, r in dfc.iterrows():
            evo_id = str(r.get("IdCliente") or "").strip()
            if not evo_id:
                continue

            nome_bruto = r.get("Nome") or ""
            nome_limpo, nivel, nivel_ordem = _extract_nome_e_nivel(nome_bruto)

            sexo = r.get("Sexo")
            cidade = r.get("Cidade")
            bairro = r.get("Bairro")
            uf = r.get("UF")
            email = r.get("Email")
            telefone = r.get("Telefone")
            criado_em = r.get("CriadoEm")

            # nível anterior antes de atualizar
            cur = conn.execute(
                "SELECT nivel_atual FROM clients WHERE evo_id = ?",
                (evo_id,),
            )
            row_prev = cur.fetchone()
            nivel_anterior = row_prev[0] if row_prev else None

            # upsert em clients
            conn.execute(
                """
                INSERT INTO clients (
                    evo_id,
                    nome_bruto,
                    nome_limpo,
                    nivel_atual,
                    nivel_ordem,
                    sexo,
                    cidade,
                    bairro,
                    uf,
                    email,
                    telefone,
                    criado_em,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(evo_id) DO UPDATE SET
                    nome_bruto   = excluded.nome_bruto,
                    nome_limpo   = excluded.nome_limpo,
                    nivel_atual  = excluded.nivel_atual,
                    nivel_ordem  = excluded.nivel_ordem,
                    sexo         = excluded.sexo,
                    cidade       = excluded.cidade,
                    bairro       = excluded.bairro,
                    uf           = excluded.uf,
                    email        = excluded.email,
                    telefone     = excluded.telefone,
                    criado_em    = COALESCE(clients.criado_em, excluded.criado_em),
                    updated_at   = excluded.updated_at
                """,
                (
                    evo_id,
                    nome_bruto,
                    nome_limpo,
                    nivel,
                    nivel_ordem,
                    sexo,
                    cidade,
                    bairro,
                    uf,
                    email,
                    telefone,
                    criado_em,
                    now_iso,
                ),
            )

            # se o nível mudou e temos um nível válido, grava no histórico
            if nivel and nivel != nivel_anterior:
                data_evento = date.today().isoformat()  # por enquanto usa hoje como data da mudança
                conn.execute(
                    """
                    INSERT OR IGNORE INTO level_history (
                        evo_id,
                        data_evento,
                        nivel,
                        nivel_ordem,
                        origem,
                        created_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        evo_id,
                        data_evento,
                        nivel,
                        nivel_ordem,
                        "sync_base_clientes",
                        now_iso,
                    ),
                )

@st.cache_data(show_spinner=False, ttl=600)
def _normalize_members_basic_cached(raw_list):
    return _normalize_members_basic(raw_list)


def _invalidate_cache():
    _cached_get_v2.clear()
    _normalize_members_basic_cached.clear()
    st.session_state.pop("_clientes_raw", None)
    st.session_state.pop("_clientes_df", None)
    st.session_state.pop("_clientes_full_df", None)
    st.session_state.pop("__last_updated__", None)

# ──────────────────────────────────────────────────────────────────────────────
# SESSÕES (AULAS) – API v1
# ──────────────────────────────────────────────────────────────────────────────

def fetch_member_sessions_range(id_member: str, start: date, end: date, page_size: int = 200):
    """
    Busca sessões (aulas) do membro na API v1 /activities/schedule entre duas datas.

    Parâmetros confirmados pelo teste:
      - idMember
      - initialDate (YYYY-MM-DD)
      - finalDate   (YYYY-MM-DD)
      - pageNumber
      - pageSize

    OBS: o endpoint retorna os campos:
      idConfiguration, idActivity, idAtividadeSessao, name, area,
      startTime, endTime, activityDate, statusName, memberStatus, etc.
    """
    all_rows = []
    page = 1
    while True:
        params = {
            "idMember": id_member,
            "initialDate": start.isoformat(),
            "finalDate": end.isoformat(),
            "pageNumber": page,
            "pageSize": page_size,
        }
        batch = _get_json_v1("activities/schedule", params=params)
        if not batch:
            break
        all_rows.extend(batch)
        if len(batch) < page_size:
            break
        page += 1
    return all_rows



def normalize_session_for_db(row: dict) -> dict:
    """
    Converte o JSON da sessão do EVO em um dict padrão para member_sessions.

    Campos de entrada confirmados (activities/schedule):
      - idConfiguration
      - idActivity
      - idAtividadeSessao
      - name
      - area
      - startTime
      - endTime
      - activityDate (ex: '2025-11-15T00:00:00')
      - statusName (ex: 'RestrictEnded', 'Full')
      - memberStatus (pode ser null ou, quando tiver inscrição, algo como 'Booked', 'Present', etc.)

    No banco, vamos guardar:
      - activity_session_id
      - configuration_id
      - data (YYYY-MM-DD)
      - start_time, end_time
      - activity_name
      - area_name
      - status_activity
      - status_client
      - is_replacement (por enquanto sempre False, pois não veio no JSON)
    """
    raw_date = row.get("activityDate") or row.get("date")
    data = None
    if raw_date:
        # vem no formato '2025-11-15T00:00:00'
        data = str(raw_date).split("T")[0]

    return {
        "activity_session_id": row.get("idAtividadeSessao"),
        "configuration_id": row.get("idConfiguration"),
        "data": data,
        "start_time": row.get("startTime"),
        "end_time": row.get("endTime"),
        "activity_name": row.get("name") or row.get("description"),
        "area_name": row.get("area"),
        "status_activity": row.get("statusName"),
        "status_client": row.get("memberStatus"),
        "is_replacement": False,
        "origem": "evo_schedule",
    }



def sync_sessions_for_members(member_ids, days_past: int = 90, days_future: int = 30):
    """
    Para cada membro, busca aulas na janela [hoje - days_past, hoje + days_future]
    e grava/atualiza em member_sessions.
    """
    hoje = date.today()
    start = hoje - timedelta(days=days_past)
    end = hoje + timedelta(days=days_future)

    progress = st.progress(0.0)
    total = len(member_ids)
    for i, mid in enumerate(member_ids, start=1):
        try:
            raw_sess = fetch_member_sessions_range(str(mid), start, end)
            for s in raw_sess:
                norm = normalize_session_for_db(s)
                if not norm.get("data"):
                    continue
                upsert_session(str(mid), norm)
        except Exception as e:
            st.warning(f"Falha ao sincronizar aulas do cliente {mid}: {e}")
        progress.progress(i / total)

    st.success("Sincronização de aulas concluída.")

# ──────────────────────────────────────────────────────────────────────────────
# UI — Coleta de clientes
# ──────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Coleta de Clientes (v2)")
    bring_all = st.checkbox("Trazer todos (sem limite)", value=True)
    take = st.slider("Tamanho de página (take)", 50, 100, 100, 10, help="Só usado se 'Trazer todos' estiver desmarcado")
    max_pages = st.slider("Máx. páginas", 1, 100, 10, 1, help="Só usado se 'Trazer todos' estiver desmarcado")

    if st.button("🔄 Atualizar clientes agora", type="primary"):
        _invalidate_cache()

    st.markdown("---")
    st.subheader("Sessões (aulas)")
    days_past = st.number_input("Dias passados para sincronizar", min_value=1, max_value=365, value=90, step=1)
    days_future = st.number_input("Dias futuros para sincronizar", min_value=0, max_value=180, value=30, step=1)
    if st.button("▶️ Sincronizar aulas de todos os clientes filtrados"):
        # só vamos ter dfc depois, então guardo isso num flag
        st.session_state["__sync_sessions_request__"] = (int(days_past), int(days_future))

# Coleta de clientes do EVO
if "_clientes_raw" not in st.session_state:
    with st.spinner("Coletando clientes do EVO (v2/members)…"):
        if bring_all:
            raw = fetch_members_v2_all(take=100)
        else:
            rows = []
            skip = 0
            for _ in range(max_pages):
                params = {
                    "take": take,
                    "skip": skip,
                    "showMemberships": "true",
                    "includeAddress": "true",
                    "includeContacts": "true",
                }
                batch = _cached_get_v2("members", tuple(params.items()))
                if not batch:
                    break
                rows.extend(batch)
                skip += take
            raw = rows
        st.session_state["_clientes_raw"] = raw
        st.session_state["__last_updated__"] = datetime.now().strftime("%d/%m/%Y %H:%M")

raw = st.session_state.get("_clientes_raw", [])
st.success(f"Clientes carregados: {len(raw)}")

# Normalização amigável
if "_clientes_df" not in st.session_state:
    st.session_state["_clientes_df"] = _normalize_members_basic_cached(raw)

dfc = st.session_state["_clientes_df"].copy()
# grava/atualiza clientes + histórico de nível no banco
with st.expander("Sincronização com banco interno (CRM)", expanded=True):
    if st.button("💾 Sincronizar clientes com banco interno"):
        with st.spinner("Sincronizando clientes com o banco interno..."):
            try:
                n = sync_clients_from_df(dfc)
            except Exception as e:
                st.error("Não foi possível sincronizar com o banco interno.")
                st.exception(e)
            else:
                st.success(f"Sincronização concluída para {n} clientes.")
                st.caption("Agora você já pode ir na aba 'Evolução de Nível'.")
# ──────────────────────────────────────────────────────────────────────────────
# Sincroniza clientes + histórico de nível com o banco
# ──────────────────────────────────────────────────────────────────────────────
sync_key = f"__db_synced_for_batch__{len(dfc)}"
if not st.session_state.get(sync_key):
    with st.spinner("Sincronizando clientes com o banco interno..."):
        for _, row in dfc.iterrows():
            evo_id = row.get("IdCliente")
            novo_nivel = row.get("NivelAtual")
            # pega nível anterior antes de atualizar
            cli = get_client_by_evo(str(evo_id)) if evo_id else None
            old_level = cli["nivel_atual"] if cli else None

            upsert_client(row)

            # data_evento = None -> db.py tenta usar última aula com presença,
            # caso exista; senão, usa hoje.
            add_level_snapshot(
                evo_id=str(evo_id),
                novo_nivel=novo_nivel,
                data_evento=None,
                origem="sync_members",
                old_level=old_level,
            )
    st.session_state[sync_key] = True

# Se o user clicou para sincronizar aulas, faz isso agora que temos dfc
if "__sync_sessions_request__" in st.session_state:
    days_past, days_future = st.session_state.pop("__sync_sessions_request__")
    with st.spinner("Sincronizando aulas dos clientes filtrados..."):
        member_ids = dfc["IdCliente"].astype(str).tolist()
        sync_sessions_for_members(member_ids, days_past=days_past, days_future=days_future)

# ──────────────────────────────────────────────────────────────────────────────
# Exportações
# ──────────────────────────────────────────────────────────────────────────────
colA, colB = st.columns(2)
with colA:
    st.download_button(
        "⬇️ Baixar clientes (CSV — amigável)",
        dfc.to_csv(index=False, encoding="utf-8-sig"),
        "clientes_amigavel.csv",
        "text/csv",
    )
with colB:
    st.download_button(
        "⬇️ Baixar clientes (XLSX — amigável)",
        _excel_bytes(dfc, "Clientes"),
        "clientes_amigavel.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

st.divider()
st.subheader("📦 Exportar base completa (bruta)")

if st.button("Gerar CSV/XLSX bruto (todas as colunas)"):
    with st.spinner("Achatar JSON completo…"):
        df_full = pd.json_normalize(raw, sep="__")
        st.session_state["_clientes_full_df"] = df_full
        st.success(f"OK! Registros: {len(df_full)} • Colunas: {len(df_full.columns)}")

df_full = st.session_state.get("_clientes_full_df")
if df_full is not None and not df_full.empty:
    c1, c2 = st.columns(2)
    with c1:
        st.download_button(
            "⬇️ Baixar clientes (CSV — completo/bruto)",
            df_full.to_csv(index=False, encoding="utf-8-sig"),
            "clientes_full_bruto.csv",
            "text/csv",
        )
    with c2:
        st.download_button(
            "⬇️ Baixar clientes (XLSX — completo/bruto)",
            _excel_bytes(df_full, "ClientesFull"),
            "clientes_full_bruto.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    st.caption(f"Colunas exportadas: {len(df_full.columns)}")

# ──────────────────────────────────────────────────────────────────────────────
# KPIs + Filtros + Gráficos
# ──────────────────────────────────────────────────────────────────────────────
st.divider()

st.caption(f"Atualizado em: {st.session_state.get('__last_updated__', '—')}")

k1, k2, k3, k4 = st.columns(4)
with k1:
    st.metric("Total de clientes", f"{len(dfc):,}".replace(",", "."))
with k2:
    if "Idade" in dfc.columns and dfc["Idade"].notna().any():
        st.metric("Idade média", f"{np.nanmean(dfc['Idade']):.1f}")
    else:
        st.metric("Idade média", "—")
with k3:
    qtd_email = int(dfc.get("Email", pd.Series([], dtype=str)).fillna("").astype(bool).sum())
    st.metric("Com email", f"{qtd_email}")
with k4:
    qtd_tel = int(dfc.get("Telefone", pd.Series([], dtype=str)).fillna("").astype(bool).sum())
    st.metric("Com telefone", f"{qtd_tel}")

colf0a, colf0b = st.columns(2)
with colf0a:
    uf_series = dfc.get("UF", pd.Series(dtype=str)).dropna()
    sel_uf = st.multiselect("UF", sorted(uf_series.unique()), default=None)
with colf0b:
    termo = st.text_input("Buscar (nome, e-mail, telefone, ID)", "")

colf1, colf2 = st.columns(2)
with colf1:
    sexos = sorted([x for x in dfc.get("Sexo", pd.Series(dtype=str)).dropna().unique()])
    sel_sexo = st.multiselect("Sexo", sexos, default=sexos)
with colf2:
    cidades = sorted([x for x in dfc.get("Cidade", pd.Series(dtype=str)).dropna().unique()])
    sel_cid = st.multiselect("Cidade", cidades, default=cidades)

colf3, colf4 = st.columns(2)
with colf3:
    faixa_idade = st.slider("Faixa etária", 0, 90, (0, 90))
with colf4:
    filtrar_data = st.checkbox("Filtrar por data de criação?")
    dt_min = None
    if filtrar_data:
        dt_min = st.date_input("Criados a partir de", value=date.today())

mask = pd.Series(True, index=dfc.index)

if sel_sexo:
    mask &= dfc.get("Sexo", pd.Series(index=dfc.index)).isin(sel_sexo)
if sel_cid:
    mask &= dfc.get("Cidade", pd.Series(index=dfc.index)).isin(sel_cid)
if sel_uf:
    mask &= dfc.get("UF", pd.Series(index=dfc.index)).isin(sel_uf)

if "Idade" in dfc.columns and dfc["Idade"].notna().any():
    mask &= dfc["Idade"].fillna(-1).between(faixa_idade[0], faixa_idade[1], inclusive="both")

if "CriadoEm" in dfc.columns and dt_min:
    mask &= pd.to_datetime(dfc["CriadoEm"], errors="coerce").dt.date >= dt_min

if termo:
    termo_low = termo.lower()
    cols_busca = ["IdCliente", "Nome", "NomeLimpo", "Email", "Telefone"]
    presentes = [c for c in cols_busca if c in dfc.columns]
    if presentes:
        mask &= dfc[presentes].astype(str).apply(
            lambda s: s.str.lower().str.contains(termo_low, na=False)
        ).any(axis=1)

dfv = dfc[mask].copy()
st.caption(f"Filtrados: {len(dfv)}")

colE1, colE2 = st.columns(2)
with colE1:
    st.download_button(
        "⬇️ Baixar filtrado (CSV — amigável)",
        dfv.to_csv(index=False, encoding="utf-8-sig"),
        "clientes_filtrado_amigavel.csv",
        "text/csv",
    )
with colE2:
    st.download_button(
        "⬇️ Baixar filtrado (XLSX — amigável)",
        _excel_bytes(dfv, "ClientesFiltrados"),
        "clientes_filtrado_amigavel.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

gcols = st.columns(2)
if "Sexo" in dfv.columns and not dfv.empty:
    with gcols[0]:
        cont = dfv.groupby("Sexo", as_index=False).size().rename(columns={"size": "Clientes"})
        fig = px.bar(cont, x="Sexo", y="Clientes", title="Clientes por sexo")
        st.plotly_chart(fig, use_container_width=True)

if "Idade" in dfv.columns and dfv["Idade"].notna().any():
    with gcols[1]:
        fig = px.histogram(dfv.dropna(subset=["Idade"]), x="Idade", nbins=25, title="Distribuição de idades")
        st.plotly_chart(fig, use_container_width=True)

st.divider()
cols2 = st.columns(2)
if "Bairro" in dfv.columns and not dfv.empty:
    with cols2[0]:
        top_bairro = (
            dfv.groupby("Bairro", as_index=False).size()
            .rename(columns={"size": "Clientes"})
            .sort_values("Clientes", ascending=False)
            .head(20)
        )
        fig = px.bar(top_bairro, x="Bairro", y="Clientes", title="Top bairros (20)")
        fig.update_layout(xaxis_tickangle=-35)
        st.plotly_chart(fig, use_container_width=True)

if "Cidade" in dfv.columns and not dfv.empty:
    with cols2[1]:
        top_cid = (
            dfv.groupby("Cidade", as_index=False).size()
            .rename(columns={"size": "Clientes"})
            .sort_values("Clientes", ascending=False)
            .head(20)
        )
        fig = px.bar(top_cid, x="Cidade", y="Clientes", title="Top cidades (20)")
        fig.update_layout(xaxis_tickangle=-35)
        st.plotly_chart(fig, use_container_width=True)

st.divider()
st.subheader("Dados (filtrados)")
st.dataframe(dfv.reset_index(drop=True), use_container_width=True, height=420)
