# -*- coding: utf-8 -*-
# pages/2_Base_de_Clientes.py

import os
from datetime import date
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

from db import sync_clients_from_df, register_daily_client_count, load_daily_client_counts


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

@st.cache_data(show_spinner=False, ttl=300)
def fetch_member_activities_history(
    id_cliente: str,
    dias_passado: int = 30,
    dias_futuro: int = 15,
):
    """
    Busca TODAS as aulas em que o cliente (idMember) esteve inscrito
    (histórico + futuras) usando:
      - GET /api/v1/activities/schedule
      - GET /api/v1/activities/schedule/detail

    Retorna uma lista de dicts pronta para virar DataFrame/tabela.
    """
    from datetime import datetime, date, timedelta
    import requests

    if not id_cliente:
        return []

    try:
        id_member = int(str(id_cliente))
    except Exception:
        return []

    today = date.today()
    start_date = today - timedelta(days=int(dias_passado))
    end_date   = today + timedelta(days=int(dias_futuro))

    # helper interno: pega o JSON cru do v1 (sem achatar em lista)
    def _get_json_v1_raw(path: str, params=None):
        url = f"{BASE_URL_V1.rstrip('/')}/{path.lstrip('/')}"
        headers = {"Accept": "application/json", **_auth_header_basic()}
        try:
            r = requests.get(
                url,
                headers=headers,
                params=params or {},
                verify=VERIFY_SSL,
                timeout=60,
            )
            if r.status_code == 204:
                return {}
            r.raise_for_status()
            return r.json()
        except Exception:
            return {}

    # Vamos buscar a agenda em blocos semanais para não estourar requests
    current = start_date
    step = timedelta(days=7)

    schedule_slots = []
    seen_slots = set()

    while current <= end_date:
        params = {
            "date": current.isoformat(),
            "showFullWeek": True,
            "onlyAvailables": False,
            "take": 500,
        }

        try:
            data = _get_json_v1("activities/schedule", params=params)
        except Exception:
            current += step
            continue

        if isinstance(data, list):
            items = data
        elif isinstance(data, dict) and isinstance(data.get("data"), list):
            items = data["data"]
        else:
            items = []

        for it in items:
            activity_date_raw = it.get("activityDate")
            id_conf = it.get("idConfiguration")

            if not activity_date_raw or not id_conf:
                continue

            dt_str = str(activity_date_raw).split("T", 1)[0]
            try:
                dt_iso = datetime.fromisoformat(dt_str).date()
            except Exception:
                continue

            if dt_iso < start_date or dt_iso > end_date:
                continue

            slot_key = (int(id_conf), dt_iso.isoformat())
            if slot_key in seen_slots:
                continue
            seen_slots.add(slot_key)

            schedule_slots.append(
                {
                    "idConfiguration": int(id_conf),
                    "date": dt_iso,
                    "raw": it,
                }
            )

        current += step

    rows = []

    for slot in schedule_slots:
        id_conf = slot["idConfiguration"]
        dt_iso  = slot["date"]
        raw     = slot["raw"]

        detail_params = {
            "idConfiguration": id_conf,
            "activityDate": dt_iso.isoformat(),
        }

        detail = _get_json_v1_raw("activities/schedule/detail", params=detail_params)

        # se por algum motivo veio lista ou vazio, pula
        if not isinstance(detail, dict):
            continue

        enrollments = detail.get("enrollments") or []

        membro_inscrito = None
        for en in enrollments:
            try:
                if int(en.get("idMember") or 0) == id_member:
                    membro_inscrito = en
                    break
            except Exception:
                continue

        if not membro_inscrito:
            continue

        nome_atividade = (
            detail.get("name")
            or raw.get("name")
            or detail.get("title")
            or ""
        )
        area       = detail.get("area") or raw.get("area") or ""
        start_time = detail.get("startTime") or raw.get("startTime") or ""
        end_time   = detail.get("endTime") or raw.get("endTime") or ""
        status_aula  = detail.get("statusName") or str(detail.get("status") or "")
        status_aluno = membro_inscrito.get("status")

        if dt_iso < today:
            categoria = "Já realizada"
        elif dt_iso == today:
            categoria = "Hoje"
        else:
            categoria = "Futura"

        rows.append(
            {
                "Data": dt_iso.isoformat(),
                "Horário": f"{start_time} - {end_time}" if (start_time or end_time) else "",
                "Atividade": nome_atividade,
                "Área": area,
                "Status aula": status_aula,
                "Status aluno (código)": status_aluno,
                "Categoria": categoria,
            }
        )

    rows.sort(key=lambda r: (r["Data"], r["Horário"]))
    return rows

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

# Mapa local de ordem de nível (independente do db.py)
LEVEL_ORDER_MAP = {
    "1A": 10, "1B": 11, "1C": 12, "1D": 13,
    "2A": 20, "2B": 21, "2C": 22, "2D": 23,
    "3A": 30, "3B": 31, "3C": 32, "3D": 33,
    "4A": 40, "4B": 41, "4C": 42, "4D": 43,
}


def split_nome_e_nivel(nome: str):
    """
    Recebe algo como:
      'DANIEL BRUNS 1B'
      'HENRIQUE 3A SB/2CSKI'
      'MARIA 1BSK'
      'JOÃO 2CSB'

    e retorna (nome_limpo, nivel_atual, nivel_ordem).

    Regra:
    - Procura todos os padrões [1-4][A-D] (aceita "2 B", "1BSK", "2CSB"...)
    - Normaliza para "2B", "1B", "2C"
    - Se houver vários, escolhe o de maior ordem
    - Remove os códigos (com eventual SK/SB junto) do nome_limpo
    """

    if not nome:
        return "", None, None

    import re

    nome_str = str(nome).strip()
    upper = nome_str.upper()

    # encontra "2B", "2 B", "1BSK", "2CSB", "3A.", "4C+"
    pattern = r"([1-4]\s*[A-D])"
    matches = re.findall(pattern, upper)

    if not matches:
        return nome_str, None, None

    # normaliza "2 B" -> "2B"
    matches = [m.replace(" ", "") for m in matches]

    # filtra só níveis válidos
    matches = [m for m in matches if m in LEVEL_ORDER_MAP]
    if not matches:
        return nome_str, None, None

    # melhor nível
    best = max(matches, key=lambda x: LEVEL_ORDER_MAP.get(x, -1))
    nivel_atual = best
    nivel_ordem = LEVEL_ORDER_MAP.get(best)

    # remove todos os códigos do nome, inclusive casos colados com SK/SB
    nome_limpo = nome_str
    for code in set(matches):
        # remove "1B", "1BSK", "1BSKI", "1BSB", etc
        padrao_remocao = r'\b' + re.escape(code) + r'(?:SKI?|SBI?)?\b'
        nome_limpo = re.sub(padrao_remocao, '', nome_limpo, flags=re.IGNORECASE)

    # limpa espaços extras
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

        # Também lê telefones/e-mails da lista de contatos vinda da EVO
        for ct in (c.get("contacts") or []):
            # Em alguns exports o campo vem como "type", em outros como "contactType"
            raw_type = ct.get("type") or ct.get("contactType") or ""
            t = str(raw_type).upper()

            # Número / e-mail podem vir em "value" ou "description"
            v = ct.get("value") or ct.get("description") or ""
            v = str(v).strip()
            if not v:
                continue

            # DDI pode vir separado (ex: "55")
            ddi = str(ct.get("ddi") or "").strip()

            # E-mail
            if not email and t in ("EMAIL", "E-MAIL", "MAIL"):
                email = _clean_email(v)

            # Celular/telefone
            if not tel and t in ("MOBILE", "CELULAR", "CELLPHONE", "PHONE", "TELEFONE"):
                num = v
                # Se tiver DDI separado, junta antes de limpar
                if ddi and not num.startswith("+"):
                    num = ddi + num
                tel = _clean_phone(num)


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
# CACHES / INVALIDAÇÃO
# ──────────────────────────────────────────────────────────────────────────────

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
# UI — Coleta de clientes
# ──────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Coleta de Clientes (v2)")
    bring_all = st.checkbox("Trazer todos (sem limite)", value=True)
    take = st.slider("Tamanho de página (take)", 50, 100, 100, 10, help="Só usado se 'Trazer todos' estiver desmarcado")
    max_pages = st.slider("Máx. páginas", 1, 100, 10, 1, help="Só usado se 'Trazer todos' estiver desmarcado")

    if st.button("🔄 Atualizar clientes agora", type="primary"):
        _invalidate_cache()

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
        from datetime import datetime as _dt
        st.session_state["__last_updated__"] = _dt.now().strftime("%d/%m/%Y %H:%M")

raw = st.session_state.get("_clientes_raw", [])
st.success(f"Clientes carregados: {len(raw)}")

# Normalização amigável
if "_clientes_df" not in st.session_state:
    st.session_state["_clientes_df"] = _normalize_members_basic_cached(raw)

dfc = st.session_state["_clientes_df"].copy()
# Registra snapshot diário da quantidade de clientes
try:
    register_daily_client_count(len(dfc))
except Exception as e:
    st.warning("Não foi possível registrar o snapshot diário de clientes.")
    st.exception(e)
# ──────────────────────────────────────────────────────────────────────────────
# Sincroniza clientes + histórico de nível com o banco
# ──────────────────────────────────────────────────────────────────────────────
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
# ─────────────────────────────────────────────────────────────
# HISTÓRICO DIÁRIO DE CLIENTES
# ─────────────────────────────────────────────────────────────
st.divider()
st.subheader("📈 Evolução diária de clientes")

try:
    df_daily = load_daily_client_counts()
except Exception as e:
    st.error("Não foi possível carregar o histórico diário de clientes.")
    st.exception(e)
    df_daily = None

if df_daily is not None and not df_daily.empty:
    df_daily = df_daily.copy()
    df_daily["data"] = pd.to_datetime(df_daily["data"]).dt.date

    c_hist1, c_hist2 = st.columns(2)

    # 1) Número de clientes dia a dia (absoluto)
    with c_hist1:
        fig_total = px.line(
            df_daily,
            x="data",
            y="total_clientes",
            markers=True,
            title="Total de clientes por dia",
        )
        fig_total.update_layout(
            xaxis_title="Data",
            yaxis_title="Total de clientes",
        )
        st.plotly_chart(fig_total, use_container_width=True)

    # Garante que a coluna 'data' é data (sem horário)
    df_daily["data"] = pd.to_datetime(df_daily["data"]).dt.date
    
    # Define o corte
    data_corte = date(2025, 11, 30)
    
    df_filtrado = df_daily[df_daily["data"] >= data_corte]
    
    with c_hist2:
        fig_var = px.bar(
            df_filtrado,
            x="data",
            y="novos_clientes",
            title="Variação diária (novos clientes)",
            text="novos_clientes",        # 👈 manda plotly usar essa coluna como rótulo
        )
    
        # coloca o texto fora da barra (em cima)
        fig_var.update_traces(
            textposition="outside",
        )
    
        fig_var.update_layout(
            xaxis_title="Data",
            yaxis_title="Novos clientes no dia",
        )
    
        st.plotly_chart(fig_var, use_container_width=True)


    # Tabela abaixo (opcional)
    st.caption("Histórico bruto")
    st.dataframe(
        df_daily.rename(
            columns={
                "data": "Data",
                "total_clientes": "Total de clientes",
                "novos_clientes": "Novos no dia",
            }
        ),
        use_container_width=True,
    )
else:
    st.info("Ainda não há histórico diário de clientes registrado.")

# ─────────────────────────────────────────────────────────────
# DETALHE DE UM CLIENTE ESPECÍFICO (substitui os gráficos)
# ─────────────────────────────────────────────────────────────

selected_row = None
if not dfv.empty:
    nomes_opcoes = ["(Nenhum)"] + sorted(dfv["Nome"].dropna().unique().tolist())
    escolha_nome = st.selectbox(
        "🔍 Ver detalhes de um cliente específico (opcional)",
        nomes_opcoes,
        index=0,
    )

    if escolha_nome != "(Nenhum)":
        # pega a primeira ocorrência do nome escolhido
        selected_row = dfv[dfv["Nome"] == escolha_nome].iloc[0]

if selected_row is not None:
    st.subheader("👤 Detalhes do cliente")

    info_dict = {
        "Nome": selected_row.get("Nome", ""),
        "Idade": selected_row.get("Idade", ""),
        "Sexo": selected_row.get("Sexo", ""),
        "Bairro": selected_row.get("Bairro", ""),
        "Telefone": selected_row.get("Telefone", ""),
        "E-mail": selected_row.get("Email", ""),
        "ID Cliente (EVO)": selected_row.get("IdCliente", ""),
    }

    df_info = pd.DataFrame(
        {"Campo": list(info_dict.keys()), "Valor": list(info_dict.values())}
    )
    st.table(df_info)

    # ID do cliente que será usado na EVO
    id_cliente_evo = selected_row.get("IdCliente", "")

    st.subheader("📅 Aulas do cliente (histórico + futuras)")

    if id_cliente_evo:
        aulas_cliente = fetch_member_activities_history(
            id_cliente_evo,
            dias_passado=365,  # 1 ano pra trás
            dias_futuro=60,    # 60 dias pra frente
        )

        if aulas_cliente:
            df_aulas = pd.DataFrame(aulas_cliente)
            st.dataframe(
                df_aulas,
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("Nenhuma aula encontrada para este cliente no período selecionado.")
    else:
        st.info("Não foi possível identificar o ID deste cliente na EVO.")

    st.divider()
    st.subheader("Dados (filtrados)")
    st.dataframe(dfv.reset_index(drop=True), use_container_width=True, height=420)

    # IMPORTANTE: não desenha os gráficos gerais quando um cliente está selecionado
    st.stop()


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
        cont = (
            dfv.groupby("Sexo", as_index=False)
               .size()
               .rename(columns={"size": "Clientes"})
               .sort_values("Clientes", ascending=False)
        )

        fig = px.pie(
            cont,
            names="Sexo",
            values="Clientes",
            title="Clientes por sexo",
            hole=0,  # pizza tradicional
        )

        # MOSTRAR Nº BRUTO DENTRO DA FATIA
        fig.update_traces(
            textposition="inside",
            textinfo="label+value",  # mostra: Sexo + Número bruto
        )

        st.plotly_chart(fig, use_container_width=True)


if "Idade" in dfv.columns and dfv["Idade"].notna().any():
    with gcols[1]:
        fig = px.histogram(dfv.dropna(subset=["Idade"]), x="Idade", nbins=25, title="Distribuição de idades")
        st.plotly_chart(fig, use_container_width=True)

st.divider()
cols2 = st.columns(2)
# TABELA DE BAIRROS (substitui o gráfico)
if "Bairro" in dfv.columns and not dfv.empty:
    with cols2[0]:
        df_bairros = (
            dfv.groupby("Bairro", as_index=False)
               .size()
               .rename(columns={"size": "Clientes"})
               .sort_values("Clientes", ascending=False)
        )
        total_clientes_bairros = df_bairros["Clientes"].sum()
        df_bairros["% do total"] = (
            df_bairros["Clientes"] / total_clientes_bairros * 100
        ).round(1)
        df_bairros["% acumulado"] = df_bairros["% do total"].cumsum().round(1)

        st.subheader("Bairros (todos)")
        st.dataframe(df_bairros, use_container_width=True)

if "Cidade" in dfv.columns and not dfv.empty:
    with cols2[1]:
        df_cidades = (
            dfv.groupby("Cidade", as_index=False)
               .size()
               .rename(columns={"size": "Clientes"})
               .sort_values("Clientes", ascending=False)
        )
        total_clientes_cidades = df_cidades["Clientes"].sum()
        df_cidades["% do total"] = (
            df_cidades["Clientes"] / total_clientes_cidades * 100
        ).round(1)
        df_cidades["% acumulado"] = df_cidades["% do total"].cumsum().round(1)

        st.subheader("Cidades (todas)")
        st.dataframe(df_cidades, use_container_width=True)


st.divider()
st.subheader("Dados (filtrados)")
st.dataframe(dfv.reset_index(drop=True), use_container_width=True, height=420)
