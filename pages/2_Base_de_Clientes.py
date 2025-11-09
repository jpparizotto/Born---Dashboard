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

# ──────────────────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Base de Clientes — Born to Ski", page_icon="👥", layout="wide")
st.title("👥 Base de Clientes — Born to Ski")

# v1 segue disponível se precisar, mas aqui usamos v2 para clientes
BASE_URL_V1 = "https://evo-integracao.w12app.com.br/api/v1"
BASE_URL_V2 = "https://evo-integracao-api.w12app.com.br/api/v2"
VERIFY_SSL = True

EVO_USER = st.secrets.get("EVO_USER", os.environ.get("EVO_USER", ""))
EVO_TOKEN = st.secrets.get("EVO_TOKEN", os.environ.get("EVO_TOKEN", ""))

if not EVO_USER or not EVO_TOKEN:
    st.error("Credenciais EVO ausentes. Configure EVO_USER e EVO_TOKEN em Secrets.")
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
    # 1) Lista 'addresses' (mais comum no v2)
    addr = c.get("addresses") or c.get("address") or []
    if isinstance(addr, dict):
        addr = [addr]
    cand = addr[0] if isinstance(addr, list) and addr else {}

    # 2) Fallback para campos chapados no root
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

    # Corrige possíveis “SÃ£o Paulo”
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

def _get_json_v2(path, params=None):
    """
    GET para a API v2 com Basic Auth.
    Retorna sempre uma LISTA (normaliza se vier 'data'/'items'/etc).
    """
    url = f"{BASE_URL_V2.rstrip('/')}/{path.lstrip('/')}"
    headers = {"Accept": "application/json", **_auth_header_basic()}
    r = requests.get(url, headers=headers, params=params or {}, verify=VERIFY_SSL, timeout=60)
    if r.status_code == 204:
        return []
    if r.status_code != 200:
        raise RuntimeError(f"GET {url} -> {r.status_code} | {r.text[:400]}")
    try:
        data = r.json()
    except Exception:
        return []
    if isinstance(data, dict):
        for k in ("data", "items", "results", "list", "rows"):
            if k in data and isinstance(data[k], list):
                return data[k]
    return data if isinstance(data, list) else []

def fetch_members_v2_all(take=100):
    """
    Pagina /members até acabar. Retorna a LISTA bruta (sem normalização).
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
        batch = _get_json_v2("members", params=params)
        if not batch:
            break
        all_rows.extend(batch)
        skip += take
    return all_rows

def _normalize_members_basic(raw_list):
    """
    Normaliza um subconjunto “amigável” de campos para análises rápidas,
    incluindo endereço completo.
    """
    out = []
    seen = set()
    for c in raw_list:
        cid = c.get("idMember") or c.get("memberId") or c.get("id") or c.get("Id")
        if cid in seen:
            continue
        seen.add(cid)

        # Nome
        nome = (c.get("fullName") or c.get("name") or "").strip()
        if not nome:
            fn = (c.get("firstName") or "").strip()
            ln = (c.get("lastName") or "").strip()
            nome = (fn + " " + ln).strip()

        # Sexo
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

        # Nascimento / Idade
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

        # Contatos
        email = c.get("email") or ""
        tel = c.get("phone") or c.get("mobile") or c.get("cellphone") or ""
        for ct in (c.get("contacts") or []):
            t = str(ct.get("type") or "").upper()
            v = str(ct.get("value") or ct.get("description") or "").strip()
            if not v:
                continue
            if not email and t in ("EMAIL", "E-MAIL", "MAIL"):
                email = v
            if not tel and t in ("MOBILE", "CELULAR", "CELLPHONE", "PHONE", "TELEFONE"):
                tel = v

        # Endereço (primeiro endereço disponível)
        street, number, compl, bairro, cidade, uf, cep = _extract_address_any(c)

        # Data de criação
        criado = c.get("createdAt") or c.get("creationDate") or ""
        if criado:
            try:
                criado = parse_date(str(criado)).date().isoformat()
            except Exception:
                pass

        out.append({
            "IdCliente": str(cid) if cid is not None else "",
            "Nome": nome,
            "Sexo": sexo_fmt,
            "Nascimento": nascimento,
            "Idade": idade,

            # Endereço detalhado
            "Rua": street,
            "Numero": number,
            "Complemento": compl,
            "Bairro": bairro,
            "Cidade": cidade,
            "UF": uf,
            "CEP": cep,

            # Linha única (útil para conferência)
            "EnderecoLinha": " | ".join([x for x in [street, number, compl, bairro, cidade, uf, cep] if x]),

            # Contatos
            "Email": email,
            "Telefone": tel,

            "CriadoEm": criado,
        })

    return pd.DataFrame(out)

def _excel_bytes(df, sheet_name="Sheet1"):
    """
    Converte DataFrame em bytes XLSX com fallback de engine.
    """
    buf = io.BytesIO()
    try:
        with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name=sheet_name)
            # Ajuste básico das primeiras colunas
            ws = writer.sheets[sheet_name]
            for i, col in enumerate(df.columns[:50]):
                ws.set_column(i, i, min(max(len(str(col)) + 2, 16), 40))
    except Exception:
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name=sheet_name)
    return buf.getvalue()

# ──────────────────────────────────────────────────────────────────────────────
# UI — Coleta
# ──────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Coleta de Clientes (v2)")
    bring_all = st.checkbox("Trazer todos (sem limite)", value=True)
    take = st.slider("Tamanho de página (take)", 50, 100, 100, 10, help="Só usado se 'Trazer todos' estiver desmarcado")
    max_pages = st.slider("Máx. páginas", 1, 100, 10, 1, help="Só usado se 'Trazer todos' estiver desmarcado")

    if st.button("🔄 Atualizar agora", type="primary"):
        st.session_state.pop("_clientes_raw", None)
        st.session_state.pop("_clientes_df", None)
        st.session_state.pop("_clientes_full_df", None)

# Coleta
if "_clientes_raw" not in st.session_state:
    with st.spinner("Coletando clientes do EVO (v2/members)…"):
        if bring_all:
            raw = fetch_members_v2_all(take=100)
        else:
            # coleta limitada (útil para testes)
            rows = []
            skip = 0
            for _ in range(max_pages):
                batch = _get_json_v2("members", params={
                    "take": take,
                    "skip": skip,
                    "showMemberships": "true",
                    "includeAddress": "true",
                    "includeContacts": "true",
                })
                if not batch:
                    break
                rows.extend(batch)
                skip += take
            raw = rows
        st.session_state["_clientes_raw"] = raw

raw = st.session_state.get("_clientes_raw", [])
st.success(f"Clientes carregados: {len(raw)}")

# Normalização “amigável” para análises rápidas
if "_clientes_df" not in st.session_state:
    st.session_state["_clientes_df"] = _normalize_members_basic(raw)

dfc = st.session_state["_clientes_df"].copy()

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

# Monta e cacheia o dataframe BRUTO (todas as chaves) via json_normalize
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
# KPIs + Filtros + Gráficos (amigável)
# ──────────────────────────────────────────────────────────────────────────────
st.divider()

k1, k2, k3, k4 = st.columns(4)
with k1:
    st.metric("Total de clientes", f"{len(dfc):,}".replace(",", "."))
with k2:
    if "Idade" in dfc.columns and dfc["Idade"].notna().any():
        st.metric("Idade média", f"{np.nanmean(dfc['Idade']):.1f}")
    else:
        st.metric("Idade média", "—")
with k3:
    st.metric("Com email", f"{int((dfc.get('Email', pd.Series([], dtype=str)).astype(bool)).sum())}")
with k4:
    st.metric("Com telefone", f"{int((dfc.get('Telefone', pd.Series([], dtype=str)).astype(bool)).sum())}")

colf1, colf2 = st.columns(2)
with colf1:
    sexos = sorted([x for x in dfc.get("Sexo", pd.Series(dtype=str)).dropna().unique()])
    sel_sexo = st.multiselect("Sexo", sexos, default=sexos)
with colf2:
    cidades = sorted([x for x in dfc.get("Cidade", pd.Series(dtype=str)).dropna().unique()])
    sel_cid = st.multiselect("Cidade", cidades, default=cidades)

mask = pd.Series(True, index=dfc.index)
if sel_sexo:
    mask &= dfc.get("Sexo", pd.Series(index=dfc.index)).isin(sel_sexo)
if sel_cid:
    mask &= dfc.get("Cidade", pd.Series(index=dfc.index)).isin(sel_cid)
dfv = dfc[mask].copy()

st.caption(f"Filtrados: {len(dfv)}")

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
            dfv.groupby("Bairro", as_index=False).size().rename(columns={"size": "Clientes"}).sort_values("Clientes", ascending=False).head(20)
        )
        fig = px.bar(top_bairro, x="Bairro", y="Clientes", title="Top bairros (20)")
        fig.update_layout(xaxis_tickangle=-35)
        st.plotly_chart(fig, use_container_width=True)

if "Cidade" in dfv.columns and not dfv.empty:
    with cols2[1]:
        top_cid = (
            dfv.groupby("Cidade", as_index=False).size().rename(columns={"size": "Clientes"}).sort_values("Clientes", ascending=False).head(20)
        )
        fig = px.bar(top_cid, x="Cidade", y="Clientes", title="Top cidades (20)")
        fig.update_layout(xaxis_tickangle=-35)
        st.plotly_chart(fig, use_container_width=True)

st.divider()
st.subheader("Dados (filtrados)")
st.dataframe(dfv.reset_index(drop=True), use_container_width=True, height=420)
