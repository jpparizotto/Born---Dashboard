# -*- coding: utf-8 -*-
# pages/2_Base_de_Clientes.py
import os
from datetime import date, datetime
from dateutil.parser import parse as parse_date
import math

import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px

# ──────────────────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Base de Clientes — Born to Ski", page_icon="👥", layout="wide")
st.title("👥 Base de Clientes — Born to Ski")

BASE_URL = "https://evo-integracao.w12app.com.br/api/v1"
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
import requests

# === DEBUG/PROBE HELPERS ===
CANDIDATE_ENDPOINTS = [
    "customers", "customer", "people", "person", "members", "clients",
    # tente caminhos alternativos comuns em algumas instâncias EVO:
    "crm/customers", "crm/clients", "person/list", "customer/list"
]

def _param_variants(page, size):
    offset = (page - 1) * size
    return [
        ("take/skip", {"take": size, "skip": offset}),
        ("page/size", {"page": page, "size": size}),
        ("pageNumber/pageSize", {"pageNumber": page, "pageSize": size}),
        ("offset/limit", {"offset": offset, "limit": size}),
        ("start/count", {"start": offset, "count": size}),
        ("skip/take", {"skip": offset, "take": size}),
    ]

def _safe_get_json(ep, params, use_branch, id_branch):
    p = dict(params)
    if use_branch and id_branch:
        p["idBranch"] = id_branch
    try:
        data, hdrs = _get_json(ep, params=p, return_headers=True)
        if isinstance(data, dict) and "data" in data:
            data = data["data"]
        lst = data if isinstance(data, list) else []
        total_hdr = hdrs.get("total") or hdrs.get("Total") or hdrs.get("X-Total-Count")
        total = None
        try:
            total = int(total_hdr) if total_hdr else None
        except Exception:
            total = None
        return (True, lst, total, hdrs, None)
    except Exception as e:
        return (False, [], None, {}, str(e))
        
def _listar_id_branch():
    try:
        cfg = _get_json("configuration")
        if isinstance(cfg, list):
            for b in cfg:
                bid = b.get("idBranch") or b.get("branchId") or b.get("id")
                if bid:
                    return bid
    except Exception:
        pass
    return None
    
def probe_customers(endpoints=None, page_size=100, pages=2, try_branch=True, try_no_branch=True):
    """
    Testa várias combinações e devolve um relatório com:
    - endpoint
    - paginador
    - usar idBranch?
    - itens na 1ª página
    - header total
    - erro (se houve)
    - sample (primeiro item)
    """
    id_branch = _listar_id_branch()
    endpoints = endpoints or CANDIDATE_ENDPOINTS
    rows = []
    best = None

    for ep in endpoints:
        for paginator, params in _param_variants(1, page_size):
            for use_branch in ([True] if (try_branch and id_branch) else []) + ([False] if try_no_branch else []):
                ok, lst, total, hdrs, err = _safe_get_json(ep, params, use_branch, id_branch)
                rows.append({
                    "endpoint": ep,
                    "paginator": paginator,
                    "use_idBranch": use_branch,
                    "first_page_count": len(lst),
                    "total_header": total,
                    "error": err[:150] if err else "",
                    "sample_keys": list(lst[0].keys())[:10] if ok and lst else [],
                })
                # guarda a melhor combinação até aqui
                score = len(lst) if ok else 0
                if (best is None) or (score > best["score"]):
                    best = {"ep": ep, "paginator": paginator, "params": params, "use_branch": use_branch,
                            "sample": lst[:3], "score": score, "total": total}

    df_report = pd.DataFrame(rows).sort_values(["first_page_count", "total_header"], ascending=[False, False])
    return df_report, best
    
def _auth_headers():
    import base64
    auth_str = f"{EVO_USER}:{EVO_TOKEN}"
    b64 = base64.b64encode(auth_str.encode()).decode()
    return {
        "Authorization": f"Basic {b64}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

def _get_json(path, params=None, return_headers=False):
    url = f"{BASE_URL.rstrip('/')}/{path.lstrip('/')}"
    r = requests.get(url, headers=_auth_headers(), params=params or {}, verify=VERIFY_SSL, timeout=60)
    if r.status_code == 204:
        return ([], r.headers) if return_headers else []
    if r.status_code != 200:
        raise RuntimeError(f"GET {url} -> {r.status_code} | {r.text[:400]}")
    try:
        data = r.json()
    except Exception:
        data = []
    # normaliza {"data":[...]}
    if isinstance(data, dict) and "data" in data and isinstance(data["data"], (list, dict)):
        data = data["data"]
    return (data, r.headers) if return_headers else data

def _to_list(maybe):
    if isinstance(maybe, list):
        return maybe
    if isinstance(maybe, dict):
        # algumas instâncias retornam {"items":[...]} etc.
        for k in ("items", "results", "content", "list", "rows"):
            if k in maybe and isinstance(maybe[k], list):
                return maybe[k]
    return []
import requests

def _get_json_v2(path, params=None):
    url = f"{BASE_URL_V2.rstrip('/')}/{path.lstrip('/')}"
    # Basic Auth igual ao v1
    import base64
    auth_str = f"{EVO_USER}:{EVO_TOKEN}"
    b64 = base64.b64encode(auth_str.encode()).decode()
    headers = {"Authorization": f"Basic {b64}", "Accept": "application/json"}

    r = requests.get(url, headers=headers, params=params or {}, verify=VERIFY_SSL, timeout=60)
    if r.status_code == 204:
        return []
    if r.status_code != 200:
        raise RuntimeError(f"GET {url} -> {r.status_code} | {r.text[:400]}")
    try:
        data = r.json()
    except Exception:
        data = []
    # alguns tenants retornam lista direta; outros envelopam
    if isinstance(data, dict):
        for k in ("data", "items", "results", "list", "rows"):
            if k in data and isinstance(data[k], list):
                return data[k]
    return data if isinstance(data, list) else []

    
# ──────────────────────────────────────────────────────────────────────────────
# COLETA DE CLIENTES (paginada, tentando diferentes esquemas)
# ──────────────────────────────────────────────────────────────────────────────

st.sidebar.subheader("🔧 Modo debug")
dbg = st.sidebar.checkbox("Ativar diagnóstico de clientes", value=False)
dbg_size = st.sidebar.slider("Page size (debug)", 20, 200, 100, 10)

if dbg:
    with st.spinner("Testando endpoints e paginações..."):
        report, best = probe_customers(page_size=dbg_size)
    st.success("Diagnóstico concluído. Veja o relatório abaixo.")
    st.dataframe(report, use_container_width=True, height=360)
    if best:
        st.caption(f"↪️ Melhor combinação detectada: **{best['ep']}** com **{best['paginator']}** "
                   f"(use_idBranch={best['use_branch']}) — itens primeira página: {best['score']}, total_header={best['total']}")
        st.code(best["sample"], language="python")

def _fetch_customers(max_pages=None, page_size=100):
    """
    Busca clientes via v2 /members com paginação take/skip.
    Normaliza: IdCliente, Nome, Sexo, Nascimento, Idade, Bairro, Cidade, UF, CEP, Email, Telefone, CriadoEm.
    """
    take = min(max(1, int(page_size)), 100)   # v2 costuma aceitar até 100
    skip = 0
    page = 1
    all_rows, seen = [], set()

    while True:
        params = {"take": take, "skip": skip, "showMemberships": "true"}
        lst = _get_json_v2("members", params=params)
        if not lst:
            break

        for c in lst:
            cid = (c.get("idMember") or c.get("memberId") or c.get("id") or c.get("Id"))
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
            if sxn in ("m","masc","masculino","male"): sexo_fmt = "Masculino"
            elif sxn in ("f","fem","feminino","female"): sexo_fmt = "Feminino"
            elif sxn: sexo_fmt = sxn.capitalize()
            else: sexo_fmt = "Não informado"

            # Nascimento/Idade
            nascimento = None; idade = None
            b = c.get("birthDate") or c.get("birthday") or c.get("dtBirth")
            if b:
                try:
                    dtn = parse_date(str(b)).date()
                    nascimento = dtn.isoformat()
                    idade = int((date.today() - dtn).days // 365.25)
                    if idade < 0 or idade > 120: idade = None
                except Exception:
                    pass

            # Contatos
            email = c.get("email") or ""
            tel   = c.get("phone") or c.get("mobile") or c.get("cellphone") or ""
            # v2 geralmente tem contacts: [{type,value}]
            if not email or not tel:
                contacts = c.get("contacts") or []
                if isinstance(contacts, list):
                    for ct in contacts:
                        t = str(ct.get("type") or "").upper()
                        v = str(ct.get("value") or "").strip()
                        if not v: 
                            continue
                        if not email and t in ("EMAIL","E-MAIL","MAIL"):
                            email = v
                        if not tel and t in ("MOBILE","CELULAR","CELLPHONE","PHONE","TELEFONE"):
                            tel = v

            # Endereço
            bairro = cidade = uf = cep = ""
            addr = c.get("addresses") or c.get("address") or []
            if isinstance(addr, dict):
                addr = [addr]
            if isinstance(addr, list) and addr:
                a0 = addr[0]
                bairro = (a0.get("neighborhood") or a0.get("bairro") or "").strip()
                cidade = (a0.get("city") or a0.get("cidade") or "").strip()
                uf     = (a0.get("state") or a0.get("uf") or "").strip()
                cep    = (a0.get("zipCode") or a0.get("cep") or "").strip()

            criado = c.get("createdAt") or c.get("creationDate") or ""
            if criado:
                try:
                    criado = parse_date(str(criado)).date().isoformat()
                except Exception:
                    pass

            all_rows.append({
                "IdCliente": str(cid) if cid is not None else "",
                "Nome": nome,
                "Sexo": sexo_fmt,
                "Nascimento": nascimento,
                "Idade": idade,
                "Bairro": bairro, "Cidade": cidade, "UF": uf, "CEP": cep,
                "Email": email, "Telefone": tel,
                "CriadoEm": criado,
            })

        page += 1
        skip += take
        if max_pages and page > max_pages:
            break
        if len(lst) < take:
            break

    return pd.DataFrame(all_rows)

# ──────────────────────────────────────────────────────────────────────────────
# UI — Coleta e Análises
# ──────────────────────────────────────────────────────────────────────────────
st.sidebar.header("Coleta de Clientes")
tam = st.sidebar.slider("Tamanho de página", 100, 500, 200, 50)
maxp = st.sidebar.slider("Máx. páginas", 1, 50, 10, 1)

btn = st.sidebar.button("🔄 Atualizar clientes agora", type="primary")
if btn:
    st.session_state["_clientes_cache"] = None

# cache leve em sessão
if "_clientes_cache" not in st.session_state or st.session_state["_clientes_cache"] is None:
    with st.spinner("Carregando base de clientes do EVO..."):
        dfc = _fetch_customers(max_pages=maxp, page_size=tam)
        st.session_state["_clientes_cache"] = dfc.copy()
else:
    dfc = st.session_state["_clientes_cache"].copy()

if dfc.empty:
    st.warning("Nenhum cliente retornado pela API. Verifique permissões ou endpoints.")
    st.stop()

st.success(f"Clientes carregados: {len(dfc)}")

# Downloads
colA, colB = st.columns(2)
with colA:
    st.download_button("⬇️ Baixar clientes (CSV)", dfc.to_csv(index=False, encoding="utf-8-sig"), "clientes.csv", "text/csv")
with colB:
    import io
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine=("xlsxwriter" if any(m for m in ("xlsxwriter",) if m) else "openpyxl")) as writer:
        dfc.to_excel(writer, index=False, sheet_name="Clientes")
    st.download_button("⬇️ Baixar clientes (XLSX)", buffer.getvalue(), "clientes.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

st.divider()

# ── KPIs
k1,k2,k3,k4 = st.columns(4)
with k1: st.metric("Total de clientes", f"{len(dfc):,}".replace(",", "."))
with k2: st.metric("Idade média", f"{np.nanmean([x for x in dfc['Idade'] if pd.notna(x)]) if 'Idade' in dfc else 0:.1f}")
with k3: st.metric("Com email", f"{int((dfc['Email'].astype(bool)).sum())}" if "Email" in dfc else "0")
with k4: st.metric("Com telefone", f"{int((dfc['Telefone'].astype(bool)).sum())}" if "Telefone" in dfc else "0")

# ── Filtros simples
colf1, colf2 = st.columns(2)
with colf1:
    sexos = sorted(dfc["Sexo"].dropna().unique()) if "Sexo" in dfc else []
    sel_sexo = st.multiselect("Sexo", sexos, default=sexos)
with colf2:
    cidade_opts = sorted(dfc["Cidade"].dropna().unique()) if "Cidade" in dfc else []
    sel_cid = st.multiselect("Cidade", cidade_opts, default=cidade_opts)

mask = pd.Series([True]*len(dfc))
if "Sexo" in dfc and sel_sexo:
    mask &= dfc["Sexo"].isin(sel_sexo)
if "Cidade" in dfc and sel_cid:
    mask &= dfc["Cidade"].isin(sel_cid)
dfv = dfc[mask].copy()

st.caption(f"Filtrados: {len(dfv)}")

# ── Gráficos
gcols = st.columns(2)

if "Sexo" in dfv.columns:
    with gcols[0]:
        cont = dfv.groupby("Sexo", as_index=False).size().rename(columns={"size":"Clientes"})
        fig = px.bar(cont, x="Sexo", y="Clientes", title="Clientes por sexo")
        st.plotly_chart(fig, use_container_width=True)

if "Idade" in dfv.columns:
    with gcols[1]:
        dfv_idade = dfv.dropna(subset=["Idade"])
        if not dfv_idade.empty:
            fig = px.histogram(dfv_idade, x="Idade", nbins=25, title="Distribuição de idades")
            st.plotly_chart(fig, use_container_width=True)

st.divider()

cols2 = st.columns(2)
if "Bairro" in dfv.columns:
    with cols2[0]:
        top_bairro = dfv.groupby("Bairro", as_index=False).size().rename(columns={"size":"Clientes"}).sort_values("Clientes", ascending=False).head(20)
        fig = px.bar(top_bairro, x="Bairro", y="Clientes", title="Top bairros (20)")
        fig.update_layout(xaxis_tickangle=-35)
        st.plotly_chart(fig, use_container_width=True)

if "Cidade" in dfv.columns:
    with cols2[1]:
        top_cid = dfv.groupby("Cidade", as_index=False).size().rename(columns={"size":"Clientes"}).sort_values("Clientes", ascending=False).head(20)
        fig = px.bar(top_cid, x="Cidade", y="Clientes", title="Top cidades (20)")
        fig.update_layout(xaxis_tickangle=-35)
        st.plotly_chart(fig, use_container_width=True)

st.divider()
st.subheader("Dados (filtrados)")
st.dataframe(dfv.reset_index(drop=True), use_container_width=True, height=420)
