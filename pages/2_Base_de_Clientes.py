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
def _auth_headers():
    import base64
    auth_str = f"{EVO_USER}:{EVO_TOKEN}"
    b64 = base64.b64encode(auth_str.encode()).decode()
    return {
        "Authorization": f"Basic {b64}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

def _get_json(path, params=None):
    url = f"{BASE_URL.rstrip('/')}/{path.lstrip('/')}"
    r = requests.get(url, headers=_auth_headers(), params=params or {}, verify=VERIFY_SSL, timeout=60)
    if r.status_code != 200:
        # permite 204 (sem conteúdo) como vazio
        if r.status_code == 204:
            return []
        raise RuntimeError(f"GET {url} -> {r.status_code} | {r.text[:400]}")
    try:
        data = r.json()
    except Exception:
        return []
    # muitos endpoints do EVO vêm como {"data": [...]}
    if isinstance(data, dict) and "data" in data and isinstance(data["data"], (list, dict)):
        return data["data"]
    return data

def _to_list(maybe):
    if isinstance(maybe, list):
        return maybe
    if isinstance(maybe, dict):
        # algumas instâncias retornam {"items":[...]} etc.
        for k in ("items", "results", "content", "list", "rows"):
            if k in maybe and isinstance(maybe[k], list):
                return maybe[k]
    return []

# ──────────────────────────────────────────────────────────────────────────────
# COLETA DE CLIENTES (paginada, tentando diferentes esquemas)
# ──────────────────────────────────────────────────────────────────────────────
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

def _fetch_customers(max_pages=20, page_size=200):
    """
    Busca clientes do EVO tentando diferentes esquemas de paginação.
    Retorna DataFrame com colunas normalizadas: Nome, Sexo, Nascimento, Idade, Bairro, Cidade, UF, CEP, Email, Telefone, CriadoEm, IdCliente
    """
    id_branch = _listar_id_branch()
    all_rows = []
    seen_ids = set()

    # Alguns tenants expõem /customers; outros /customer ou /person
    endpoints = ["customers", "customer", "person", "people", "members", "clients"]

    for ep in endpoints:
        # testa rapidamente sem paginação
        try:
            lst = _to_list(_get_json(ep))
            if lst:
                # ok, endpoint válido -> vamos paginar
                valid_endpoint = ep
                break
        except Exception:
            continue
    else:
        # nenhum endpoint respondeu
        return pd.DataFrame()

    # possíveis combos de paginação
    def _page_param_variants(page, size):
        offset = (page - 1) * size
        return [
            {"page": page, "size": size},
            {"pageNumber": page, "pageSize": size},
            {"page": page, "per_page": size},
            {"offset": offset, "limit": size},
            {"skip": offset, "take": size},
            {"start": offset, "count": size},
        ]

    # coleta páginas
    for page in range(1, max_pages + 1):
        got = False
        for params in _page_param_variants(page, page_size):
            if id_branch:
                params["idBranch"] = id_branch
            try:
                data = _get_json(valid_endpoint, params=params)
            except Exception:
                continue
            lst = _to_list(data)
            if not lst:
                continue
            got = True
            # normaliza itens
            for c in lst:
                # id
                cid = c.get("id") or c.get("idCustomer") or c.get("customerId") or c.get("personId")
                if cid in seen_ids:
                    continue
                seen_ids.add(cid)

                # nome
                nome = c.get("name") or c.get("fullName") or c.get("displayName") or c.get("customerName") or c.get("personName") or ""

                # sexo
                sexo = c.get("gender") or c.get("sexo") or c.get("sex") or ""
                if isinstance(sexo, dict):
                    sexo = sexo.get("name") or sexo.get("description")
                sexo = str(sexo).strip().lower()
                if sexo in ("m", "masc", "masculino", "male"):
                    sexo = "Masculino"
                elif sexo in ("f", "fem", "feminino", "female"):
                    sexo = "Feminino"
                elif sexo:
                    sexo = sexo.capitalize()
                else:
                    sexo = "Não informado"

                # nascimento -> idade
                nasc_raw = c.get("birthDate") or c.get("birthday") or c.get("dtBirth") or c.get("birth") or None
                idade = None
                nascimento = None
                if nasc_raw:
                    try:
                        dtn = parse_date(str(nasc_raw)).date()
                        nascimento = dtn.isoformat()
                        today = date.today()
                        idade = int((today - dtn).days // 365.25)
                        if idade < 0 or idade > 120:
                            idade = None
                    except Exception:
                        nascimento = None
                        idade = None

                # contato
                email = c.get("email") or c.get("mail") or ""
                tel = c.get("phone") or c.get("mobile") or c.get("cellphone") or c.get("telefone") or ""

                # endereço
                addr = c.get("address") or c.get("endereco") or {}
                if isinstance(addr, list) and addr:
                    addr = addr[0]
                bairro = (addr.get("neighborhood") or addr.get("bairro") or "").strip() if isinstance(addr, dict) else ""
                cidade = (addr.get("city") or addr.get("cidade") or "").strip() if isinstance(addr, dict) else ""
                uf = (addr.get("state") or addr.get("uf") or "").strip() if isinstance(addr, dict) else ""
                cep = (addr.get("zipCode") or addr.get("cep") or "").strip() if isinstance(addr, dict) else ""

                # criado/em
                criado = c.get("createdAt") or c.get("creationDate") or c.get("dtCreated") or ""
                if criado:
                    try:
                        criado = parse_date(str(criado)).date().isoformat()
                    except Exception:
                        pass

                all_rows.append({
                    "IdCliente": cid,
                    "Nome": nome,
                    "Sexo": sexo or "Não informado",
                    "Nascimento": nascimento,
                    "Idade": idade,
                    "Bairro": bairro,
                    "Cidade": cidade,
                    "UF": uf,
                    "CEP": cep,
                    "Email": email,
                    "Telefone": tel,
                    "CriadoEm": criado,
                })
            break  # não tenta outras variantes se uma já funcionou nesta página

        if not got:
            # nenhuma variante retornou nesta página → encerramos
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
