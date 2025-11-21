# app.py
import streamlit as st
from auth import require_login

# importa as "páginas" como módulos
from app_pages.dashboard_page import render as render_dashboard
from app_pages.base_clientes_page import render as render_base_clientes
from app_pages.evolucao_nivel_page import render as render_evolucao
from app_pages.wipe_db_page import render as render_wipe_db

st.set_page_config(
    page_title="Born to Ski Dashboard",
    page_icon="⛷️",
    layout="wide",
)

# Login obrigatório (admin ou colaborador)
user = require_login(allowed_roles=["admin", "colaborador"])
role = user["role"]

st.sidebar.title("Born to Ski")

# Menu condicional por papel
if role == "admin":
    opcoes = ["Dashboard", "Base de Clientes", "Evolução de Nível", "Wipe DB"]
else:  # colaborador
    opcoes = ["Base de Clientes", "Evolução de Nível"]

escolha = st.sidebar.radio("Navegação", opcoes)

# Roteamento simples
if escolha == "Dashboard":
    render_dashboard()
elif escolha == "Base de Clientes":
    render_base_clientes()
elif escolha == "Evolução de Nível":
    render_evolucao()
elif escolha == "Wipe DB":
    render_wipe_db()
