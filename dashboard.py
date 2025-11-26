# app.py — Roteador + Login da Born to Ski

import os
import streamlit as st

# Config geral do app
st.set_page_config(
    page_title="Born to Ski — Painel Interno",
    page_icon="⛷️",
    layout="wide",
)

# ─────────────────────────────────────────────────────────────
# Sessão / estado
# ─────────────────────────────────────────────────────────────
if "user" not in st.session_state:
    st.session_state.user = None
    st.session_state.role = None  # "admin" ou "coach"

# ─────────────────────────────────────────────────────────────
# Config de usuários (vem de st.secrets ou variáveis de ambiente)
# ─────────────────────────────────────────────────────────────
def get_users_config():
    """
    Lê usuários e senhas de st.secrets ou variáveis de ambiente.
    Você vai configurar isso depois no Streamlit Cloud.
    """
    users = {}

    # Admin (pode tudo)
    admin_user = st.secrets.get("ADMIN_USER", os.environ.get("ADMIN_USER"))
    admin_pwd = st.secrets.get("ADMIN_PASSWORD", os.environ.get("ADMIN_PASSWORD"))
    if admin_user and admin_pwd:
        users[admin_user] = {"password": admin_pwd, "role": "admin"}

    # Isabella (head coach) — acesso limitado
    coach_user = st.secrets.get("COACH_ISABELLA_USER", os.environ.get("COACH_ISABELLA_USER"))
    coach_pwd = st.secrets.get("COACH_ISABELLA_PASSWORD", os.environ.get("COACH_ISABELLA_PASSWORD"))
    if coach_user and coach_pwd:
        users[coach_user] = {"password": coach_pwd, "role": "coach"}

    return users


# ─────────────────────────────────────────────────────────────
# Login / Logout
# ─────────────────────────────────────────────────────────────
def show_login():
    st.title("Born to Ski — Login")

    username = st.text_input("Usuário")
    password = st.text_input("Senha", type="password")

    if st.button("Entrar", type="primary"):
        users = get_users_config()

        user_cfg = users.get(username)
        if not user_cfg or password != user_cfg["password"]:
            st.error("Usuário ou senha inválidos.")
            return

        # Login OK
        st.session_state.user = username
        st.session_state.role = user_cfg["role"]
        st.rerun()


def logout():
    st.session_state.clear()
    st.rerun()


# ─────────────────────────────────────────────────────────────
# Fluxo principal
# ─────────────────────────────────────────────────────────────
# Se não está logado, mostra tela de login e para aqui
if st.session_state.user is None:
    show_login()
    st.stop()

# Já está logado: mostra info + botão de sair
with st.sidebar:
    st.markdown(f"👤 **{st.session_state.user}**")
    role_label = "Administração" if st.session_state.role == "admin" else "Head Coach"
    st.caption(f"Perfil: {role_label}")

    if st.button("Sair"):
        logout()

# ─────────────────────────────────────────────────────────────
# Definição das páginas de acordo com o perfil
# ─────────────────────────────────────────────────────────────

dashboard_page = st.Page("dashboard_page.py", title="Dashboard", icon="📊")

base_clientes_page = st.Page(
    "pages/2_Base_de_Clientes.py",
    title="Base de Clientes",
    icon="👥",
)

evolucao_nivel_page = st.Page(
    "pages/3_Evolucao_de_Nivel.py",
    title="Evolução de Nível",
    icon="📈",
)

metricas_vendas_page = st.Page(
    "pages/4_Metricas_Vendas.py",
    title="Métricas de Vendas",
    icon="📗",
)

reporte_acidentes_page = st.Page(
    "pages/5_Reporte_de_Acidentes.py",
    title="Reporte de Acidentes",
    icon="⚠️",
)

restore_db_page = st.Page(
    "pages/98_Restaurar_DB_de_Backup.py",
    title="Restaurar DB (GitHub)",
    icon="💾",
)

wipe_db_page = st.Page(
    "pages/99_Wipe_DB.py",
    title="Wipe DB",
    icon="🧹",
)

# Páginas por perfil
if st.session_state.role == "admin":
    pages_for_role = [
        dashboard_page,
        base_clientes_page,
        evolucao_nivel_page,
        reporte_acidentes_page,   # 👈 novo
        metricas_vendas_page,
        restore_db_page,
        wipe_db_page,
    ]

elif st.session_state.role == "coach":
    pages_for_role = [
        base_clientes_page,
        evolucao_nivel_page,
        reporte_acidentes_page,   # 👈 novo
    ]

else:
    st.error("Perfil sem páginas configuradas. Fale com a administração.")
    st.stop()

# Cria o menu de navegação dinâmico
pg = st.navigation(pages_for_role)
pg.run()
