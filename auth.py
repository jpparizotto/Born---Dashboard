# auth.py
import streamlit as st

# "Banco" de usuários (depois podemos mover para st.secrets)
USERS = {
    "admin": {
        "password": "admin123",    # TROQUE por uma senha forte
        "role": "admin",
    },
    "funcionaria": {
        "password": "func123",     # TROQUE por uma senha real
        "role": "colaborador",
    },
}


def _init_auth_state():
    if "auth" not in st.session_state:
        st.session_state.auth = {
            "logged_in": False,
            "username": None,
            "role": None,
        }


def logout():
    _init_auth_state()
    st.session_state.auth = {
        "logged_in": False,
        "username": None,
        "role": None,
    }
    st.experimental_rerun()


def require_login(allowed_roles=None):
    """
    - Se não estiver logado, mostra tela de login e dá st.stop()
    - Se estiver logado mas o papel não estiver em allowed_roles, bloqueia
    - Se estiver ok, retorna {"username": ..., "role": ...}
    """
    _init_auth_state()
    auth = st.session_state.auth

    # Já logado
    if auth["logged_in"]:
        with st.sidebar:
            st.markdown(
                f"👤 **Usuário:** {auth['username']}  \n"
                f"🔐 **Perfil:** {auth['role']}"
            )
            if st.button("Sair"):
                logout()

        if allowed_roles is not None and auth["role"] not in allowed_roles:
            st.error("Você não tem permissão para acessar esta página.")
            st.stop()

        return auth

    # Não logado → tela de login
    st.title("🔐 Login Born to Ski")

    with st.form("login_form"):
        username = st.text_input("Usuário")
        password = st.text_input("Senha", type="password")
        ok = st.form_submit_button("Entrar")

    if ok:
        user_cfg = USERS.get(username)
        if user_cfg and user_cfg["password"] == password:
            st.session_state.auth = {
                "logged_in": True,
                "username": username,
                "role": user_cfg["role"],
            }
            st.experimental_rerun()
        else:
            st.error("Usuário ou senha inválidos.")

    st.stop()
