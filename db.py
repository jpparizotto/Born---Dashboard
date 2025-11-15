# db.py
# Banco interno da Born to Ski para clientes, sessões (aulas) e histórico de nível

import sqlite3
from pathlib import Path
from datetime import datetime, date

DB_DIR = Path(__file__).parent / "data"
DB_DIR.mkdir(exist_ok=True)
DB_PATH = DB_DIR / "bts_clients.db"

# Níveis válidos e ordem
LEVELS = [
    "1A", "1B", "1C", "1D",
    "2A", "2B", "2C", "2D",
    "3A", "3B", "3C", "3D",
    "4A", "4B", "4C", "4D",
]
LEVEL_ORDER = {code: i for i, code in enumerate(LEVELS)}


# ---------------------------------------------------------------------------
# Conexão e criação de tabelas
# ---------------------------------------------------------------------------
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    # Estado atual do cliente
    cur.execute("""
    CREATE TABLE IF NOT EXISTS clients (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        evo_id          TEXT UNIQUE NOT NULL,
        nome_bruto      TEXT,
        nome_limpo      TEXT,
        nivel_atual     TEXT,
        nivel_ordem     INTEGER,
        sexo            TEXT,
        nascimento      TEXT,
        idade           INTEGER,
        cidade          TEXT,
        bairro          TEXT,
        uf              TEXT,
        email           TEXT,
        telefone        TEXT,
        criado_em       TEXT,
        updated_at      TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # Histórico de nível do cliente
    cur.execute("""
    CREATE TABLE IF NOT EXISTS level_history (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id       INTEGER NOT NULL,
        evo_id          TEXT NOT NULL,
        data_evento     TEXT NOT NULL,
        nivel           TEXT,
        nivel_ordem     INTEGER,
        origem          TEXT,
        created_at      TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(client_id) REFERENCES clients(id)
    );
    """)

    # Sessões (aulas) do cliente – vindas da API de schedule do EVO
    cur.execute("""
    CREATE TABLE IF NOT EXISTS member_sessions (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        evo_id              TEXT NOT NULL,
        activity_session_id TEXT,
        configuration_id    TEXT,
        data                TEXT,
        start_time          TEXT,
        end_time            TEXT,
        activity_name       TEXT,
        area_name           TEXT,
        status_activity     TEXT,
        status_client       TEXT,
        is_replacement      INTEGER,
        origem              TEXT,
        created_at          TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at          TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(evo_id, activity_session_id, data)
    );
    """)

    # Índices
    cur.execute("CREATE INDEX IF NOT EXISTS idx_clients_evo_id ON clients(evo_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_hist_client ON level_history(client_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_hist_data ON level_history(data_evento);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sessions_evo_id ON member_sessions(evo_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sessions_data ON member_sessions(data);")

    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Helpers de clientes
# ---------------------------------------------------------------------------
def get_client_by_evo(evo_id: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM clients WHERE evo_id = ?", (evo_id,))
    row = cur.fetchone()
    conn.close()
    return row


def upsert_client(row):
    """
    row é um dict (p.ex. linha do DataFrame amigável) com campos:
      IdCliente, Nome, NomeLimpo, NivelAtual, NivelOrdem, Sexo, Nascimento, Idade,
      Cidade, Bairro, UF, Email, Telefone, CriadoEm
    """
    conn = get_conn()
    cur = conn.cursor()
    now = datetime.utcnow().isoformat()

    cur.execute("""
        INSERT INTO clients (
            evo_id, nome_bruto, nome_limpo, nivel_atual, nivel_ordem,
            sexo, nascimento, idade, cidade, bairro, uf,
            email, telefone, criado_em, updated_at
        )
        VALUES (
            :evo_id, :nome_bruto, :nome_limpo, :nivel_atual, :nivel_ordem,
            :sexo, :nascimento, :idade, :cidade, :bairro, :uf,
            :email, :telefone, :criado_em, :updated_at
        )
        ON CONFLICT(evo_id) DO UPDATE SET
            nome_bruto   = excluded.nome_bruto,
            nome_limpo   = excluded.nome_limpo,
            nivel_atual  = excluded.nivel_atual,
            nivel_ordem  = excluded.nivel_ordem,
            sexo         = excluded.sexo,
            nascimento   = excluded.nascimento,
            idade        = excluded.idade,
            cidade       = excluded.cidade,
            bairro       = excluded.bairro,
            uf           = excluded.uf,
            email        = excluded.email,
            telefone     = excluded.telefone,
            criado_em    = COALESCE(clients.criado_em, excluded.criado_em),
            updated_at   = :updated_at
        ;
    """, {
        "evo_id": row.get("IdCliente"),
        "nome_bruto": row.get("Nome"),
        "nome_limpo": row.get("NomeLimpo"),
        "nivel_atual": row.get("NivelAtual"),
        "nivel_ordem": row.get("NivelOrdem"),
        "sexo": row.get("Sexo"),
        "nascimento": row.get("Nascimento"),
        "idade": row.get("Idade"),
        "cidade": row.get("Cidade"),
        "bairro": row.get("Bairro"),
        "uf": row.get("UF"),
        "email": row.get("Email"),
        "telefone": row.get("Telefone"),
        "criado_em": row.get("CriadoEm"),
        "updated_at": now,
    })

    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Sessões (aulas)
# ---------------------------------------------------------------------------
def upsert_session(evo_id: str, sess: dict):
    """
    sess: dict já normalizado com:
      activity_session_id, configuration_id, data, start_time, end_time,
      activity_name, area_name, status_activity, status_client, is_replacement, origem
    """
    conn = get_conn()
    cur = conn.cursor()
    now = datetime.utcnow().isoformat()

    cur.execute("""
        INSERT INTO member_sessions (
            evo_id, activity_session_id, configuration_id, data,
            start_time, end_time, activity_name, area_name,
            status_activity, status_client, is_replacement,
            origem, created_at, updated_at
        )
        VALUES (
            :evo_id, :activity_session_id, :configuration_id, :data,
            :start_time, :end_time, :activity_name, :area_name,
            :status_activity, :status_client, :is_replacement,
            :origem, :created_at, :updated_at
        )
        ON CONFLICT(evo_id, activity_session_id, data) DO UPDATE SET
            configuration_id = excluded.configuration_id,
            start_time       = excluded.start_time,
            end_time         = excluded.end_time,
            activity_name    = excluded.activity_name,
            area_name        = excluded.area_name,
            status_activity  = excluded.status_activity,
            status_client    = excluded.status_client,
            is_replacement   = excluded.is_replacement,
            origem           = excluded.origem,
            updated_at       = :updated_at
        ;
    """, {
        "evo_id": evo_id,
        "activity_session_id": sess.get("activity_session_id"),
        "configuration_id": sess.get("configuration_id"),
        "data": sess.get("data"),
        "start_time": sess.get("start_time"),
        "end_time": sess.get("end_time"),
        "activity_name": sess.get("activity_name"),
        "area_name": sess.get("area_name"),
        "status_activity": sess.get("status_activity"),
        "status_client": sess.get("status_client"),
        "is_replacement": int(bool(sess.get("is_replacement"))),
        "origem": sess.get("origem") or "evo_schedule",
        "created_at": now,
        "updated_at": now,
    })

    conn.commit()
    conn.close()


def get_last_presence_session_date(evo_id: str):
    """
    Retorna a data (string YYYY-MM-DD) da última aula em que o cliente teve presença
    (ou, na falta disso, a última sessão qualquer), ou None.
    """
    conn = get_conn()
    cur = conn.cursor()

    # Tenta último registro com status_client indicando presença
    cur.execute("""
        SELECT data, status_client FROM member_sessions
        WHERE evo_id = ?
        ORDER BY date(data) DESC
        LIMIT 50
    """, (evo_id,))
    rows = cur.fetchall()
    conn.close()

    if not rows:
        return None

    # Dá preferência a algo que pareça "Presença"
    for r in rows:
        sc = (r["status_client"] or "").upper()
        if "PRESEN" in sc:  # pega 'PRESENÇA' / 'PRESENCA'
            return r["data"]

    # Se nada parecer presença, pega a data mais recente mesmo
    return rows[0]["data"]


# ---------------------------------------------------------------------------
# Histórico de nível
# ---------------------------------------------------------------------------
def add_level_snapshot(evo_id: str, novo_nivel: str,
                       data_evento: str | None = None,
                       origem: str = "sync_members",
                       old_level: str | None = None):
    """
    Grava um registro de histórico de nível se:
    - houver cliente com esse evo_id
    - novo_nivel não for vazio
    - novo_nivel for diferente de old_level

    Se data_evento for None:
      - tenta usar a data da última sessão com presença
      - se não houver, usa hoje
    """
    if not novo_nivel:
        return

    novo_nivel = str(novo_nivel).upper()
    nivel_ordem = LEVEL_ORDER.get(novo_nivel)

    cli = get_client_by_evo(evo_id)
    if not cli:
        return

    if old_level:
        if str(old_level).upper() == novo_nivel:
            return  # nada mudou
    else:
        # fallback: compara com o que está no banco
        atual_db = cli["nivel_atual"]
        if atual_db and str(atual_db).upper() == novo_nivel:
            return

    if not data_evento:
        data_evento = get_last_presence_session_date(evo_id) or date.today().isoformat()

    conn = get_conn()
    cur = conn.cursor()
    now = datetime.utcnow().isoformat()

    cur.execute("""
        INSERT INTO level_history (
            client_id, evo_id, data_evento,
            nivel, nivel_ordem, origem, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        cli["id"], evo_id, data_evento,
        novo_nivel, nivel_ordem, origem, now
    ))

    conn.commit()
    conn.close()
