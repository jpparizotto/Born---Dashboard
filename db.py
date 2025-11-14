# db.py
# Banco interno da Born to Ski para clientes + histórico de nível

import sqlite3
from pathlib import Path
from datetime import datetime

DB_DIR = Path(__file__).parent / "data"
DB_DIR.mkdir(exist_ok=True)
DB_PATH = DB_DIR / "bts_clients.db"

LEVELS = [
    "1A", "1B", "1C", "1D",
    "2A", "2B", "2C", "2D",
    "3A", "3B", "3C", "3D",
    "4A", "4B", "4C", "4D",
]
LEVEL_ORDER = {code: i for i, code in enumerate(LEVELS)}

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_conn()
    cur = conn.cursor()

    # Tabela de clientes (estado atual)
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

    # Histórico de nível do cliente (evolução ao longo do tempo)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS level_history (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id       INTEGER NOT NULL,
        data_evento     TEXT NOT NULL,
        nivel           TEXT,
        nivel_ordem     INTEGER,
        origem          TEXT,            -- ex: 'sync_members', 'aula_manual', etc.
        created_at      TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(client_id) REFERENCES clients(id)
    );
    """)

    # Índices úteis
    cur.execute("CREATE INDEX IF NOT EXISTS idx_clients_evo_id ON clients(evo_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_hist_client ON level_history(client_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_hist_data ON level_history(data_evento);")

    conn.commit()
    conn.close()

def upsert_client(row):
    """
    row é um dict com campos compatíveis com a tabela clients.
    upsert baseado em evo_id.
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

def _get_client_id_by_evo(evo_id: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, nivel_atual, nivel_ordem FROM clients WHERE evo_id = ?", (evo_id,))
    row = cur.fetchone()
    conn.close()
    return row

def add_level_snapshot(evo_id: str, nivel: str, data_evento: str, origem: str = "sync_members"):
    """
    Grava um registro de histórico de nível se:
    - cliente existir
    - nível não for vazio
    - for diferente do último nível registrado no banco (clients.nivel_atual)
    """
    if not nivel:
        return

    nivel = nivel.upper()
    nivel_ordem = LEVEL_ORDER.get(nivel)
    cli = _get_client_id_by_evo(evo_id)
    if not cli:
        return

    client_id = cli["id"]
    nivel_atual_db = cli["nivel_atual"]

    # se nada mudou, não precisa gravar nova linha
    if nivel_atual_db and nivel_atual_db.upper() == nivel:
        return

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO level_history (client_id, data_evento, nivel, nivel_ordem, origem)
        VALUES (?, ?, ?, ?, ?)
    """, (client_id, data_evento, nivel, nivel_ordem, origem))
    conn.commit()
    conn.close()
