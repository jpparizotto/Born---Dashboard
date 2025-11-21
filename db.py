# db.py
# Banco interno de clientes + histórico de nível Born to Ski

import os
import sqlite3
from pathlib import Path
from datetime import date

import pandas as pd

# Caminho do banco
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "bts_clients.db"


# ---------------------------------------------------------------------------
# Conexão / inicialização
# ---------------------------------------------------------------------------

def get_connection() -> sqlite3.Connection:
    """Abre conexão com o SQLite garantindo PRAGMAs básicos."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_db_if_needed() -> None:
    """Cria as tabelas se ainda não existirem."""
    conn = get_connection()
    cur = conn.cursor()

    # Tabela principal de clientes
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS clients (
            evo_id        TEXT PRIMARY KEY,
            nome_bruto    TEXT,
            nome_limpo    TEXT,
            sexo          TEXT,
            nascimento    TEXT,
            idade         INTEGER,
            rua           TEXT,
            numero        TEXT,
            complemento   TEXT,
            bairro        TEXT,
            cidade        TEXT,
            uf            TEXT,
            cep           TEXT,
            email         TEXT,
            telefone      TEXT,
            criado_em     TEXT,
            nivel_atual   TEXT,
            nivel_ordem   INTEGER,
            updated_at    TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    # Histórico de nível
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS level_history (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            evo_id       TEXT NOT NULL,
            data         TEXT NOT NULL,
            nivel        TEXT NOT NULL,
            nivel_ordem  INTEGER,
            origem       TEXT NOT NULL DEFAULT 'sync_clientes',
            created_at   TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    conn.commit()
    conn.close()


def wipe_db() -> None:
    """Apaga completamente o banco (usado só manualmente / debug)."""
    if DB_PATH.exists():
        DB_PATH.unlink()


# ---------------------------------------------------------------------------
# Regras de nível
# ---------------------------------------------------------------------------

# Mapa para ordenar os níveis
LEVEL_ORDER = {
    "1A": 10, "1B": 11, "1C": 12, "1D": 13,
    "2A": 20, "2B": 21, "2C": 22, "2D": 23,
    "3A": 30, "3B": 31, "3C": 32, "3D": 33,
    "4A": 40, "4B": 41, "4C": 42, "4D": 43,
}


def _extract_nome_e_nivel(nome_bruto: str):
    """
    Recebe o nome como vem do EVO, ex:
    'João Paulo 3C', 'HENRIQUE BISSOCI 3A SB/2CSKI', 'Marina 2B+'

    Retorna (nome_limpo, nivel), ex:
    ('João Paulo', '3C')
    ('HENRIQUE BISSOCI SB/ 2CSKI', '3A')
    """
    if not isinstance(nome_bruto, str):
        return "", None

    texto = nome_bruto.strip()
    if not texto:
        return "", None

    import re

    # Aceita variações como "2 B", "2B+", "3C.", "4A/3B" etc.
    pattern = r"\b([1-4]\s*[A-D])\b|([1-4]\s*[A-D])(?=[^A-Za-z0-9])"

    matches = re.findall(pattern, texto.upper())
    # junta os dois grupos possíveis da regex
    matches = [m[0] or m[1] for m in matches if m[0] or m[1]]
    # normaliza "2 B" -> "2B"
    matches = [m.replace(" ", "") for m in matches]

    # filtra só níveis válidos
    matches = [m for m in matches if m in LEVEL_ORDER]
    if not matches:
        return texto, None

    # Se houver mais de um, pega o de maior ordem (ex: 2A/3C -> 3C)
    best = max(matches, key=lambda x: LEVEL_ORDER.get(x, -1))

    # Nome limpo: hoje você usa só o texto original;
    # se quiser mesmo tirar o nível, dá pra ajustar aqui.
    nome_limpo = texto.strip()

    return nome_limpo, best

# ---------------------------------------------------------------------------
# Sincronização de clientes + histórico de nível
# ---------------------------------------------------------------------------

def sync_clients_from_df(df_clientes: pd.DataFrame) -> int:
    """
    Recebe o DF 'amigável' da página 2_Base_de_Clientes e
    sincroniza com o banco local.

    - Upsert na tabela clients
    - Se o nível mudou, grava uma linha em level_history
    Retorna o número de clientes processados.
    """
    init_db_if_needed()
    conn = get_connection()
    cur = conn.cursor()

    hoje = date.today().isoformat()

    # Garante colunas esperadas existindo no DF
    cols = df_clientes.columns

    def get(row, col, default=None):
        return row[col] if col in cols else default

    processed = 0

    for _, row in df_clientes.iterrows():
        evo_id = str(get(row, "IdCliente", "")).strip()
        if not evo_id:
            continue

        nome_bruto = str(get(row, "Nome", "") or "").strip()
        nome_limpo, nivel = _extract_nome_e_nivel(nome_bruto)
        nivel_ordem = LEVEL_ORDER.get(nivel)

        sexo = get(row, "Sexo")
        nascimento = get(row, "Nascimento")
        idade = get(row, "Idade")
        rua = get(row, "Rua")
        numero = get(row, "Numero")
        compl = get(row, "Complemento")
        bairro = get(row, "Bairro")
        cidade = get(row, "Cidade")
        uf = get(row, "UF")
        cep = get(row, "CEP")
        email = get(row, "Email")
        tel = get(row, "Telefone")
        criado_em = get(row, "CriadoEm")

        # Nível anterior (se já existir cliente)
        cur.execute(
            "SELECT nivel_atual FROM clients WHERE evo_id = ?",
            (evo_id,),
        )
        row_prev = cur.fetchone()
        nivel_anterior = row_prev["nivel_atual"] if row_prev else None

        # UPSERT do cliente
        cur.execute(
            """
            INSERT INTO clients (
                evo_id, nome_bruto, nome_limpo, sexo, nascimento, idade,
                rua, numero, complemento, bairro, cidade, uf, cep,
                email, telefone, criado_em, nivel_atual, nivel_ordem, updated_at
            )
            VALUES (
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, CURRENT_TIMESTAMP
            )
            ON CONFLICT(evo_id) DO UPDATE SET
                nome_bruto   = excluded.nome_bruto,
                nome_limpo   = excluded.nome_limpo,
                sexo         = excluded.sexo,
                nascimento   = excluded.nascimento,
                idade        = excluded.idade,
                rua          = excluded.rua,
                numero       = excluded.numero,
                complemento  = excluded.complemento,
                bairro       = excluded.bairro,
                cidade       = excluded.cidade,
                uf           = excluded.uf,
                cep          = excluded.cep,
                email        = excluded.email,
                telefone     = excluded.telefone,
                criado_em    = COALESCE(clients.criado_em, excluded.criado_em),
                nivel_atual  = excluded.nivel_atual,
                nivel_ordem  = excluded.nivel_ordem,
                updated_at   = CURRENT_TIMESTAMP;
            """,
            (
                evo_id, nome_bruto, nome_limpo, sexo, nascimento, idade,
                rua, numero, compl, bairro, cidade, uf, cep,
                email, tel, criado_em, nivel, nivel_ordem
            ),
        )

        # Se o nível mudou, grava histórico
        if nivel and nivel != nivel_anterior:
            cur.execute(
                """
                INSERT INTO level_history (evo_id, data, nivel, nivel_ordem, origem)
                VALUES (?, ?, ?, ?, ?);
                """,
                (evo_id, hoje, nivel, nivel_ordem, "sync_clientes"),
            )

        processed += 1

    conn.commit()
    conn.close()
    return processed

# ---------------------------------------------------------------------------
# Snapshot diário de quantidade de clientes
# ---------------------------------------------------------------------------

def _ensure_daily_clients_table():
    """Garante a existência da tabela de histórico diário."""
    init_db_if_needed()
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_clients (
            data TEXT PRIMARY KEY,
            total_clientes INTEGER NOT NULL,
            novos_clientes INTEGER NOT NULL
        );
        """
    )
    conn.commit()
    conn.close()


def register_daily_client_count(total_clientes: int) -> None:
    """
    Registra o total de clientes do dia.
    Se já existir registro no dia, ATUALIZA o valor.
    """
    _ensure_daily_clients_table()
    conn = get_connection()
    cur = conn.cursor()

    hoje = date.today().isoformat()

    # pega o último snapshot anterior
    cur.execute(
        "SELECT data, total_clientes FROM daily_clients WHERE data < ? ORDER BY data DESC LIMIT 1;",
        (hoje,)
    )
    row = cur.fetchone()
    last_total = row[1] if row else 0

    novos = max(int(total_clientes) - int(last_total), 0)

    # Se já existe registro pro dia, atualiza
    cur.execute("SELECT 1 FROM daily_clients WHERE data = ?;", (hoje,))
    if cur.fetchone():
        cur.execute(
            "UPDATE daily_clients SET total_clientes = ?, novos_clientes = ? WHERE data = ?;",
            (int(total_clientes), int(novos), hoje)
        )
    else:
        cur.execute(
            "INSERT INTO daily_clients (data, total_clientes, novos_clientes) VALUES (?, ?, ?);",
            (hoje, int(total_clientes), int(novos))
        )

    conn.commit()
    conn.close()

def load_daily_client_counts() -> pd.DataFrame:
    """
    Retorna DataFrame com:
      - data
      - total_clientes
      - novos_clientes
    ordenado por data.
    """
    _ensure_daily_clients_table()
    conn = get_connection()
    df = pd.read_sql_query(
        """
        SELECT data, total_clientes, novos_clientes
        FROM daily_clients
        ORDER BY data;
        """,
        conn,
        parse_dates=["data"],
    )
    conn.close()
    return df

