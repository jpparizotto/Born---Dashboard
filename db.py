# db.py
# Banco interno de clientes + histórico de nível Born to Ski

import os
import sqlite3
from pathlib import Path
from datetime import date

import pandas as pd
import base64
import io
import requests

# Caminho do banco
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "bts_clients.db"
GITHUB_OWNER = os.environ.get("GITHUB_OWNER", "")
GITHUB_REPO = os.environ.get("GITHUB_REPO", "")
GITHUB_BRANCH = os.environ.get("GITHUB_BRANCH", "main")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
ACCIDENTS_CSV_PATH = os.path.join(DATA_DIR, "acidentes.csv")
def _github_headers():
    if not GITHUB_TOKEN:
        return None
    return {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _github_file_url(path_in_repo: str) -> str:
    if not GITHUB_OWNER or not GITHUB_REPO:
        raise RuntimeError("GITHUB_OWNER e GITHUB_REPO não configurados")
    return f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/{path_in_repo}"

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

def _upload_bytes_to_github(path_in_repo: str, content: bytes, message: str) -> None:
    """
    Cria/atualiza um arquivo no GitHub usando a API de contents.
    Não levanta erro para não derrubar a app se falhar.
    """
    headers = _github_headers()
    if not headers:
        # Se não tiver token configurado, não faz nada.
        print("[backup_db_to_github] GITHUB_TOKEN não configurado, ignorando backup.")
        return

    url = _github_file_url(path_in_repo)

    try:
        # Descobre se já existe para pegar o sha
        resp = requests.get(url, headers=headers, timeout=10)
        sha = resp.json().get("sha") if resp.status_code == 200 else None

        data = {
            "message": message,
            "content": base64.b64encode(content).decode("utf-8"),
            "branch": GITHUB_BRANCH,
        }
        if sha:
            data["sha"] = sha

        put_resp = requests.put(url, headers=headers, json=data, timeout=10)
        put_resp.raise_for_status()
    except Exception as e:
        # Log simples – não quebra a app
        print(f"[backup_db_to_github] Erro ao subir {path_in_repo}: {e}")
        
def backup_db_to_github() -> None:
    """
    Exporta as tabelas principais para CSV e envia para o GitHub.

    Arquivos criados no repositório:
      - backups/clients.csv
      - backups/level_history.csv
      - backups/daily_clients.csv
    """
    headers = _github_headers()
    if not headers:
        # Se não tiver token, simplesmente não faz backup
        return

    try:
        conn = get_connection()

        for table in ["clients", "level_history", "daily_clients"]:
            try:
                df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
            except Exception as e:
                print(f"[backup_db_to_github] Não foi possível ler tabela {table}: {e}")
                continue

            csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
            path_repo = f"backups/{table}.csv"
            msg = f"Snapshot automático da tabela {table}"
            _upload_bytes_to_github(path_repo, csv_bytes, msg)

        print("[backup_db_to_github] Backup concluído.")
        
    except Exception as e:
        print("[backup_db_to_github] Erro geral ao fazer backup:", e)

def wipe_db() -> None:
    """Apaga completamente o banco (usado só manualmente / debug)."""
    if DB_PATH.exists():
        DB_PATH.unlink()

def restore_db_from_github() -> int:
    """
    Apaga o arquivo data/bts_clients.db (se existir) e recria as tabelas
    carregando os dados dos CSVs armazenados em:

      backups/clients.csv
      backups/level_history.csv
      backups/daily_clients.csv

    Retorna o número total de linhas importadas.
    """
    if not GITHUB_OWNER or not GITHUB_REPO:
        raise RuntimeError("GITHUB_OWNER e GITHUB_REPO não configurados.")

    # Apaga o DB atual (se existir)
    if DB_PATH.exists():
        DB_PATH.unlink()

    # Cria o arquivo e as tabelas vazias
    init_db_if_needed()
    conn = get_connection()

    base_raw = f"https://raw.githubusercontent.com/{GITHUB_OWNER}/{GITHUB_REPO}/{GITHUB_BRANCH}/backups"
    total_rows = 0

    for table in ["clients", "level_history", "daily_clients"]:
        url = f"{base_raw}/{table}.csv"
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                print(f"[restore_db_from_github] CSV de {table} não encontrado ({resp.status_code}).")
                continue

            csv_text = resp.text
            df = pd.read_csv(io.StringIO(csv_text))
            if not df.empty:
                df.to_sql(table, conn, if_exists="append", index=False)
                total_rows += len(df)
                print(f"[restore_db_from_github] Importadas {len(df)} linhas em {table}.")
        except Exception as e:
            print(f"[restore_db_from_github] Erro ao restaurar {table}: {e}")

    conn.commit()
    conn.close()

    return total_rows

def backup_acidentes_to_github() -> None:
    """
    Envia o arquivo data/acidentes.csv para o GitHub em backups/acidentes.csv.
    Usa a mesma infraestrutura de upload do backup_db_to_github.
    """
    from pathlib import Path

    csv_path = Path(ACCIDENTS_CSV_PATH)
    if not csv_path.exists():
        print("[backup_acidentes_to_github] Arquivo de acidentes não encontrado, nada para fazer.")
        return

    with csv_path.open("rb") as f:
        content = f.read()

    # ⚠️ IMPORTANTE:
    # Aqui você deve usar a MESMA função interna que o backup_db_to_github usa
    # para mandar bytes para o GitHub.
    #
    # Se no seu código essa função tiver outro nome,
    # troque "_upload_bytes_to_github" pelo nome correto.
    from .github_utils import upload_bytes_to_github  # ajuste este import conforme seu projeto

    upload_bytes_to_github(
        repo_path="backups/acidentes.csv",
        content=content,
        commit_message="Backup acidentes.csv (reporte de acidentes)",
    )

    print("[backup_acidentes_to_github] Backup de acidentes enviado para GitHub.")


def restore_acidentes_from_github() -> None:
    """
    Baixa backups/acidentes.csv do GitHub e sobrescreve data/acidentes.csv local.
    """
    from pathlib import Path
    from .github_utils import download_bytes_from_github  # ajuste conforme seu projeto

    content = download_bytes_from_github("backups/acidentes.csv")

    # Garante que a pasta data existe
    csv_path = Path(ACCIDENTS_CSV_PATH)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    with csv_path.open("wb") as f:
        f.write(content)

    print("[restore_acidentes_from_github] Arquivo de acidentes restaurado a partir do GitHub.")


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
    'João Paulo 3C',
    'HENRIQUE 3A SB/2CSKI',
    'MARIA 1BSK',
    'JOÃO 2CSB'

    Regra:
    - Procura todos os padrões [1-4][A-D], com ou sem espaço (2 B → 2B)
    - Funciona mesmo colado em SK/SB, ponto, +, etc. (1BSK, 2CSB, 3A., 4C+)
    - Se houver mais de um, escolhe o de maior ordem (1A..4D)
    """

    if not isinstance(nome_bruto, str):
        return "", None

    texto = nome_bruto.strip()
    if not texto:
        return "", None

    import re

    # aceita "2B", "2 B", "1BSK", "2CSB", "3A.", "4C+"
    pattern = r"([1-4]\s*[A-D])"
    matches = re.findall(pattern, texto.upper())

    if not matches:
        return texto, None

    # normaliza "2 B" -> "2B"
    matches = [m.replace(" ", "") for m in matches]

    # mantém só níveis válidos (1A..4D)
    matches = [m for m in matches if m in LEVEL_ORDER]
    if not matches:
        return texto, None

    # pega o maior nível
    best = max(matches, key=lambda x: LEVEL_ORDER.get(x, -1))

    # por enquanto deixamos o nome inteiro como "limpo"
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

    # Backup manual: agora é feito via botão na página 98_Restaurar_DB_de_Backup
    # (não fazemos mais backup automático aqui para evitar sobrescrever dados bons
    # com estados intermediários ou ambientes vazios)

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
    # Não fazemos mais backup automático aqui.
    # O snapshot diário continua sendo gravado na tabela daily_clients,
    # mas o backup completo do banco é feito manualmente via botão.
        
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

