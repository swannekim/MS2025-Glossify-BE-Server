# cosmos_terms.py
# Import-friendly Cosmos upsert helpers for Glossify CSV (server.py 호환)
import os, csv, glob, uuid, unicodedata
from typing import Dict, Tuple, Optional
from dataclasses import dataclass

from dotenv import load_dotenv
import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_values, register_uuid

load_dotenv()

# ---------------- DB Config ----------------
@dataclass
class DBConfig:
    host: str
    name: str
    user: str
    password: str
    sslmode: str = "require"

    @classmethod
    def from_env(cls) -> "DBConfig":
        return cls(
            host=os.getenv("DB_HOST", ""),
            name=os.getenv("DB_NAME", ""),
            user=os.getenv("DB_USER", ""),
            password=os.getenv("DB_PASSWORD", ""),
            sslmode=os.getenv("DB_SSLMODE", "require"),
        )

def _build_conn_string(cfg: DBConfig) -> str:
    return f"host={cfg.host} user={cfg.user} dbname={cfg.name} password={cfg.password} sslmode={cfg.sslmode}"

# ---------------- CSV Utils ----------------
def newest_glossify_csv(dirpath: str) -> Optional[str]:
    """dirpath/glossify_*.csv 중 가장 최근 파일 경로 반환"""
    paths = sorted(glob.glob(os.path.join(dirpath, "glossify_*.csv")), key=os.path.getmtime, reverse=True)
    return paths[0] if paths else None

def canonicalize_term(term: str) -> str:
    return unicodedata.normalize("NFKC", (term or "")).strip().lower()

def term_to_uuid(term: str) -> uuid.UUID:
    return uuid.uuid5(uuid.NAMESPACE_URL, f"glossify-term:{canonicalize_term(term)}")

def _pick_explanation(row: dict) -> str:
    """
    서버/에이전트 버전에 따라 'explanation' 또는 'body' 로 올 수 있으니 둘 다 지원.
    (우선순위: explanation > body)
    """
    val = (row.get("explanation") or "").strip()
    if not val:
        val = (row.get("body") or "").strip()
    return val

def load_latest_rows(csv_path: str) -> Dict[Tuple[uuid.UUID, str], Tuple[str, str]]:
    """
    key=(termid, domain), value=(Term, Explanation).
    같은 키가 여러 번 나오면 '마지막 것'만 남긴다.
    CSV 헤더는 다음을 기대:
      - entity (필수, term)
      - domain (필수)
      - explanation 또는 body (필수)
    """
    latest: Dict[Tuple[uuid.UUID, str], Tuple[str, str]] = {}
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            term = (row.get("entity") or "").strip()
            domain = (row.get("domain") or "").strip()
            explanation = _pick_explanation(row)
            if not term or not domain or not explanation:
                continue
            tid = term_to_uuid(term)
            latest[(tid, domain)] = (term, explanation)
    return latest

# ---------------- DB Primitives ----------------
def ensure_table_and_indexes(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS term (
                termid      uuid        NOT NULL,
                term        text        NOT NULL,
                explanation text        NOT NULL,
                domain      text        NOT NULL,
                PRIMARY KEY (termid, domain)
            );
        """)
        cur.execute("""CREATE INDEX IF NOT EXISTS idx_term_term_domain ON term(term, domain);""")
        conn.commit()
        # (옵션) Citus 분산 테이블 설정 – 권한/엔진 없으면 조용히 무시
        try:
            cur.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_dist_partition WHERE logicalrelid = 'term'::regclass
                    ) THEN
                        PERFORM create_distributed_table('term', 'termid');
                    END IF;
                END
                $$;
            """)
            conn.commit()
        except Exception:
            conn.rollback()

def upsert_terms(conn, rows: Dict[Tuple[uuid.UUID, str], Tuple[str, str]]) -> int:
    if not rows:
        return 0
    values = [(tid, term, expl, domain) for (tid, domain), (term, expl) in rows.items()]
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO term (termid, term, explanation, domain)
            VALUES %s
            ON CONFLICT (termid, domain) DO UPDATE
              SET explanation = EXCLUDED.explanation,
                  term        = EXCLUDED.term
        """, values, page_size=500)
    conn.commit()
    return len(values)

# ---------------- High-level API (for server.py) ----------------
class CosmosTermStore:
    """Reusable store with connection pool (thread-safe)."""
    def __init__(self, cfg: Optional[DBConfig] = None, minconn: int = 1, maxconn: int = 10):
        self.cfg = cfg or DBConfig.from_env()
        self.pool: Optional[pool.SimpleConnectionPool] = None
        self.minconn = minconn
        self.maxconn = maxconn

    def start(self):
        if self.pool:
            return
        register_uuid()  # global (idempotent)
        # psycopg2 풀 생성
        self.pool = psycopg2.pool.SimpleConnectionPool(self.minconn, self.maxconn, _build_conn_string(self.cfg))
        # 테이블/인덱스는 한 번만 보장해두면 안전
        conn = self.pool.getconn()
        try:
            register_uuid(conn_or_curs=conn)  # idempotent
            ensure_table_and_indexes(conn)
        finally:
            # 중요: 풀 커넥션은 절대 close() 하지 말고 putconn() 만!
            self.pool.putconn(conn)

    def close(self):
        if self.pool:
            try:
                self.pool.closeall()
            finally:
                self.pool = None

    def upsert_from_csv(self, csv_path: str) -> int:
        """Load CSV and upsert. Returns upserted row count."""
        if not self.pool:
            self.start()
        conn = self.pool.getconn()
        try:
            register_uuid(conn_or_curs=conn)  # idempotent
            rows = load_latest_rows(csv_path)
            return upsert_terms(conn, rows)
        finally:
            # 중요: close() 하지 말고 putconn() 만!
            self.pool.putconn(conn)

# ---------------- Optional CLI ----------------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Upsert Glossify CSV into Cosmos DB for PostgreSQL")
    parser.add_argument("--csv", "-c", help="Path to glossify_*.csv")
    parser.add_argument("--dir", "-d", help="Directory for glossify_*.csv (default: $AGENT_RESULTS_DIR or ./agent_results)")
    args = parser.parse_args()

    base_dir = args.dir or os.getenv("AGENT_RESULTS_DIR", os.path.join(os.getcwd(), "agent_results"))
    csv_path = args.csv or newest_glossify_csv(base_dir)
    if not csv_path:
        raise SystemExit(f"No CSV found. Try --csv or create {base_dir}/glossify_*.csv")

    store = CosmosTermStore()
    store.start()
    try:
        n = store.upsert_from_csv(csv_path)
        print(f"✅ Upserted rows: {n} from {csv_path}")
    finally:
        store.close()
