# term_viewer.py
# streamlit run term_viewer.py

# app.py
import os
from typing import List, Tuple
import streamlit as st
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import pandas as pd

# ----------------------------------
# Env & Streamlit page config
# ----------------------------------
load_dotenv()  # reads .env when present

st.set_page_config(page_title="Glossify Terms Viewer", layout="wide")
st.title("📚 Glossify Terms Viewer")

# ----------------------------------
# Connection (SSL required on Cosmos DB for PostgreSQL)
# ----------------------------------
def _conn_params():
    return dict(
        host=os.getenv("DB_HOST", ""),
        dbname=os.getenv("DB_NAME", ""),
        user=os.getenv("DB_USER", ""),
        password=os.getenv("DB_PASSWORD", ""),
        sslmode=os.getenv("DB_SSLMODE", "require"),  # Cosmos requires TLS
        connect_timeout=10,
    )

@st.cache_resource(show_spinner=False)
def get_conn():
    p = _conn_params()
    if not all([p["host"], p["dbname"], p["user"], p["password"]]):
        raise RuntimeError("환경변수(DB_HOST, DB_NAME, DB_USER, DB_PASSWORD)가 비어 있습니다.")
    conn = psycopg2.connect(**p)
    conn.autocommit = True
    return conn

def fetch_domains(conn) -> List[str]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT DISTINCT domain FROM term ORDER BY domain;")
        rows = cur.fetchall()
        return [r["domain"] for r in rows if r["domain"]]

def fetch_terms(conn, q_term: str, domains: List[str], limit: int = 300) -> pd.DataFrame:
    where = []
    params: List = []

    # term 검색 (term 컬럼만 검색; 필요시 explanation도 추가 가능)
    if q_term:
        where.append("term ILIKE %s")
        params.append(f"%{q_term}%")

    # domain 필터
    if domains:
        placeholders = ",".join(["%s"] * len(domains))
        where.append(f"domain IN ({placeholders})")
        params.extend(domains)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    # '최근 입력'은 ctid DESC로 정렬 (물리 순서 기반, 간단한 최근값 확인 용)
    sql = f"""
        SELECT termid, term, domain, explanation
        FROM term
        {where_sql}
        ORDER BY ctid DESC
        LIMIT %s;
    """
    params.append(limit)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
        return pd.DataFrame(rows)

# ----------------------------------
# Sidebar controls
# ----------------------------------
with st.sidebar:
    st.header("Filters")
    try:
        domains = fetch_domains(get_conn())
    except Exception as e:
        st.error(f"DB 연결 실패: {e}")
        st.stop()

    q_term = st.text_input("🔎 Search term", placeholder="예: CPK, ERP, ...")
    sel_domains = st.multiselect("🏷️ Domain", options=domains)
    limit = st.slider("표시 개수 (최근순)", min_value=50, max_value=2000, value=300, step=50)
    st.caption("※ 아무것도 선택하지 않아도 최근 등록순으로 자동 표시")

# ----------------------------------
# Main table
# ----------------------------------
try:
    df = fetch_terms(get_conn(), q_term=q_term.strip(), domains=sel_domains, limit=limit)
    st.success(f"총 {len(df)}건")
    if len(df) > 0:
        # 보기 편하게 컬럼 순서 조정
        cols = [c for c in ["term", "domain", "explanation", "termid"] if c in df.columns]
        df = df[cols]
        st.dataframe(df, width='stretch', height=600)
    else:
        st.info("결과가 없습니다. 검색어나 도메인을 조정해보세요.")
except Exception as e:
    st.error(f"조회 중 오류: {e}")
