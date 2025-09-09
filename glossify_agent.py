# glossify_agent.py
# - ner_results/ner_entities_*.csv 실시간 tail → 작업큐 적재
# - 워커 스레드: 각자 Foundry Thread 사용, Azure Agent 호출(재시도/타임아웃)
# - 결과 CSV 저장(락), 그리고 서버 /meeting/<MEETING_ID>/terms 로 REST POST
# - 콘솔 로그/파일 로그 선택(SILENT, LOG_TO_FILE)
# - 임포트 친화적: AgentService.start() 호출 전까지 부작용 없음

import os
import io
import re
import csv
import glob
import time
import json
import queue
import random
import threading
import logging
import requests
from logging.handlers import RotatingFileHandler
from collections import deque
from typing import Optional, Tuple

from dotenv import load_dotenv
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Azure AI Foundry SDK
from azure.identity import DefaultAzureCredential
from azure.ai.projects import AIProjectClient
from azure.ai.agents.models import ListSortOrder, MessageRole

load_dotenv()

# ---------------------- 환경설정 ----------------------
PROJECT_ENDPOINT      = os.getenv("PROJECT_ENDPOINT", "").strip()
MODEL_DEPLOYMENT_NAME = os.getenv("MODEL_DEPLOYMENT_NAME", "gpt-4o").strip()

NER_RESULTS_DIR   = os.getenv("NER_RESULTS_DIR", os.path.join(os.getcwd(), "ner_results"))
AGENT_RESULTS_DIR = os.getenv("AGENT_RESULTS_DIR", os.path.join(os.getcwd(), "agent_results"))
os.makedirs(NER_RESULTS_DIR, exist_ok=True)
os.makedirs(AGENT_RESULTS_DIR, exist_ok=True)

BACKEND_BASE_URL = (os.getenv("BACKEND_BASE_URL", "http://localhost:5000").rstrip("/"))
MEETING_ID       = os.getenv("MEETING_ID", "demo123")

START_FROM_BEGINNING = (os.getenv("START_FROM_BEGINNING", "false").lower() in {"1","true","y"})

# 카테고리/토큰 규칙
ALLOWED_CATS = {c.strip() for c in (os.getenv("ALLOWED_CATS",
                    "Person,PersonType,Organization,Event,Product,Skill").split(",")) if c.strip()}
MIN_TERM_TOKENS              = int(os.getenv("MIN_TERM_TOKENS", "2"))
DEDUP_IN_TIMESTAMP           = (os.getenv("DEDUP_IN_TIMESTAMP", "true").lower() == "true")
ALLOW_ONE_TOKEN_IF_CONF_GE   = float(os.getenv("ALLOW_ONE_TOKEN_IF_CONF_GE", "0.92"))
ALLOW_ACRONYM_LEN_LE         = int(os.getenv("ALLOW_ACRONYM_LEN_LE", "3"))

# 워커/큐/재시도/타임아웃
MAX_WORKERS            = int(os.getenv("MAX_WORKERS", "5")) # 5가 시스템 상 최대. 느리면 4도 ok
MAX_QUEUE              = int(os.getenv("MAX_QUEUE", "1000"))
AGENT_RETRY_MAX        = int(os.getenv("AGENT_RETRY_MAX", "3"))
AGENT_RETRY_BASE_SEC   = float(os.getenv("AGENT_RETRY_BASE_SEC", "0.8"))
AGENT_RUN_TIMEOUT_SEC  = float(os.getenv("AGENT_RUN_TIMEOUT_SEC", "25"))  # 1회 run 예산
AGENT_TOTAL_TIMEOUT_SEC= float(os.getenv("AGENT_TOTAL_TIMEOUT_SEC","60"))  # 재시도 포함 총 예산
HTTP_POST_CONNECT_TO   = float(os.getenv("HTTP_POST_CONNECT_TIMEOUT_SEC", "3"))
HTTP_POST_READ_TO      = float(os.getenv("HTTP_POST_READ_TIMEOUT_SEC", "7"))
REFEED_BATCH           = int(os.getenv("REFEED_BATCH", "256"))

# 로깅
SILENT      = (os.getenv("SILENT","0").lower() in {"1","true","y"})
LOG_TO_FILE = (os.getenv("LOG_TO_FILE","1").lower() in {"1","true","y"})
LOG_LEVEL   = os.getenv("LOG_LEVEL","INFO").upper()

logger = logging.getLogger("glossify_agent")
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
if LOG_TO_FILE:
    os.makedirs("logs", exist_ok=True)
    fh = RotatingFileHandler("logs/glossify_agent.log", maxBytes=2_000_000, backupCount=3, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(fh)
if not SILENT:
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(ch)

def _log_info(msg): logger.info(msg)
def _log_warn(msg): logger.warning(msg)
def _log_err(msg):  logger.error(msg)

# ---------------------- 유틸/파서 ----------------------
DOMAIN_PREFIX_RE = re.compile(r'^\s*(Finance|Logistics|EnterpriseIT)\s*(?:[.:]|—|–|-)?\s*(.*)\s*$', re.S)
CONTEXT_PREFIX_RE = re.compile(
    r'^\s*(여기서는|이\s*맥락(에서|에서는)?|현재\s*맥락(에서)?|본\s*맥락(에서)?|이\s*경우(에는)?|해당\s*(문맥|맥락)(에서)?)(\s|[,，])?',
    re.IGNORECASE
)
_SENT_ITER_RE = re.compile(r'[^.!?。！？…]+(?:[.!?。！？…]+|$)')

def newest_csv(dirpath: str) -> Optional[str]:
    paths = sorted(glob.glob(os.path.join(dirpath, "ner_entities_*.csv")),
                   key=os.path.getmtime, reverse=True)
    return paths[0] if paths else None

def parse_csv_line(line: str) -> Optional[dict]:
    f = io.StringIO(line)
    r = csv.reader(f)
    row = next(r, None)
    if not row or len(row) < 5:
        return None
    return {
        "timestamp":   (row[0] or "").strip(),
        "category":    (row[1] or "").strip(),
        "entity":      (row[2] or "").strip(),
        "confidence":  (row[3] or "").strip(),
        "source_text": row[4] if len(row) > 4 else ""
    }

def split_domain_and_body(text: str) -> Tuple[str, str]:
    if not text:
        return "", ""
    s = text.strip()
    m = DOMAIN_PREFIX_RE.match(s)
    if m:
        return (m.group(1), (m.group(2) or "").strip())
    parts = s.split(None, 1)
    if len(parts) == 1: return parts[0], ""
    return parts[0], parts[1].strip()

def split_sentences_with_spans(text: str):
    return [(m.group(0), m.start(), m.end())
            for m in _SENT_ITER_RE.finditer(text or "")
            if m.group(0).strip()]

def drop_trailing_context_sentence(body: str) -> Tuple[str, bool]:
    if not body:
        return body, False
    sents = split_sentences_with_spans(body)
    if not sents:
        return body, False
    last_txt, start, _ = sents[-1]
    if CONTEXT_PREFIX_RE.match(last_txt.strip()):
        return body[:start].rstrip(), True
    return body, False

# ---------------------- AgentService ----------------------
AGENT_STATE_PATH = os.getenv("AGENT_STATE_PATH", "foundry_agent.json")
AGENT_STATE_LOCK = threading.Lock()        # 파일 IO 락
AGENT_INIT_LOCK  = threading.Lock()        # 클라이언트/에이전트 초기화 단일화

def _load_agent_state(path: str = AGENT_STATE_PATH) -> dict:
    with AGENT_STATE_LOCK:
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            return {}
        except json.JSONDecodeError:
            _log_warn("[agent-state] JSON partial? retrying once")
            time.sleep(0.12)
            try:
                with open(path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                _log_warn(f"[agent-state] still invalid: {e}")
                return {}
        except Exception as e:
            _log_warn(f"[agent-state] load fail: {e}")
            return {}

def _save_agent_state(agent_id: str, name: str, endpoint: str, model: str, path: str = AGENT_STATE_PATH):
    tmp = path + ".tmp"
    payload = {
        "agent_id": agent_id,
        "agent_name": name,
        "project_endpoint": endpoint,
        "model_deployment": model,
    }
    with AGENT_STATE_LOCK:
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)  # 원자적 교체
        except Exception as e:
            _log_warn(f"[agent-state] save fail: {e}")


class AgentService:
    def __init__(self,
                 project_endpoint: str,
                 model_deployment: str,
                 backend_base_url: str,
                 meeting_id: str):

        if not project_endpoint or not model_deployment:
            raise RuntimeError("PROJECT_ENDPOINT / MODEL_DEPLOYMENT_NAME 필요")

        self.project_endpoint = project_endpoint
        self.model_deployment = model_deployment
        self.backend_base_url = backend_base_url.rstrip("/")
        self.meeting_id = meeting_id

        self.cred = None
        self.project_client: Optional[AIProjectClient] = None
        self.agent_id: Optional[str] = None

        self._q: "queue.Queue[dict]" = queue.Queue(MAX_QUEUE)
        self._overflow = deque(maxlen=5000)
        self._workers: list[threading.Thread] = []
        self._observer: Optional[Observer] = None
        self._stop_event = threading.Event()

        self.metrics = {
            "read": 0, "enq": 0, "overflow": 0,
            "filtered_empty_ent": 0, "filtered_dup": 0, "filtered_cat": 0,
            "filtered_conf": 0, "filtered_tokens": 0
        }

        # dedup in timestamp-group
        self._ts_lock = threading.Lock()
        self._last_ts = None
        self._seen_in_ts: set = set()

        # result csv
        self._write_lock = threading.Lock()
        ts = time.strftime("%Y%m%d_%H%M%S")
        self.explain_csv = os.path.join(AGENT_RESULTS_DIR, f"glossify_{ts}.csv")
        with open(self.explain_csv, "w", encoding="utf-8-sig", newline="") as f:
            csv.writer(f).writerow(["timestamp", "entity", "explanation", "domain"])
        _log_info(f"[ExplainLog] {self.explain_csv}")

        # thread-local for Foundry Thread id
        self._tls = threading.local()

        # --- 기존 에이전트 상태 재사용 (ENV 우선) ---
        state = _load_agent_state()
        env_agent_id = (os.getenv("AGENT_ID") or "").strip()

        if env_agent_id:
            self.agent_id = env_agent_id
            _log_info(f"🔁 Using existing agent from ENV AGENT_ID: {self.agent_id}")
        elif state.get("agent_id"):
            self.agent_id = state["agent_id"]
            _log_info(f"🔁 Using existing agent from {AGENT_STATE_PATH}: {self.agent_id}")
        else:
            # 여기서 바로 에러로 멈추게 해도 되고, _ensure_client_and_agent에서 한 번 더 체크해도 됨
            _log_err(
                "❌ No existing Agent ID found. "
                "Set AGENT_ID env or provide foundry_agent.json with {'agent_id': '...'}."
            )
            # 계속 진행하더라도 _ensure_client_and_agent에서 반드시 RuntimeError로 막힘


    # ---------- Azure Agent ----------
    def _ensure_client_and_agent(self):
        """기존 agent만 사용. 없거나 무효면 절대 생성하지 않고 에러."""
        # 1) Client 준비
        if not self.project_client:
            self.cred = DefaultAzureCredential()
            self.project_client = AIProjectClient(endpoint=self.project_endpoint, credential=self.cred)
            _log_info("✅ AIProjectClient ready")

        # 2) agent_id 필수
        if not self.agent_id:
            raise RuntimeError(
                "Agent ID is required but missing. "
                "Set AGENT_ID env or create foundry_agent.json with a valid 'agent_id'."
            )

        # 3) (가볍게) 유효성 점검: SDK에 get_agent가 있으면 호출해봄
        try:
            if hasattr(self.project_client.agents, "get_agent"):
                _ = self.project_client.agents.get_agent(self.agent_id)
            _log_info(f"✅ Using existing Agent: {self.agent_id}")
        except Exception as e:
            # 절대 새로 만들지 않음
            raise RuntimeError(
                f"Configured AGENT_ID seems invalid or inaccessible: {self.agent_id} ({e})"
            ) from e


    def _get_worker_thread_id(self) -> str:
        tid = getattr(self._tls, "thread_id", None)
        if tid: return tid
        th = self.project_client.agents.threads.create()
        self._tls.thread_id = th.id
        _log_info(f"🧵 Worker {threading.current_thread().name} uses Thread: {th.id}")
        return th.id

    def _get_last_agent_text(self, thread_id: str) -> Optional[str]:
        try:
            last_txt = self.project_client.agents.messages.get_last_message_text_by_role(
                thread_id=thread_id, role=MessageRole.AGENT
            )
            if last_txt and getattr(last_txt, "value", None):
                return last_txt.value.strip()
        except Exception:
            pass
        try:
            msgs = self.project_client.agents.messages.list(thread_id=thread_id,
                                                            order=ListSortOrder.DESCENDING, limit=20)
            for m in msgs:
                role = getattr(m, "role", None)
                if (getattr(role, "value", role) or "").lower() in ("assistant","agent"):
                    for c in getattr(m, "content", []) or []:
                        text = getattr(getattr(c, "text", None), "value", None)
                        if text and text.strip():
                            return text.strip()
        except Exception:
            pass
        return None

    def _explain_with_agent(self, term: str, category: str, context: str) -> str:
        self._ensure_client_and_agent()
        thread_id = self._get_worker_thread_id()

        start_overall = time.time()
        attempt = 0
        recreated_once = False

        while True:
            attempt += 1
            try:
                remain = AGENT_TOTAL_TIMEOUT_SEC - (time.time() - start_overall)
                if remain <= 0:
                    raise TimeoutError("agent overall timeout")

                self.project_client.agents.messages.create(
                    thread_id=thread_id, role="user",
                    content=f"term: {term};\ncategory: {category};\nsource_text: {context}"
                )
                t0 = time.time()
                self.project_client.agents.runs.create_and_process(
                    thread_id=thread_id, agent_id=self.agent_id
                )
                if (time.time() - t0) > AGENT_RUN_TIMEOUT_SEC:
                    raise TimeoutError("agent run timeout")

                text = self._get_last_agent_text(thread_id)
                return (text or "__SKIP__").strip()

            except Exception as e:
                msg = str(e).lower()

                # ID가 무효/권한 문제 → 절대 재생성하지 않고 즉시 중단
                if any(x in msg for x in ["not found", "does not exist", "invalid agent", "unauthorized"]):
                    _log_err(
                        "❌ Configured AGENT_ID is invalid or unauthorized. "
                        "Refusing to create a new one. Fix AGENT_ID or foundry_agent.json."
                    )
                    raise

                # 그 외(네트워크/일시적 5xx 등)는 제한적 재시도
                if attempt >= AGENT_RETRY_MAX:
                    raise
                backoff = AGENT_RETRY_BASE_SEC * (2 ** (attempt - 1)) * (1.0 + random.random()*0.2)
                _log_warn(f"[Retry {attempt}/{AGENT_RETRY_MAX}] agent call failed: {e} → sleep {backoff:.2f}s")
                time.sleep(backoff)

    # ---------- 결과 저장/전송 ----------
    def _append_explain_row(self, ts: str, ent: str, explanation: str, domain: str):
        with self._write_lock:
            with open(self.explain_csv, "a", encoding="utf-8-sig", newline="") as f:
                csv.writer(f).writerow([ts, ent, explanation, domain])
                f.flush()

    def _post_term_to_server(self, ts: str, ent: str, domain: str, body: str):
        url = f"{self.backend_base_url}/meeting/{self.meeting_id}/terms"
        payload = {"timestamp": ts, "entity": ent, "domain": domain or "-", "body": body}
        r = requests.post(url, json=payload, timeout=(HTTP_POST_CONNECT_TO, HTTP_POST_READ_TO))

        if r.ok:
            _log_info(f"[Glossify] Term posted successfully: {payload}")
        else:
            _log_warn(f"[Glossify] Failed to post term: {payload}")
            _log_err(f"[Glossify] Error details: {r.text}")
            
        r.raise_for_status()

    # ---------- 필터/적재 ----------
    def _reset_timestamp_group(self):
        with self._ts_lock:
            self._last_ts = None
            self._seen_in_ts.clear()

    def _pass_filters(self, item: dict) -> bool:
        ts, cat, ent, conf, src = (
            item["timestamp"], item["category"], item["entity"],
            float(item["confidence"]), (item["source_text"] or "")
        )
        if not ent:
            self.metrics["filtered_empty_ent"] += 1
            return False

        if DEDUP_IN_TIMESTAMP:
            with self._ts_lock:
                if ts != self._last_ts:
                    self._last_ts = ts
                    self._seen_in_ts.clear()
                key = (cat, ent, src)
                if key in self._seen_in_ts:
                    self.metrics["filtered_dup"] += 1
                    return False
                self._seen_in_ts.add(key)

        if cat not in ALLOWED_CATS:
            self.metrics["filtered_cat"] += 1
            return False
        if conf < 0.5:
            self.metrics["filtered_conf"] += 1
            return False

        toks = len(ent.replace('-', ' ').replace('/', ' ').split())
        if toks < MIN_TERM_TOKENS:
            allow_one = (conf >= ALLOW_ONE_TOKEN_IF_CONF_GE) or \
                        (ent.isupper() and len(ent) <= 6) or \
                        (len(ent) <= ALLOW_ACRONYM_LEN_LE)
            if not allow_one:
                self.metrics["filtered_tokens"] += 1
                return False
        return True

    def _enqueue_if_pass(self, item: dict):
        self.metrics["read"] += 1
        if not self._pass_filters(item):
            return
        task = {
            "timestamp": item["timestamp"],
            "category": item["category"],
            "entity": item["entity"],
            "confidence": float(item["confidence"]),
            "source_text": item["source_text"] or ""
        }
        try:
            self._q.put_nowait(task)
            self.metrics["enq"] += 1
        except queue.Full:
            self._overflow.append(task)
            self.metrics["overflow"] += 1

    def _refeed_overflow(self):
        n = 0
        while self._overflow and n < REFEED_BATCH:
            try:
                self._q.put_nowait(self._overflow.popleft())
                n += 1
                self.metrics["enq"] += 1
            except queue.Full:
                break

    # ---------- CSV tail ----------
    def _read_complete_csv_record(self, f) -> Optional[str]:
        start_pos = f.tell()
        buf = f.readline()
        if not buf:
            return None
        while buf.count('"') % 2 == 1:
            more = f.readline()
            if not more:
                f.seek(start_pos)
                return None
            buf += more
        while True:
            try:
                test = next(csv.reader(io.StringIO(buf)))
                if len(test) >= 5:
                    return buf
            except (StopIteration, csv.Error):
                pass
            more = f.readline()
            if not more:
                f.seek(start_pos)
                return None
            buf += more

    class _CsvTailHandler(FileSystemEventHandler):
        def __init__(self, service: "AgentService", dirpath: str, pattern="ner_entities_"):
            self.svc = service
            self.dir = dirpath
            self.pattern = pattern
            self.active_path = newest_csv(self.dir)
            self.f = None
            if self.active_path:
                self._open_active(self.active_path)

        def _open_active(self, path):
            if self.f:
                try: self.f.close()
                except: pass
            self.active_path = path
            self.f = open(self.active_path, "r", encoding="utf-8-sig", newline="")
            self.f.readline()  # skip header
            if not START_FROM_BEGINNING:
                self.f.seek(0, os.SEEK_END)
            _log_info(f"[Watcher] Active → {path} (from_beginning={START_FROM_BEGINNING})")
            self.svc._reset_timestamp_group()

        def _switch_to_latest(self):
            latest = newest_csv(self.dir)
            if latest and (self.active_path is None or os.path.abspath(latest) != os.path.abspath(self.active_path)):
                self._open_active(latest)

        def _drain(self):
            if not self.f:
                return
            while True:
                pos = self.f.tell()
                line = self.svc._read_complete_csv_record(self.f)
                if line is None:
                    self.f.seek(pos)
                    break
                item = parse_csv_line(line)
                if not item: 
                    continue
                self.svc._enqueue_if_pass(item)

        def on_created(self, event):
            if event.is_directory:
                return
            if self.pattern in os.path.basename(event.src_path):
                self._switch_to_latest()

        def on_modified(self, event):
            if event.is_directory or not self.active_path:
                return
            if os.path.abspath(event.src_path) == os.path.abspath(self.active_path):
                self._drain()

    # ---------- 워커 ----------
    def _worker_loop(self, idx: int):
        # ensure per-worker Foundry Thread
        self._ensure_client_and_agent()
        self._get_worker_thread_id()

        while not self._stop_event.is_set():
            try:
                item = self._q.get(timeout=0.2)
            except queue.Empty:
                # backpressure 완화
                if self._q.qsize() < max(1, MAX_QUEUE//2) and self._overflow:
                    self._refeed_overflow()
                continue

            try:
                ts  = item["timestamp"]
                cat = item["category"]
                ent = item["entity"]
                src = item["source_text"]

                raw = self._explain_with_agent(ent, cat, src)
                if raw == "__SKIP__":
                    _log_info(f"SKIP  [{idx}] {ent}")
                    continue

                domain, body = split_domain_and_body(raw)
                if not body:
                    _log_info(f"SKIP  [{idx}] {ent} (no body, domain='{domain or '-'}')")
                    continue

                # 프론트로 전달 (REST)
                try:
                    self._post_term_to_server(ts, ent, domain or "-", body)
                except Exception as e:
                    _log_warn(f"[POST terms] fail: {e}")

                # 저장(마지막 문맥문장 제거본)
                cosmos_body, removed = drop_trailing_context_sentence(body)
                preview = (cosmos_body[:60] + "…") if len(cosmos_body) > 60 else cosmos_body
                _log_info(f"WRITE [{idx}] {ent} (domain={domain or '-'}, ctx-removed={removed}) → {preview}")
                self._append_explain_row(ts, ent, cosmos_body, domain)

            except Exception as e:
                _log_err(f"ERR   [worker {idx}] {e}")
            finally:
                self._q.task_done()
                if self._q.qsize() < max(1, MAX_QUEUE//2) and self._overflow:
                    self._refeed_overflow()

    # ---------- 시작/정지 ----------
    def start(self):
        """watchdog + workers 시작 (동일 프로세스 내 백그라운드 실행)"""
        if self._observer:
            return

        # 워커
        for i in range(MAX_WORKERS):
            t = threading.Thread(target=self._worker_loop, args=(i+1,), name=f"worker-{i+1}", daemon=True)
            t.start()
            self._workers.append(t)
        _log_info(f"🚀 Workers: {MAX_WORKERS} (queue max={MAX_QUEUE})")

        # tail
        handler = self._CsvTailHandler(self, NER_RESULTS_DIR, pattern="ner_entities_")
        self._observer = Observer()
        self._observer.schedule(handler, NER_RESULTS_DIR, recursive=False)
        self._observer.start()
        _log_info(f"[Glossify] Watching: {NER_RESULTS_DIR}")

        # 메트릭 루프(백그라운드)
        threading.Thread(target=self._metrics_loop, name="metrics", daemon=True).start()

    def _metrics_loop(self):
        last = time.time()
        while not self._stop_event.is_set():
            time.sleep(0.2)
            if time.time() - last >= 2.0:
                _log_info(
                    f"[METRICS] read={self.metrics['read']} enq={self.metrics['enq']} "
                    f"overflow={self.metrics['overflow']} qsize={self._q.qsize()} of={len(self._overflow)} "
                    f"filtered(cat={self.metrics['filtered_cat']}, conf={self.metrics['filtered_conf']}, "
                    f"tokens={self.metrics['filtered_tokens']}, dup={self.metrics['filtered_dup']}, "
                    f"empty={self.metrics['filtered_empty_ent']})"
                )
                last = time.time()

    def stop(self):
        self._stop_event.set()
        if self._observer:
            self._observer.stop()
            self._observer.join()
            self._observer = None
        # 큐 drain 기다림
        try:
            self._q.join()
        except Exception:
            pass

# 편의 함수: 서버에서 쉽게 호출
def start_agent_in_background(meeting_id: Optional[str] = None) -> AgentService:
    svc = AgentService(
        project_endpoint=PROJECT_ENDPOINT,
        model_deployment=MODEL_DEPLOYMENT_NAME,
        backend_base_url=BACKEND_BASE_URL,
        meeting_id=meeting_id or MEETING_ID
    )
    print(f"[Glossify] Starting agent (backend_base_url={BACKEND_BASE_URL}, meeting_id={meeting_id})")
    svc.start()
    return svc

if __name__ == "__main__":
    # 독립 실행도 가능
    service = start_agent_in_background(MEETING_ID)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        service.stop()
