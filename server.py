# server.py
import os, importlib
import sys, io, json
from datetime import datetime, timezone
from collections import defaultdict, deque

from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_socketio import SocketIO, join_room, leave_room, emit  # 프론트 push용

from ner_core import (
    analyze_ner,
    init_ner_log,
    init_stt_log,
    append_ner_rows,
    append_stt_line,
    print_ner,
)

from glossify_agent import start_agent_in_background

import threading
from cosmos_terms import CosmosTermStore, newest_glossify_csv

# ----------------- Flask & Socket.IO -----------------
app = Flask(__name__)
CORS(app, resources={r"*": {"origins": "*"}})

def _pick_async_mode() -> str:
    pref = (os.getenv("SOCKETIO_ASYNC_MODE") or "").strip().lower()
    valid = {"threading", "eventlet", "gevent", "gevent_uwsgi"}
    if pref in {"eventlet", "gevent", "gevent_uwsgi"}:
        # 요청 모드가 필요한 패키지가 있나 체크
        try:
            importlib.import_module("eventlet" if pref == "eventlet" else "gevent")
            return pref
        except Exception:
            print(f"[socketio] async_mode='{pref}' requested but package missing → fallback to 'threading'")
    elif pref in valid:
        return pref
    # 지정 안 했거나 이상한 값이면 안전하게 threading
    # Azure VM에서 서버 구동 시 gevent 사용 권장
    return "gevent"

ASYNC_MODE = _pick_async_mode()
sio = SocketIO(app, cors_allowed_origins="*", async_mode=ASYNC_MODE)
print(f"[socketio] using async_mode = {sio.async_mode}")

# sio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

@app.get("/")
def home():
    return jsonify({
        "message": "Hello from Glossify backend!",
        "time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    })

# ----------------- Helper: WS broadcast -----------------
def broadcast_to_meeting(meeting_id: str, payload: dict) -> bool:
    """특정 meeting_id 룸으로 payload 브로드캐스트"""
    try:
        sio.emit("terms", payload, to=meeting_id)
        return True
    except Exception as e:
        print(f"[WS] broadcast error: {e}")
        return False

# ----------------- ENV toggles -----------------
# 옵션: partial(임시 인식)에도 NER 수행할지
RUN_NER_ON_PARTIAL = (os.getenv("RUN_NER_ON_PARTIAL") or "0").lower() in {"1", "true", "y"}

# ----------------- Logs init -----------------
# 초기 로그 파일 준비 (reloader 중복 생성 방지하려면 app.run(use_reloader=False) 권장)
init_ner_log()
init_stt_log()

# ----------------- Per-meeting state -----------------
# 중복 최종문 방지(회의별 LRU)
LAST_FINAL = defaultdict(lambda: deque(maxlen=32))

_AGENTS: dict[str, object] = {}                      # meeting_id -> AgentService
_AGENTS_LOCK = threading.Lock()

#_agent_handle = None  # 백그라운드 에이전트 핸들(중복 기동 방지)

# helper functions
def _as_bool(v, default=False):
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    if isinstance(v, str):
        return v.strip().lower() in {"1", "true", "t", "y", "yes"}
    return default

def _now_iso_z():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def _read_payload():
    """
    JSON 우선, 실패 시 raw(JSON 재시도) -> form 순으로 파싱
    """
    data = request.get_json(silent=True)
    if isinstance(data, dict) and data:
        return data

    raw = request.get_data(cache=False, as_text=True)
    # 디버그용: 실제 들어온 원문을 1회 확인해보고 싶을 때 주석 해제
    # print(f"[DEBUG] Content-Type={request.headers.get('Content-Type')} raw={raw!r}")

    if raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass

    if request.form:
        return request.form.to_dict(flat=True)

    return {}

def _ensure_agent_for(meeting_id: str):
    """요청 path의 meeting_id로 AgentService를 meeting별 1개만 기동."""
    with _AGENTS_LOCK:
        svc = _AGENTS.get(meeting_id)
        if svc:
            return svc
        # meeting_id를 AgentService에 바인딩해서, 에이전트의 REST POST가 항상
        # /meeting/<meeting_id>/terms 로 가도록 보장
        svc = start_agent_in_background(meeting_id=meeting_id)
        _AGENTS[meeting_id] = svc
        print(f"[server] Agent started for meeting '{meeting_id}' (csv={getattr(svc, 'explain_csv', None)})")
        return svc

# -------------------------------------------------
# 서버 시작: 로그 파일 초기화 → 에이전트 백그라운드 시작
# -------------------------------------------------

# if _agent_handle is None:
#     # MEETING_ID는 기본값으로 'demo123' 사용. 실제로는 STT 생산자와 동일한 meeting_id를 쓰세요.
#     default_meeting = os.getenv("MEETING_ID", "demo123")
#     _agent_handle = start_agent_in_background(meeting_id=default_meeting)
#     print("[server] glossify agent started (after NER init)")

# ------------------- REST -------------------

@app.get("/health")
def health():
    return jsonify({"status": "ok"})

# ------------------- Agent lifecycle (optional) -------------------
@app.post("/meeting/<meeting_id>/start")
def start_agent(meeting_id: str):
    """명시적으로 특정 meeting의 Agent를 시작하고 상태를 반환(선택)."""
    svc = _ensure_agent_for(meeting_id)
    return jsonify({
        "status": "ok",
        "meeting_id": meeting_id,
        "csv_path": getattr(svc, "explain_csv", None)
    })

@app.post("/meeting/<meeting_id>/stt")
def receive_stt(meeting_id: str):
    """
    외부 STT 모듈이 계속 호출하는 엔드포인트.
    요청 바디 예:
    {
      "text": "문장...",
      "is_final": true,          # 기본 true
      "timestamp": "ISO8601",    # 생략시 서버 시각
      "speaker": "A"             # 선택
    }
    응답은 항상 최소 ACK만 반환: {"status":"ok"}
    """
    # meeting_id로 Agent가 바인딩되도록 보장
    _ensure_agent_for(meeting_id)

    data = _read_payload()
    text = (data.get("text") or "").strip()
    is_final = _as_bool(data.get("is_final", True), default=True)
    ts = data.get("timestamp") or _now_iso_z()
    # speaker, seq 등은 현재 로깅/처리 안 함 (원하면 추가)

    if not text:
        return jsonify({"error": "text required"}), 400

    # STT 라인 로그 (모든 partial/final 공통)
    append_stt_line(text, ts)

    if not is_final and not RUN_NER_ON_PARTIAL:
        print(f"[STT][partial][{meeting_id}] {text}")
        return jsonify({"status": "ok", "skipped_ner": True})

    # 최종문 중복 방지
    if is_final:
        if text in LAST_FINAL[meeting_id]:
            print(f"[STT][final][{meeting_id}] (dup) {text}")
            return jsonify({"status": "ok", "duplicate_final": True})
        LAST_FINAL[meeting_id].append(text)

    # NER 수행 → CSV 누적 + 콘솔 출력
    try:
        print(f"[STT][{'final' if is_final else 'partial'}][{meeting_id}] {text}")
        entities, grouped = analyze_ner(text)
        append_ner_rows(entities, text, ts)
        # print_ner(grouped)  # 필요 시 콘솔에 요약 찍기
        print("-" * 60, flush=True)
    except Exception as e:
        # 실패해도 외부 STT 모듈엔 ACK만 (파이프라인 끊기지 않도록)
        print(f"[NER ERROR] {e}")

    return jsonify({"status": "ok"})


# ------------------- Agent -> server (terms) -------------------
# 외부 프로세스가 에이전트 결과를 REST로 보내고 싶을 때 호환용
@app.post("/meeting/<meeting_id>/terms")
def receive_terms(meeting_id: str):
    data = _read_payload() or {}
    items = data.get("items")
    if not items: # 단건도 허용
        maybe = {k: data.get(k) for k in ("timestamp", "entity", "domain", "body")}
        if any(maybe.values()):
            items = [maybe]
    if not items or not isinstance(items, list):
        return jsonify({"error": "items or single term payload required"}), 400

    # timestamp 기본값 / 필수 필드 보정
    out = []
    for it in items:
        ts = (it.get("timestamp") or _now_iso_z())
        ent = (it.get("entity") or "").strip()
        body = (it.get("body") or "").strip()
        dom = (it.get("domain") or "-").strip()
        if not ent or not body:
            continue
        out.append({"timestamp": ts, "entity": ent, "domain": dom, "body": body})

    if not out:
        return jsonify({"error": "no valid items"}), 400

    # WebSocket(room=meeting_id)으로 브로드캐스트
    # sio.emit("terms", {"type": "terms", "meeting_id": meeting_id, "items": out}, to=meeting_id)
    # return jsonify({"status": "ok", "count": len(out)})
    payload = {"type": "terms", "meeting_id": meeting_id, "items": out}
    ok = broadcast_to_meeting(meeting_id, payload)

    return jsonify({"status": "ok", "count": len(out), "broadcasted": ok})

# ------------------- Stop & Cosmos upsert -------------------
# ===== Cosmos upsert wiring =====
_STOP_STATUS = {}           # meeting_id -> dict(status, csv_path, upserted, error, started_at, ended_at)
_STOP_LOCK = threading.Lock()
_term_store = None          # lazy CosmosTermStore

def _ensure_store():
    global _term_store
    if _term_store is None:
        _term_store = CosmosTermStore()   # env에서 DB 구성 읽음
        _term_store.start()
    return _term_store

def _pick_csv_for_meeting(meeting_id: str) -> str | None:
    """해당 미팅의 Agent가 생성한 glossify CSV(우선), 없으면 디렉토리 최신."""
    try:
        with _AGENTS_LOCK:
            svc = _AGENTS.get(meeting_id)
        if svc and getattr(svc, "explain_csv", None):
            return svc.explain_csv
    except Exception:
        pass
    base = os.getenv("AGENT_RESULTS_DIR", os.path.join(os.getcwd(), "agent_results"))
    return newest_glossify_csv(base)

def _set_stop_status(meeting_id: str, **kw):
    with _STOP_LOCK:
        _STOP_STATUS[meeting_id] = {**(_STOP_STATUS.get(meeting_id) or {}), **kw}

@app.post("/meeting/<meeting_id>/stop")
def stop_and_upsert(meeting_id: str):
    """
    Front(Teams) 'Stop' → Glossify CSV를 Cosmos에 upsert.
    body(optional): {"csv_path": "..."} 명시 우선. 없으면 에이전트/디렉토리 최신 사용.
    응답은 즉시 ACK, 실제 upsert는 백그라운드 실행.
    완료/에러 결과는 WebSocket 'cosmos_upsert_done' / 'cosmos_upsert_error' 로 room에 브로드캐스트.
    """
    data = _read_payload() or {}
    csv_path = (data.get("csv_path") or "").strip() or _pick_csv_for_meeting(meeting_id)
    if not csv_path or not os.path.exists(csv_path):
        return jsonify({"error": f"csv not found", "csv_path": csv_path}), 404

    _set_stop_status(meeting_id,
                     status="running",
                     csv_path=csv_path,
                     upserted=0,
                     error=None,
                     started_at=_now_iso_z(),
                     ended_at=None)

    def _worker():
        try:
            store = _ensure_store()
            n = store.upsert_from_csv(csv_path)
            _set_stop_status(meeting_id, status="done", upserted=n, ended_at=_now_iso_z())
            # WebSocket notify
            sio.emit("cosmos_upsert_done", {
                "meeting_id": meeting_id,
                "csv_path": csv_path,
                "upserted": n,
            }, to=meeting_id)
            print(f"[COSMOS][{meeting_id}] upsert done: {n} rows from {csv_path}")
        except Exception as e:
            _set_stop_status(meeting_id, status="error", error=str(e), ended_at=_now_iso_z())
            sio.emit("cosmos_upsert_error", {
                "meeting_id": meeting_id,
                "csv_path": csv_path,
                "error": str(e),
            }, to=meeting_id)
            print(f"[COSMOS][{meeting_id}] upsert error: {e}")

    threading.Thread(target=_worker, name=f"cosmos-upsert-{meeting_id}", daemon=True).start()
    return jsonify({"status": "accepted", "csv_path": csv_path})

@app.get("/meeting/<meeting_id>/stop/status")
def stop_status(meeting_id: str):
    return jsonify(_STOP_STATUS.get(meeting_id) or {"status": "idle"})

# 프론트(Teams Side Panel)에서 호출 예시
# Stop 버튼 클릭 →
# POST /meeting/<mid>/stop (body 비우면 최신 CSV로 실행)
# 완료 이벤트 받기(WebSocket, 방 <mid>):
# 성공: 이벤트명 cosmos_upsert_done → { meeting_id, csv_path, upserted }
# 실패: 이벤트명 cosmos_upsert_error → { meeting_id, csv_path, error }
# (optional) 폴링: GET /meeting/<mid>/stop/status

# ---------------- WebSocket ----------------
@sio.on("join")
def ws_join(data):
    meeting_id = (data or {}).get("meeting_id")
    user_id = (data or {}).get("user_id")
    user_name = (data or {}).get("user_name")
    if not meeting_id:
        emit("error", {"message": "meeting_id required"})
        return
    join_room(meeting_id)
    emit("ack", {"message": "joined", "meeting_id": meeting_id, "user_id": user_id, "user_name": user_name})

@sio.on("leave")
def ws_leave(data):
    meeting_id = (data or {}).get("meeting_id")
    if meeting_id:
        leave_room(meeting_id)
        emit("ack", {"message": "left", "meeting_id": meeting_id})

@sio.on("ping")
def ws_ping(_data=None):
    emit("pong", {"t": _now_iso_z()})

# ------------------- 서버 시작 -------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    # 개발 편의: reloader가 로그 파일을 두 번 만들지 않게 하려면 use_reloader=False 권장
    # app.run(host="0.0.0.0", port=port, debug=True, use_reloader=False)
    sio.run(app, host="0.0.0.0", port=port, debug=True, use_reloader=False)  # ✅ 웹소켓 사용시 app 대신 sio 실행