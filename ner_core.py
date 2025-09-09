# ner_core.py
import os, csv
from datetime import datetime
from threading import Lock
from collections import defaultdict
from dotenv import load_dotenv
import requests

# -----------------------------
# 0) 환경 & 경로
# -----------------------------
script_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv()

language_key      = os.getenv("LANGUAGE_KEY")
language_endpoint = os.getenv("LANGUAGE_ENDPOINT")
if not all([language_key, language_endpoint]):
    raise RuntimeError("환경 변수를 확인하세요: LANGUAGE_KEY, LANGUAGE_ENDPOINT")

LOG_FORMAT = (os.getenv("NER_LOG_FORMAT") or "csv").strip().lower()  # csv | txt

stt_results_dir = os.path.join(script_dir, "stt_results")
ner_results_dir = os.path.join(script_dir, "ner_results")
os.makedirs(stt_results_dir, exist_ok=True)
os.makedirs(ner_results_dir, exist_ok=True)

NER_LOG_PATH = None
STT_LOG_PATH = None
NER_LOG_LOCK = Lock()
STT_LOG_LOCK = Lock()

NER_URL = f"{language_endpoint}/language/:analyze-text?api-version=2024-11-01"
HEADERS = {
    "Ocp-Apim-Subscription-Key": language_key,
    "Content-Type": "application/json",
}

# -----------------------------
# 1) NER
# -----------------------------
def analyze_ner(text: str):
    """
    Azure Language NER 호출 -> (entities, grouped)
    """
    payload = {
        "kind": "EntityRecognition",
        "parameters": {"modelVersion": "latest"},
        "analysisInput": {
            "documents": [{"id": "1", "language": "ko", "text": text}]
        },
    }
    resp = requests.post(NER_URL, headers=HEADERS, json=payload, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    entities = data["results"]["documents"][0]["entities"]

    grouped = defaultdict(list)
    for e in entities:
        cat = e.get("category", "Unknown")
        txt = e.get("text", "")
        score = e.get("confidenceScore")
        if txt:
            grouped[cat].append((txt, score))
    return entities, grouped

def print_ner(grouped):
    """콘솔 출력(가독성)"""
    if not grouped:
        print(">> [NER] 인식된 개체 없음.")
        return
    print(">> [NER] 개체명 인식 결과:")
    for category, items in grouped.items():
        print(f"   - {category}:")
        seen = set()
        for txt, score in items:
            key = (txt or "").strip()
            if key and key not in seen:
                seen.add(key)
                if score is not None:
                    print(f"       • {txt} (score: {score:.2f})")
                else:
                    print(f"       • {txt}")

# -----------------------------
# 2) 로그 파일
# -----------------------------
def init_ner_log():
    global NER_LOG_PATH
    if NER_LOG_PATH:
        return
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    fname = f"ner_entities_{ts}.{'csv' if LOG_FORMAT=='csv' else 'txt'}"
    NER_LOG_PATH = os.path.join(ner_results_dir, fname)

    if LOG_FORMAT == "csv":
        with open(NER_LOG_PATH, "w", encoding="utf-8-sig", newline="") as f:
            csv.writer(f).writerow(["timestamp", "category", "entity", "confidence", "source_text"])
    else:
        with open(NER_LOG_PATH, "w", encoding="utf-8") as f:
            f.write("# timestamp | category | entity | confidence | source_text\n")

    print(f"[NER LOG] Writing to {NER_LOG_PATH}")

def append_ner_rows(entities, full_text, ts):
    if not entities:
        return
    if LOG_FORMAT == "csv":
        rows = [[ts, e.get("category"), e.get("text"), e.get("confidenceScore"), full_text] for e in entities]
        with NER_LOG_LOCK:
            with open(NER_LOG_PATH, "a", encoding="utf-8-sig", newline="") as f:
                csv.writer(f).writerows(rows)
    else:
        lines = [
            f"{ts} | {e.get('category')} | {e.get('text')} | {e.get('confidenceScore')} | {full_text}\n"
            for e in entities
        ]
        with NER_LOG_LOCK:
            with open(NER_LOG_PATH, "a", encoding="utf-8") as f:
                f.writelines(lines)

def init_stt_log():
    global STT_LOG_PATH
    if STT_LOG_PATH:
        return
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    fname = f"stt_transcripts_{ts}.txt"
    STT_LOG_PATH = os.path.join(stt_results_dir, fname)
    with open(STT_LOG_PATH, "w", encoding="utf-8") as f:
        f.write("# timestamp | text\n")
    print(f"[STT LOG] Writing to {STT_LOG_PATH}")

def append_stt_line(text: str, ts: str):
    if not text:
        return
    if not STT_LOG_PATH:
        init_stt_log()
    with STT_LOG_LOCK:
        with open(STT_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(f"{ts} | {text}\n")
