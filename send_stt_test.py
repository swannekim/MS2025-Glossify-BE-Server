# send_stt_test.py
# 간단한 STT 전송 테스트 클라이언트
# python send_stt_test.py finance
# python send_stt_test.py logistics

import time, json, sys
from datetime import datetime, timezone
import urllib.request

BASE = "http://localhost:5000"

def post(meeting_id, text, is_final=True, seq=None, speaker=None):
    url = f"{BASE}/meeting/{meeting_id}/stt"
    payload = {
        "text": text,
        "is_final": is_final,
        "seq": seq,
        "speaker": speaker,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json; charset=utf-8"})
    with urllib.request.urlopen(req) as r:
        print(r.read().decode("utf-8"))

def scenario_finance(meeting="finance-005"):
    parts = [
        "오늘 오전 10시 삼성전자와 SK하이닉스 실적 콜에서",
        "DRAM 가격 7% 상승, 환율 1,385원 언급,",
        "반도체 영업이익 가이던스 4조 2천억 원, 2025년 CAPEX 53조 원 유지 계획입니다."
    ]
    for i, p in enumerate(parts, 1):
        post(meeting, p, is_final=False, seq=i, speaker="A")
        time.sleep(0.25)
    final = " ".join(parts)
    post(meeting, final, is_final=True, seq=len(parts)+1, speaker="A")

def scenario_logistics(meeting="logi-005"):
    final = ("부산신항 PNCT HMM Nuri(V.023E) 선적 마감은 10월 3일 18:00로 변경, "
             "KE913편 10월 2일 09:45 인천 출발 13:20 바르샤바 도착. "
             "평택 DC 3층 랙 A-12, B-07 재고 2,450ea, 파손 12ea는 10월 1일 클레임 접수.")
    post(meeting, final, is_final=True, seq=1, speaker="B")

if __name__ == "__main__":
    what = sys.argv[1] if len(sys.argv) > 1 else "finance"
    if what == "finance":
        scenario_finance()
    else:
        scenario_logistics()
