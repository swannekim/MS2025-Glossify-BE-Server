# send_stt_test_long.py
# 간단한 STT 전송 테스트 클라이언트 (긴 텍스트 버전)
# 사용 예:
#   python send_stt_test_long.py finance
#   python send_stt_test_long.py logistics
#   python send_stt_test_long.py both

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

# ---------------------------------------
# 시나리오: Finance (긴 텍스트, 부분→최종)
# ---------------------------------------
def scenario_finance(meeting="finance-005", delay=0.25):
    parts = [
        "오늘 오전 10시 삼성전자 IR룸에서 진행된 3분기 실적 콜 주요 요약입니다. 참석자는 CFO 김현수 부사장, 메모리사업부장, 무선사업부 전략담당 등이었고, 주요 고객 및 파트너 동향도 간략히 공유되었습니다.",
        "메모리 부문에서 DRAM 고정거래가격은 전월 대비 7.3% 상승, NAND는 5.1% 상승했습니다. 서버용 HBM3E 수요가 강하게 유지되었고, AI 가속기 탑재용 고대역 메모리 중심으로 믹스 개선이 있었습니다.",
        "환율 가정은 달러당 1,385원 수준이며, 4분기 영업이익 가이던스는 4조 2천억에서 4조 6천억 원 범위로 제시되었습니다. 수요 회복과 원가 개선, 제품 믹스 개선이 동시에 반영된 수치입니다.",
        "2025년 CAPEX는 총 53조 원으로 계획되었고, 메모리 28조, 파운드리 17조, 디스플레이 및 기타 8조 수준입니다. 평택 P3, P4 라인 증설과 테일러 파운드리 공정 전환 투자가 포함되었습니다.",
        "모바일 부문은 갤럭시 S25 초기 수요가 견조하며, 플래그십 중심의 ASP 상승과 프리미엄 액세서리 번들 전략으로 수익성 개선을 목표로 합니다. 중저가 라인업은 채널 재고 정상화에 집중합니다.",
        "주요 고객사로 엔비디아, 마이크로소프트, 아마존이 언급되었고, HBM 품질/발열/전력 검증은 계획대로 진행 중이라고 밝혔습니다. 일부 고객의 검증 일정이 앞당겨질 가능성도 있습니다.",
        "ESG와 관련해서는 RE100 로드맵에 맞춰 2030년까지 주요 사업장의 재생에너지 전환을 확대하고, 탄소 배출 강도와 물 재이용률 KPI를 강화한다고 했습니다. 협력사 공급망 관리 항목도 개선합니다.",
        "Q&A에서는 주주환원정책, 배당성향 상향 가능성, AI 반도체 파운드리 경쟁력, 차세대 패키징(2.5D/3D) 로드맵에 질문이 집중되었습니다. 다음 IR 미팅은 10월 6일 09:30 강남N타워 12층 Beta 회의실 예정입니다."
    ]

    # partial 전송
    for i, p in enumerate(parts, 1):
        post(meeting, p, is_final=False, seq=i, speaker="A")
        time.sleep(delay)

    # 최종문: 모든 조각을 하나로 합쳐 전송
    final = " ".join(parts)
    post(meeting, final, is_final=True, seq=len(parts)+1, speaker="A")

# ---------------------------------------
# 시나리오: Logistics (긴 텍스트, 부분→최종)
# ---------------------------------------
def scenario_logistics(meeting="logi-005", delay=0.25):
    parts = [
        "부산신항 PNCT 터미널 HMM Nuri(V.023E) 스케줄 변경 안내입니다. CY-Cut은 10월 3일 12:00, SI-Cut은 10월 2일 20:00, 선적 마감은 10월 3일 18:00로 최종 확정되었습니다.",
        "출고 화물은 40HC 컨테이너 3대, 총중량 58,240kg이며, HS Code는 8471.70, 인코텀즈는 DDP Warsaw 조건입니다. 포장 사양은 Export Grade Pallet + 스트래핑 + 코너보드입니다.",
        "항공 보완 물동으로 KE913편이 10월 2일 09:45 인천 출발, 13:20 바르샤바 도착 예정이며, MAWB 180-12345678, HAWB 987654321이 할당되었습니다. 긴급 샘플은 항공으로 전환됩니다.",
        "평택 DC 재고 현황은 3층 랙 A-12, B-07, C-21에 SKU LGM-24F, LOT# 24Q3-PLT009 기준으로 가용 재고 2,450ea, 파손 12ea, 불량 7ea입니다. 파손 품목은 리워크 가능 여부 검토 중입니다.",
        "라스트마일 택배는 차량 89허1234, 기사 박진수 배차 완료, 픽업 10월 1일 16:00입니다. 납품지는 ul. Prosta 51, Warsaw, PL-00-838이며, 납품 창구는 3번 게이트로 지정되었습니다.",
        "통관은 선적 전 사전심사(Pre-clearance)를 완료했고, 원산지증명서(Form EUR.1) 발급 예정입니다. 관세율 3.5%, 부가가치세 23%가 적용되며, 서류는 전자 송부 후 원본 동시 발송합니다.",
        "이슈/액션으로 CFS 포장 보강이 필요하며, 파손 12ea는 10월 1일자로 클레임 접수되어 보상한도 및 면책사항 확인이 요구됩니다. MSDS와 시험성적서는 폴란드어 번역본 추가 제출 예정입니다."
    ]

    # partial 전송
    for i, p in enumerate(parts, 1):
        post(meeting, p, is_final=False, seq=i, speaker="B")
        time.sleep(delay)

    # 최종문: 모든 조각을 하나로 합쳐 전송
    final = " ".join(parts)
    post(meeting, final, is_final=True, seq=len(parts)+1, speaker="B")

# ---------------------------------------
# 엔트리포인트
# ---------------------------------------
if __name__ == "__main__":
    what = (sys.argv[1] if len(sys.argv) > 1 else "finance").lower()
    if what == "finance":
        scenario_finance()
    elif what == "logistics":
        scenario_logistics()
    elif what == "both":
        scenario_finance()
        time.sleep(0.5)
        scenario_logistics()
    else:
        # 알 수 없는 옵션이면 finance 기본 실행
        scenario_finance()
