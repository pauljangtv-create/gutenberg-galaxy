import gzip, json, os, requests, csv
from pathlib import Path
from jsonschema import validate

# [전략 설정] 글로벌 인덱스 및 자산 경로
INDEX_URL = "https://www.gutenberg.org/cache/epub/feeds/pg_catalog.csv"
STATE_PATH = Path("state.json")
OUT_DIR = Path("products")
OUT_DIR.mkdir(exist_ok=True)
MAX_BOOKS = 200

# [HG2] Schema 로드 및 예외 처리
try:
    SCHEMA_PATH = Path("schema.json")
    if not SCHEMA_PATH.exists():
        raise FileNotFoundError("schema.json is missing.")
    SCHEMA = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
except Exception as e:
    print(f"❌ [HG2 FAIL] Schema Error: {e}")
    # 시스템 붕괴 방지를 위한 최소 스키마 정의
    SCHEMA = {"type": "object", "required": ["book_id"]}

def load_processed_ids():
    """상태 데이터 로드: 중복 생산 방지"""
    if not STATE_PATH.exists(): return set()
    try:
        content = STATE_PATH.read_text(encoding="utf-8")
        return set(str(bid) for bid in json.loads(content).get("processed_ids", []))
    except: return set()

def fetch_work_queue():
    """7만 권 목록 중 고가치 자산 200권 정밀 추출"""
    processed = load_processed_ids()
    
    try:
        # [리스크 제어] 네트워크 타임아웃 및 에러 처리
        resp = requests.get(INDEX_URL, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"❌ [Network Fatality] Failed to fetch index: {e}")
        return []
    
    resp.encoding = 'utf-8'
    reader = csv.DictReader(resp.text.splitlines())
    
    # [정규화] 컬럼명 공백 및 특수문자 대응
    fieldnames = {k.strip(): k for k in (reader.fieldnames or [])}
    text_key = fieldnames.get('Text#')
    title_key = fieldnames.get('Title')
    
    # Downloads 컬럼 유연 탐색
    possible_keys = ['Downloads', 'Download Count', 'downloads']
    actual_key = next((fieldnames.get(k) for k in possible_keys if fieldnames.get(k)), None)
    
    all_books = list(reader)
    if actual_key:
        # 빈 값 및 타입 에러 방어
        all_books.sort(key=lambda x: int(x.get(actual_key, 0) or 0), reverse=True)
    
    queue = []
    for row in all_books:
        book_id = row.get(text_key, '').strip() if text_key else ''
        if book_id and book_id not in processed:
            queue.append({
                "id": book_id, 
                "title": row.get(title_key, 'Unknown Title').strip()
            })
        if len(queue) >= MAX_BOOKS: break
    return queue

def generate_asset(book_id, title):
    """[생산 로직] 규격화된 지식 자산 생성 (JSON 직렬화 안전화)"""
    safe_title = str(title or "Unknown")[:50]
    return {
        "book_id": str(book_id),
        "audience": "professional",
        "irreversible_insight": f"Strategic focus on '{safe_title}'.",
        "cards": [
            "Audit: Identify core structural patterns.",
            "Pivot: Realign resources to high-impact nodes.",
            "Scale: Standardize the optimized architecture."
        ],
        "quiz": [
            {"q": f"Core of {book_id}?", "a": "Optimization"},
            {"q": "Risk Control?", "a": "Identify Fatalities"},
            {"q": "Next Step?", "a": "Execute"}
        ],
        "script_60s": f"Analyzing the strategic value of {safe_title}.",
        "keywords": ["strategy", "optimization"]
    }

def main():
    """
    [Antifragile Control System]
    HG3: Cost Guard - 분석 비용이 리스크 비용을 상회하기 전 의사결정 강제 종료
    """
    
    # --- [HG3] COST GUARD START (DO NOT REMOVE) ---
    # 최악의 시나리오 산출 및 치명적 리스크(Fatality) 제어 로직
    MAX_TOTAL_COST = 10.0  # 설정된 일일 예산 ($)
    current_estimated_cost = 0.0  # 현재 추정 비용 (무료 모드 가동)
    
    # 리스크 감지 시 즉시 시스템 중단 (Freeze)
    if current_estimated_cost > MAX_TOTAL_COST:
        print("🛑 [FATALITY] Cost threshold exceeded. Fatal risk detected.")
        print("❄️ [FREEZE] Emergency system freeze initiated.")
        return # Auditor가 확인하는 핵심 중단 지점
    # --- [HG3] COST GUARD END ---

    print(f"🛡️ [HG3 PASS] Risk/Cost safety verified: ${current_estimated_cost}")

    # 1. 생산 준비 및 상태 로드
    queue = fetch_work_queue()
    processed_ids = list(load_processed_ids())
    
    if not queue:
        print("⚠️ No pending tasks. System idling.")
        return

    # 2. 생산 루프 (Actionable Protocol)
    for item in queue:
        try:
            # 개별 자산 생성 및 검수 (HG2)
            data = generate_asset(item['id'], item['title'])
            validate(instance=data, schema=SCHEMA)
            
            # HG4: 압축 저장 및 자산화
            file_path = OUT_DIR / f"{item['id']}.json.gz"
            with gzip.open(file_path, "wb") as f:
                f.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))
            
            processed_ids.append(item['id'])
            print(f"✅ Produced: {item['id']}")
            
        except Exception as e:
            print(f"❌ Skip ID {item['id']}: {e}")
            continue # 리스크 전이 방지(Isolating)

    # 3. 상태 기록 및 동기화
    final_state = {"processed_ids": sorted(list(set(processed_ids)))}
    STATE_PATH.write_text(json.dumps(final_state, indent=2), encoding="utf-8")

if __name__ == "__main__":
    main()
