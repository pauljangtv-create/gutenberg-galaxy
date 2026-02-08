import gzip
import json
import os
import requests
import pandas as pd
from pathlib import Path
from jsonschema import validate

# [설정] 글로벌 인덱스 및 자산 경로
INDEX_URL = "https://www.gutenberg.org/cache/epub/feeds/pg_catalog.csv"
STATE_PATH = Path("state.json")
OUT_DIR = Path("products")
OUT_DIR.mkdir(exist_ok=True)
SCHEMA = json.loads(Path("schema.json").read_text(encoding="utf-8"))

# HG1: 하루 생산량 제한 (Actions 무료 쿼터 최적화)
MAX_BOOKS = 200 

def load_processed_ids():
    """상태 데이터 로드: 중복 생산 방지"""
    if not STATE_PATH.exists(): return set()
    try:
        data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        return set(str(bid) for bid in data.get("processed_ids", []))
    except: return set()

def fetch_work_queue():
    """7만 권 중 미처리된 인기 도서 200권 추출"""
    processed = load_processed_ids()
    print(f"🔍 Accessing Global Index: {INDEX_URL}")
    df = pd.read_csv(INDEX_URL)
    
    # 다운로드 수 기준 정렬 (가장 시장성 높은 고전 우선순위)
    df = df.sort_values(by='Downloads', ascending=False)
    
    queue = []
    for _, row in df.iterrows():
        book_id = str(row['Text#'])
        if book_id not in processed:
            queue.append({
                "id": book_id, 
                "title": row['Title'],
                "authors": row['Authors']
            })
        if len(queue) >= MAX_BOOKS: break
    return queue

def get_remote_text(book_id):
    """구텐베르크 미러 서버에서 원재료 직접 수급"""
    url = f"https://www.gutenberg.org/cache/epub/{book_id}/pg{book_id}.txt"
    try:
        resp = requests.get(url, timeout=10)
        return resp.text if resp.status_code == 200 else None
    except: return None

def generate_asset(book_id, title):
    """[생산 로직] 텍스트 분석 및 규격화된 상품 생성"""
    # 현재는 인프라 검증을 위해 규격에 맞춘 폴백 데이터 생성
    # 추후 PAID_LLM_ENABLED 설정을 통해 실제 AI 통찰로 교체 가능
    return {
        "book_id": book_id,
        "audience": "professional",
        "irreversible_insight": f"Strategic analysis of '{title}' for global optimization.",
        "cards": ["Assess Core Strategy", "Execute Micro-experiment", "Validate Results"],
        "quiz": [
            {"q": "What is the primary goal?", "a": "Strategic Optimization"},
            {"q": "How to manage risk?", "a": "Identify Fatalities"},
            {"q": "Current Phase?", "a": "Automated Production"}
        ],
        "script_60s": f"Discover the hidden patterns in {title}.",
        "keywords": ["strategy", "classics", "optimization"]
    }

def main():
    queue = fetch_work_queue()
    processed_ids = list(load_processed_ids())
    
    print(f"🚀 Starting Production Line: {len(queue)} items in queue.")
    
    for item in queue:
        try:
            data = generate_asset(item['id'], item['title'])
            validate(instance=data, schema=SCHEMA) # HG2: 품질 검수
            
            # HG4: 압축 저장
            with gzip.open(OUT_DIR / f"{item['id']}.json.gz", "wb") as f:
                f.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))
            
            processed_ids.append(item['id'])
            print(f"✅ Produced: {item['id']} - {item['title'][:30]}")
        except Exception as e:
            print(f"❌ Skip {item['id']}: {e}")
            
    # 최종 상태 기록
    STATE_PATH.write_text(
        json.dumps({"processed_ids": sorted(list(set(processed_ids)))}, indent=2),
        encoding="utf-8"
    )

if __name__ == "__main__":
    main()
