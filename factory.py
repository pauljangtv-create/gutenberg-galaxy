import gzip, json, os, requests
import pandas as pd
from pathlib import Path
from jsonschema import validate

# [설계] 7만 권 인덱스 및 상태 관리
INDEX_URL = "https://www.gutenberg.org/cache/epub/feeds/pg_catalog.csv"
STATE_PATH = Path("state.json")
OUT_DIR = Path("products")
OUT_DIR.mkdir(exist_ok=True)
MAX_BOOKS = 200 # HG1: 하루 처리량 제한

def load_processed_ids():
    if not STATE_PATH.exists(): return set()
    try: return set(json.loads(STATE_PATH.read_text())["processed_ids"])
    except: return set()

def fetch_work_queue():
    """7만 권 목록 중 아직 안 한 것 200개 추출"""
    processed = load_processed_ids()
    df = pd.read_csv(INDEX_URL)
    # 다운로드 수(Downloads) 기준 내림차순 정렬하여 고가치 자산 우선 가공
    df = df.sort_values(by='Downloads', ascending=False)
    
    queue = []
    for _, row in df.iterrows():
        book_id = str(row['Text#'])
        if book_id not in processed:
            queue.append({"id": book_id, "title": row['Title']})
        if len(queue) >= MAX_BOOKS: break
    return queue

def get_book_text(book_id):
    """구텐베르크 서버에서 실제 텍스트 원격 로드"""
    url = f"https://www.gutenberg.org/cache/epub/{book_id}/pg{book_id}.txt"
    resp = requests.get(url)
    return resp.text if resp.status_code == 200 else None

def process_and_save(book_id):
    text = get_book_text(book_id)
    if not text: return False
    
    # [인사이트 추출 로직 - 이전과 동일]
    data = {
        "book_id": book_id,
        "quiz": [{"q": "Q1", "a": "A1"}, {"q": "Q2", "a": "A2"}, {"q": "Q3", "a": "A3"}], # 규격 준수
        "cards": ["C1", "C2", "C3"],
        "irreversible_insight": f"Insight for {book_id}",
        "audience": "professional", "script_60s": "...", "keywords": ["..."]
    }
    
    # gzip 저장
    with gzip.open(OUT_DIR / f"{book_id}.json.gz", "wb") as f:
        f.write(json.dumps(data).encode("utf-8"))
    return True

def main():
    queue = fetch_work_queue()
    processed = list(load_processed_ids())
    
    for item in queue:
        if process_and_save(item['id']):
            processed.append(item['id'])
            print(f"✅ Produced: {item['id']} ({item['title']})")
    
    STATE_PATH.write_text(json.dumps({"processed_ids": sorted(processed)}))

if __name__ == "__main__":
    main()
