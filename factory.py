import gzip, json, os, requests, csv
from pathlib import Path
from jsonschema import validate

# [설정] 글로벌 인덱스 및 자산 경로
INDEX_URL = "https://www.gutenberg.org/cache/epub/feeds/pg_catalog.csv"
STATE_PATH = Path("state.json")
OUT_DIR = Path("products")
OUT_DIR.mkdir(exist_ok=True)
SCHEMA = json.loads(Path("schema.json").read_text(encoding="utf-8"))
MAX_BOOKS = 200 

def load_processed_ids():
    if not STATE_PATH.exists(): return set()
    try:
        data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        return set(str(bid) for bid in data.get("processed_ids", []))
    except: return set()

def fetch_work_queue():
    """Pandas 없이 대용량 CSV를 스트리밍 방식으로 읽어 200권 추출"""
    processed = load_processed_ids()
    resp = requests.get(INDEX_URL)
    resp.encoding = 'utf-8'
    
    # CSV 파싱 (메모리 효율적)
    lines = resp.text.splitlines()
    reader = csv.DictReader(lines)
    
    # 컬럼명 유연성 확보 (Downloads 또는 Download Count 대응)
    possible_keys = ['Downloads', 'Download Count', 'downloads']
    actual_key = next((k for k in possible_keys if k in reader.fieldnames), None)
    
    # 데이터 리스트화 및 정렬
    all_books = list(reader)
    if actual_key:
        all_books.sort(key=lambda x: int(x[actual_key] or 0), reverse=True)
    
    queue = []
    for row in all_books:
        book_id = str(row['Text#'])
        if book_id not in processed:
            queue.append({"id": book_id, "title": row['Title']})
        if len(queue) >= MAX_BOOKS: break
    return queue

def generate_asset(book_id, title):
    return {
        "book_id": book_id,
        "audience": "professional",
        "irreversible_insight": f"Strategic analysis of '{title}'",
        "cards": ["Phase 1: Audit", "Phase 2: Pivot", "Phase 3: Scale"],
        "quiz": [
            {"q": "Q1: Verify Asset?", "a": "Yes"},
            {"q": "Q2: Target Volume?", "a": "70k"},
            {"q": "Q3: Strategy?", "a": "Optimization"}
        ],
        "script_60s": f"Insight on {title}",
        "keywords": ["strategy", "automated"]
    }

def main():
    queue = fetch_work_queue()
    processed_ids = list(load_processed_ids())
    
    for item in queue:
        try:
            data = generate_asset(item['id'], item['title'])
            validate(instance=data, schema=SCHEMA)
            with gzip.open(OUT_DIR / f"{item['id']}.json.gz", "wb") as f:
                f.write(json.dumps(data).encode("utf-8"))
            processed_ids.append(item['id'])
            print(f"✅ Produced: {item['id']}")
        except Exception as e:
            print(f"❌ Error {item['id']}: {e}")
            
    STATE_PATH.write_text(json.dumps({"processed_ids": sorted(list(set(processed_ids)))}))

if __name__ == "__main__":
    main()
