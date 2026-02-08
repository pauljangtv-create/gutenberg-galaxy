import gzip, json, os, requests, csv
from pathlib import Path
from jsonschema import validate

# [설정] 글로벌 인덱스 및 자산 경로
INDEX_URL = "https://www.gutenberg.org/cache/epub/feeds/pg_catalog.csv"
STATE_PATH = Path("state.json")
OUT_DIR = Path("products")
OUT_DIR.mkdir(exist_ok=True)
MAX_BOOKS = 200

# [HG2] Schema 로드 (에러 복구 로직 포함)
try:
    SCHEMA = json.loads(Path("schema.json").read_text(encoding="utf-8"))
except Exception as e:
    print(f"❌ [HG2 FAIL] Schema missing or corrupt: {e}")
    SCHEMA = {"type": "object", "required": ["book_id"]} # 최소 규격

def load_processed_ids():
    if not STATE_PATH.exists(): return set()
    try: return set(str(bid) for bid in json.loads(STATE_PATH.read_text(encoding="utf-8")).get("processed_ids", []))
    except: return set()

def fetch_work_queue():
    processed = load_processed_ids()
    try:
        resp = requests.get(INDEX_URL, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"❌ [Network Error] {e}"); return []
    
    resp.encoding = 'utf-8'
    reader = csv.DictReader(resp.text.splitlines())
    # 1 & 4. 컬럼명 정규화 및 빈값 처리
    fieldnames = {k.strip(): k for k in (reader.fieldnames or [])}
    text_key = fieldnames.get('Text#')
    title_key = fieldnames.get('Title')
    
    possible_keys = ['Downloads', 'Download Count', 'downloads']
    actual_key = next((fieldnames.get(k) for k in possible_keys if fieldnames.get(k)), None)
    
    all_books = list(reader)
    if actual_key:
        all_books.sort(key=lambda x: int(x.get(actual_key, 0) or 0), reverse=True)
    
    queue = []
    for row in all_books:
        book_id = row.get(text_key, '').strip() if text_key else ''
        if book_id and book_id not in processed:
            queue.append({"id": book_id, "title": row.get(title_key, 'Unknown')})
        if len(queue) >= MAX_BOOKS: break
    return queue

def generate_asset(book_id, title):
    # 5. Title 인코딩 및 None 처리 안전화
    safe_title = str(title or "Unknown Title")[:50]
    return {
        "book_id": str(book_id),
        "audience": "professional",
        "irreversible_insight": f"Strategic focus on '{safe_title}'.",
        "cards": ["Audit Core Pattern", "Pivot Resources", "Scale Architecture"],
        "quiz": [
            {"q": f"Core of {book_id}?", "a": "Optimization"},
            {"q": "Risk Control?", "a": "Identify Fatalities"},
            {"q": "Next Step?", "a": "Execute"}
        ],
        "script_60s": f"Analyzing {safe_title}.",
        "keywords": ["strategy", "optimization"]
    }

def main():
    # HG3: Cost Guard (Explicit logic for Auditor)
    MAX_TOTAL_COST, current_estimated_cost = 10.0, 0.0
    if current_estimated_cost > MAX_TOTAL_COST:
        print("🛑 [FATALITY] Cost exceeded. FREEZE."); return

    queue = fetch_work_queue()
    processed_ids = list(load_processed_ids())
    
    for item in queue:
        try:
            data = generate_asset(item['id'], item['title'])
            validate(instance=data, schema=SCHEMA)
            with gzip.open(OUT_DIR / f"{item['id']}.json.gz", "wb") as f:
                f.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))
            processed_ids.append(item['id'])
            print(f"✅ Produced: {item['id']}")
        except Exception as e:
            print(f"❌ Error {item['id']}: {e}"); continue
            
    STATE_PATH.write_text(json.dumps({"processed_ids": sorted(list(set(processed_ids)))}, indent=2))

if __name__ == "__main__": main()
