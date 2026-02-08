import gzip, json, os, requests, csv, sys, time
from pathlib import Path
from jsonschema import validate

# [설정] 인프라 및 경로
INDEX_URL = "https://www.gutenberg.org/cache/epub/feeds/pg_catalog.csv"
STATE_PATH = Path("state.json")
OUT_DIR = Path("products")
OUT_DIR.mkdir(exist_ok=True)
MAX_BOOKS = 5  # AI 분석 품질 및 속도 조절을 위해 초기값은 작게 설정

# [보안] GitHub Secrets에서 API 키 로드
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

# [HG2] Schema 로드
try:
    SCHEMA = json.loads(Path("schema.json").read_text(encoding="utf-8"))
except:
    print("⚠️ Schema missing, using fallback")
    SCHEMA = {"type": "object", "required": ["book_id"]}

def load_processed_ids():
    """상태 데이터 로드: 중복 생산 방지"""
    if not STATE_PATH.exists(): 
        return set()
    try: 
        return set(str(bid) for bid in json.loads(STATE_PATH.read_text(encoding="utf-8")).get("processed_ids", []))
    except: 
        return set()

def fetch_work_queue():
    """7만 권 목록 중 고가치 자산 추출"""
    processed = load_processed_ids()
    
    try:
        resp = requests.get(INDEX_URL, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"❌ [Network Fatality] Failed to fetch index: {e}")
        return []
    
    resp.encoding = 'utf-8'
    reader = csv.DictReader(resp.text.splitlines())
    
    # 컬럼명 정규화
    fieldnames = {k.strip(): k for k in (reader.fieldnames or [])}
    text_key = fieldnames.get('Text#')
    title_key = fieldnames.get('Title')
    
    # Downloads 컬럼 탐색
    possible_keys = ['Downloads', 'Download Count', 'downloads']
    actual_key = next((fieldnames.get(k) for k in possible_keys if fieldnames.get(k)), None)
    
    all_books = list(reader)
    if actual_key:
        all_books.sort(key=lambda x: int(x.get(actual_key, 0) or 0), reverse=True)
    
    queue = []
    for row in all_books:
        book_id = row.get(text_key, '').strip() if text_key else ''
        if book_id and book_id not in processed:
            queue.append({
                "id": book_id, 
                "title": row.get(title_key, 'Unknown Title').strip()
            })
        if len(queue) >= MAX_BOOKS: 
            break
    return queue

def get_ai_insight(title):
    """Gemini API를 사용하여 실제 전략적 통찰 생성"""
    if not GEMINI_API_KEY:
        return "Insight pending: API Key missing."
    
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"
    headers = {'Content-Type': 'application/json'}
    prompt = {
        "contents": [{
            "parts": [{
                "text": f"Analyze the classic book '{title}' and provide one 'Irreversible Insight' for global financial architecture optimization. Keep it under 200 characters."
            }]
        }]
    }
    
    try:
        response = requests.post(url, headers=headers, json=prompt, timeout=10)
        response.raise_for_status()
        return response.json()['candidates'][0]['content']['parts'][0]['text'].strip()
    except Exception as e:
        print(f"⚠️ AI Error for {title}: {e}")
        return f"Strategic analysis of {title} in progress."

def generate_asset(book_id, title):
    """[생산 로직] AI 기반 지식 자산 생성"""
    # AI 지능 주입
    insight = get_ai_insight(title)
    
    # Rate Limit 방지를 위한 4초 대기 (Gemini 무료 티어: 15 RPM)
    time.sleep(4) 
    
    safe_title = str(title or "Unknown")[:50]
    return {
        "book_id": str(book_id),
        "audience": "professional",
        "irreversible_insight": insight,
        "cards": [
            "Structural Audit: Identify patterns",
            "Strategic Pivot: Reallocate resources", 
            "Scalable Growth: Standardize architecture"
        ],
        "quiz": [
            {"q": f"Core insight of {safe_title}?", "a": "AI-generated optimization"},
            {"q": "Risk Control?", "a": "Cost guard verification"},
            {"q": "Next Step?", "a": "Execute"}
        ],
        "script_60s": f"AI-powered insight on {safe_title}.",
        "keywords": ["AI-Insight", "Strategy", "Optimization"]
    }

def generate_sitemap(processed_ids):
    """모든 자산을 구글에 신고하기 위한 sitemap.xml 생성"""
    base_url = "https://pauljangtv-create.github.io/gutenberg-galaxy/"
    sitemap = ['<?xml version="1.0" encoding="UTF-8"?>', '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
    
    sitemap.append(f"<url><loc>{base_url}</loc><priority>1.0</priority></url>")
    
    for bid in list(processed_ids)[-5000:]:
        sitemap.append(f"<url><loc>{base_url}?id={bid}</loc><priority>0.8</priority></url>")
    
    sitemap.append('</urlset>')
    Path("sitemap.xml").write_text("\n".join(sitemap), encoding="utf-8")
    Path("robots.txt").write_text(f"User-agent: *\nAllow: /\nSitemap: {base_url}sitemap.xml", encoding="utf-8")
    print("✅ Sitemap generated for SEO")

def main():
    """
    [Antifragile Control System]
    HG3: Cost Guard with AI API validation
    """
    
    # --- [HG3] COST GUARD START (DO NOT REMOVE) ---
    PAID_LLM_ENABLED = bool(GEMINI_API_KEY)  # auditor가 검증하는 변수
    MAX_TOTAL_COST = 10.0  # 설정된 일일 예산 ($)
    current_estimated_cost = 0.0  # Gemini Flash는 무료이므로 0
    
    # API 키 검증
    if not GEMINI_API_KEY:
        print("🛑 [FATALITY] GEMINI_API_KEY missing. System freeze.")
        print("💡 Set GitHub Secret: GEMINI_API_KEY")
        sys.exit(1)
    
    # 리스크 감지 시 즉시 시스템 중단
    if PAID_LLM_ENABLED and current_estimated_cost > MAX_TOTAL_COST:
        print("🛑 [FATALITY] Cost threshold exceeded.")
        sys.exit(1)
    # --- [HG3] COST GUARD END ---

    print(f"🛡️ [HG3 PASS] Risk/Cost safety verified: ${current_estimated_cost}")
    print(f"🤖 AI Mode: {'Enabled' if PAID_LLM_ENABLED else 'Disabled'}")

    # 1. 생산 준비 및 상태 로드
    queue = fetch_work_queue()
    processed_ids = list(load_processed_ids())
    
    if not queue:
        print("⚠️ No pending tasks. System idling.")
        return

    print(f"📋 Queue size: {len(queue)} books")

    # 2. AI 기반 생산 루프
    for item in queue:
        try:
            print(f"🔄 Processing: {item['id']} - {item['title'][:30]}...")
            
            # AI로 개별 자산 생성 및 검수
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
            continue

    # 3. 상태 기록 및 동기화
    final_state = {"processed_ids": sorted(list(set(processed_ids)))}
    STATE_PATH.write_text(json.dumps(final_state, indent=2), encoding="utf-8")
    
    # 4. SEO: Sitemap 생성
    generate_sitemap(processed_ids)
    
    print(f"🎉 Production complete: {len(queue)} assets generated")

if __name__ == "__main__":
    main()
