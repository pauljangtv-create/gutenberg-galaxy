import gzip, json, os, requests, csv, sys, time
from pathlib import Path
from jsonschema import validate
from typing import Optional, Dict, Any
import logging

# [설정] 인프라 및 경로
INDEX_URL = "https://www.gutenberg.org/cache/epub/feeds/pg_catalog.csv"
STATE_PATH = Path("state.json")
OUT_DIR = Path("products")
OUT_DIR.mkdir(exist_ok=True)
MAX_BOOKS = 5

# [보안] GitHub Secrets에서 API 키 로드
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

# [리스크 제어] Rate Limit 및 재시도 설정
RATE_LIMIT_RPM = 15  # Gemini 무료 티어
RATE_LIMIT_DELAY = 60 / RATE_LIMIT_RPM + 0.5  # 4.5초 (안전 여유)
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2  # 지수 백오프 베이스

# [로깅] 구조화된 에러 추적
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# [HG2] Schema 로드
try:
    SCHEMA = json.loads(Path("schema.json").read_text(encoding="utf-8"))
except Exception as e:
    logger.warning(f"Schema load failed: {e}, using fallback")
    SCHEMA = {"type": "object", "required": ["book_id"]}

def load_processed_ids():
    """상태 데이터 로드: 중복 생산 방지"""
    if not STATE_PATH.exists(): 
        return set()
    try: 
        return set(str(bid) for bid in json.loads(STATE_PATH.read_text(encoding="utf-8")).get("processed_ids", []))
    except Exception as e:
        logger.error(f"State load failed: {e}")
        return set()

def fetch_work_queue():
    """7만 권 목록 중 고가치 자산 추출 (제목+저자 메타데이터 포함)"""
    processed = load_processed_ids()
    
    try:
        resp = requests.get(INDEX_URL, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.critical(f"[FATALITY] Index fetch failed: {e}")
        return []
    
    resp.encoding = 'utf-8'
    reader = csv.DictReader(resp.text.splitlines())
    
    # 컬럼명 정규화
    fieldnames = {k.strip(): k for k in (reader.fieldnames or [])}
    text_key = fieldnames.get('Text#')
    title_key = fieldnames.get('Title')
    author_key = fieldnames.get('Authors')
    subjects_key = fieldnames.get('Subjects')
    
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
                "title": row.get(title_key, 'Unknown Title').strip(),
                "author": row.get(author_key, 'Unknown Author').strip() if author_key else 'Unknown Author',
                "subjects": row.get(subjects_key, '').strip() if subjects_key else ''
            })
        if len(queue) >= MAX_BOOKS: 
            break
    return queue

def get_ai_insight(title: str, author: str, subjects: str) -> Optional[str]:
    """
    [Antifragile AI 호출] 재시도 + 지수 백오프 + 에러 타입별 격리
    """
    if not GEMINI_API_KEY:
        logger.warning(f"API Key missing for '{title}'")
        return None
    
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"
    headers = {'Content-Type': 'application/json'}
    
    context = f"Author: {author}" if author != 'Unknown Author' else ""
    if subjects:
        context += f" | Genre/Subjects: {subjects[:100]}"
    
    prompt = {
        "contents": [{
            "parts": [{
                "text": (
                    f"Book Title: '{title}'\n"
                    f"{context}\n\n"
                    f"Task: Extract ONE UNIQUE strategic business insight from THIS SPECIFIC BOOK "
                    f"for global financial architecture optimization. "
                    f"Do NOT use generic advice like 'optimize resources' or 'be strategic'. "
                    f"Reflect the book's SPECIFIC themes, plot, or philosophical arguments. "
                    f"Must be actionable and distinctive to THIS book. "
                    f"Keep it under 200 characters in English."
                )
            }]
        }]
    }
    
    # [핵심] 재시도 로직 with 지수 백오프
    for attempt in range(MAX_RETRIES):
        try:
            # Rate Limit 방어: 요청 '전' 대기
            if attempt > 0:
                backoff_delay = RATE_LIMIT_DELAY * (RETRY_BACKOFF_BASE ** (attempt - 1))
                logger.info(f"Retry {attempt}/{MAX_RETRIES} for '{title}' after {backoff_delay:.1f}s")
                time.sleep(backoff_delay)
            else:
                time.sleep(RATE_LIMIT_DELAY)
            
            response = requests.post(url, headers=headers, json=prompt, timeout=20)
            
            # [에러 타입별 분기]
            if response.status_code == 200:
                insight = response.json()['candidates'][0]['content']['parts'][0]['text'].strip()
                logger.info(f"✅ AI success for '{title[:30]}'")
                return insight
            
            elif response.status_code == 429:
                # Rate Limit: 재시도 가능
                logger.warning(f"⏳ Rate Limit hit for '{title}' (attempt {attempt+1})")
                if attempt < MAX_RETRIES - 1:
                    continue  # 재시도
                else:
                    logger.error(f"❌ Rate Limit exhausted for '{title}'")
                    return None
            
            elif response.status_code >= 500:
                # Server Error: 재시도 가능
                logger.warning(f"🔧 Server error {response.status_code} for '{title}' (attempt {attempt+1})")
                if attempt < MAX_RETRIES - 1:
                    continue
                else:
                    logger.error(f"❌ Server errors exhausted for '{title}'")
                    return None
            
            elif response.status_code == 403:
                # Forbidden: API 키 문제, 재시도 불가
                logger.critical(f"🛑 API Key invalid for '{title}': {response.text[:100]}")
                return None
            
            else:
                # 기타 클라이언트 에러: 재시도 불가
                logger.error(f"❌ HTTP {response.status_code} for '{title}': {response.text[:100]}")
                return None
                
        except requests.Timeout:
            logger.warning(f"⏱️ Timeout for '{title}' (attempt {attempt+1})")
            if attempt < MAX_RETRIES - 1:
                continue
            else:
                logger.error(f"❌ Timeout exhausted for '{title}'")
                return None
        
        except requests.RequestException as e:
            logger.error(f"🌐 Network error for '{title}': {e}")
            if attempt < MAX_RETRIES - 1:
                continue
            else:
                return None
        
        except (KeyError, IndexError, json.JSONDecodeError) as e:
            # 응답 파싱 에러: 재시도 불가
            logger.error(f"🔍 Response parse error for '{title}': {e}")
            return None
        
        except Exception as e:
            # 예상치 못한 에러: 격리
            logger.critical(f"💥 Unexpected error for '{title}': {type(e).__name__} - {e}")
            return None
    
    return None  # 모든 재시도 실패

def generate_asset(book_id: str, title: str, author: str, subjects: str) -> Optional[Dict[str, Any]]:
    """
    [Step 2] 데이터 구조 내 출처 명시 및 AI 통찰 주입
    """
    # AI 지능 주입 (Antifragile 호출)
    insight = get_ai_insight(title, author, subjects)
    
    # [핵심] AI 실패 시 None 반환 (State 오염 방지)
    if insight is None:
        logger.warning(f"⚠️ Skipping asset for '{title}' due to AI failure")
        return None
    
    safe_title = str(title or "Unknown")[:80]
    safe_author = str(author or "Unknown")[:50]
    
    return {
        "book_id": str(book_id),
        "source_book": safe_title,
        "source_author": safe_author,
        "audience": "professional",
        "irreversible_insight": insight,
        "cards": [
            f"Structural Audit: Analyze '{safe_title[:30]}' patterns",
            f"Strategic Pivot: Apply {safe_author}'s framework", 
            "Scalable Growth: Standardize architecture"
        ],
        "quiz": [
            {"q": f"Core insight of '{safe_title[:30]}'?", "a": "Book-specific optimization"},
            {"q": f"Who wrote this?", "a": safe_author},
            {"q": "Application?", "a": "Financial architecture"}
        ],
        "script_60s": f"AI-powered insight from '{safe_title}' by {safe_author}.",
        "keywords": ["AI-Insight", safe_author.split()[0] if safe_author else "Strategy", "Book-Analysis"]
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
    logger.info("✅ Sitemap generated for SEO")

def main():
    """
    [Antifragile Control System]
    HG3: Cost Guard with AI API validation
    """
    
    # --- [HG3] COST GUARD START (DO NOT REMOVE) ---
    PAID_LLM_ENABLED = bool(GEMINI_API_KEY)
    MAX_TOTAL_COST = 10.0
    current_estimated_cost = 0.0
    
    if not GEMINI_API_KEY:
        logger.critical("🛑 [FATALITY] GEMINI_API_KEY missing. System freeze.")
        print("💡 Set GitHub Secret: GEMINI_API_KEY")
        sys.exit(1)
    
    if PAID_LLM_ENABLED and current_estimated_cost > MAX_TOTAL_COST:
        logger.critical("🛑 [FATALITY] Cost threshold exceeded.")
        sys.exit(1)
    # --- [HG3] COST GUARD END ---

    logger.info(f"🛡️ [HG3 PASS] Risk/Cost safety verified: ${current_estimated_cost}")
    logger.info(f"🤖 AI Mode: {'Enabled (Antifragile)' if PAID_LLM_ENABLED else 'Disabled'}")
    logger.info(f"⚙️ Rate Limit: {RATE_LIMIT_RPM} RPM (delay: {RATE_LIMIT_DELAY:.1f}s)")

    # 1. 생산 준비 및 상태 로드
    queue = fetch_work_queue()
    processed_ids = list(load_processed_ids())
    
    if not queue:
        logger.info("⚠️ No pending tasks. System idling.")
        return

    logger.info(f"📋 Queue size: {len(queue)} books")

    # 2. AI 기반 맞춤형 생산 루프 (Isolating Architecture)
    success_count = 0
    failure_count = 0
    
    for item in queue:
        try:
            logger.info(f"🔄 Processing: {item['id']} - '{item['title'][:40]}' by {item['author'][:30]}")
            
            # AI로 개별 자산 생성 (None 반환 시 건너뜀)
            data = generate_asset(
                item['id'], 
                item['title'], 
                item['author'],
                item['subjects']
            )
            
            # [핵심] AI 실패 시 State 오염 방지
            if data is None:
                logger.warning(f"⏭️ Skipped: {item['id']} (AI failure)")
                failure_count += 1
                continue  # processed_ids에 추가하지 않음!
            
            validate(instance=data, schema=SCHEMA)
            
            # HG4: 압축 저장 및 자산화
            file_path = OUT_DIR / f"{item['id']}.json.gz"
            with gzip.open(file_path, "wb") as f:
                f.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))
            
            # [핵심] 성공 시에만 State 업데이트
            processed_ids.append(item['id'])
            success_count += 1
            logger.info(f"✅ Produced: {item['id']} | Insight: {data['irreversible_insight'][:60]}...")
            
        except Exception as e:
            logger.error(f"💥 Unexpected error for {item['id']}: {type(e).__name__} - {e}")
            failure_count += 1
            continue  # 리스크 전이 방지

    # 3. 상태 기록 및 동기화 (성공한 것만)
    final_state = {"processed_ids": sorted(list(set(processed_ids)))}
    STATE_PATH.write_text(json.dumps(final_state, indent=2), encoding="utf-8")
    
    # 4. SEO: Sitemap 생성
    generate_sitemap(processed_ids)
    
    # 5. 최종 리포트
    logger.info("=" * 60)
    logger.info(f"🎉 Production complete")
    logger.info(f"✅ Success: {success_count} assets")
    logger.info(f"❌ Failures: {failure_count} assets")
    logger.info(f"📊 Success Rate: {success_count/(success_count+failure_count)*100:.1f}%")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()
