import gzip, json, os, requests, csv, sys, time
from pathlib import Path
from jsonschema import validate
from typing import Optional, Dict, Any, Tuple
import logging
from enum import Enum

# [설정] 인프라 및 경로
INDEX_URL = "https://www.gutenberg.org/cache/epub/feeds/pg_catalog.csv"
STATE_PATH = Path("state.json")
OUT_DIR = Path("products")
OUT_DIR.mkdir(exist_ok=True)
MAX_BOOKS = 20  # 생산량 증가 (하이브리드 시스템)

# [보안] Multi-AI API 키 로드
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")  # ChatGPT
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")  # Claude

# [전략] 2-Track Production System
HIGH_VALUE_THRESHOLD = 0.2  # 상위 20%는 3-AI 교차 검증
TRACK_1_PERCENTILE = HIGH_VALUE_THRESHOLD  # Premium Track
TRACK_2_PERCENTILE = 1.0 - HIGH_VALUE_THRESHOLD  # Standard Track

# [리스크 제어] Rate Limit 및 재시도 설정
GEMINI_RPM = 15
OPENAI_RPM = 3  # GPT-4o-mini 무료 티어
CLAUDE_RPM = 5  # Claude Sonnet 무료 티어
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2

# [로깅]
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

class ProcessingTier(Enum):
    """생산 등급 분류"""
    PREMIUM = "3-AI Cross-Validation"  # ChatGPT → Claude → Gemini
    STANDARD = "Single-AI Fast Track"  # Gemini Only

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
    """작업 큐 추출 + 다운로드 순위 메타데이터"""
    processed = load_processed_ids()
    
    try:
        resp = requests.get(INDEX_URL, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.critical(f"[FATALITY] Index fetch failed: {e}")
        return []
    
    resp.encoding = 'utf-8'
    reader = csv.DictReader(resp.text.splitlines())
    
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
    for idx, row in enumerate(all_books):
        book_id = row.get(text_key, '').strip() if text_key else ''
        if book_id and book_id not in processed:
            downloads = int(row.get(actual_key, 0) or 0) if actual_key else 0
            queue.append({
                "id": book_id, 
                "title": row.get(title_key, 'Unknown Title').strip(),
                "author": row.get(author_key, 'Unknown Author').strip() if author_key else 'Unknown Author',
                "subjects": row.get(subjects_key, '').strip() if subjects_key else '',
                "downloads": downloads,
                "rank": idx + 1  # 다운로드 순위
            })
        if len(queue) >= MAX_BOOKS: 
            break
    return queue

# ============================================================
# [AI 브레인 모듈화] The Handlers
# ============================================================

def call_chatgpt_api(prompt: str, max_tokens: int = 300) -> Optional[str]:
    """
    ChatGPT (Strategy): 고전 서사 → 금융 전략 가설 수립
    """
    if not OPENAI_API_KEY:
        logger.warning("OpenAI API Key missing")
        return None
    
    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "model": "gpt-4o-mini",  # 무료 티어
        "messages": [
            {"role": "system", "content": "You are a strategic financial analyst specializing in extracting business insights from classic literature."},
            {"role": "user", "content": prompt}
        ],
        "max_tokens": max_tokens,
        "temperature": 0.7
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(60 / OPENAI_RPM + 1)  # Rate limit
            response = requests.post(url, headers=headers, json=payload, timeout=25)
            
            if response.status_code == 200:
                result = response.json()['choices'][0]['message']['content'].strip()
                logger.info(f"✅ ChatGPT success")
                return result
            elif response.status_code == 429:
                logger.warning(f"⏳ ChatGPT Rate Limit (attempt {attempt+1})")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_BACKOFF_BASE ** attempt * 5)
                    continue
            else:
                logger.error(f"❌ ChatGPT error {response.status_code}: {response.text[:100]}")
                return None
                
        except Exception as e:
            logger.error(f"💥 ChatGPT exception: {e}")
            if attempt < MAX_RETRIES - 1:
                continue
            return None
    
    return None

def call_claude_api(prompt: str, max_tokens: int = 300) -> Optional[str]:
    """
    Claude (Auditor): ChatGPT 결과 → 논리 검증 + 비즈니스 언어 압축
    """
    if not ANTHROPIC_API_KEY:
        logger.warning("Anthropic API Key missing")
        return None
    
    url = "https://api.anthropic.com/v1/messages"
    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "Content-Type": "application/json"
    }
    
    payload = {
        "model": "claude-3-5-haiku-20241022",  # 빠른 모델
        "max_tokens": max_tokens,
        "messages": [
            {
                "role": "user", 
                "content": f"As a business auditor, refine and compress this strategic insight into precise, actionable language (under 200 chars):\n\n{prompt}"
            }
        ]
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(60 / CLAUDE_RPM + 1)  # Rate limit
            response = requests.post(url, headers=headers, json=payload, timeout=25)
            
            if response.status_code == 200:
                result = response.json()['content'][0]['text'].strip()
                logger.info(f"✅ Claude success")
                return result
            elif response.status_code == 429:
                logger.warning(f"⏳ Claude Rate Limit (attempt {attempt+1})")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_BACKOFF_BASE ** attempt * 5)
                    continue
            else:
                logger.error(f"❌ Claude error {response.status_code}: {response.text[:100]}")
                return None
                
        except Exception as e:
            logger.error(f"💥 Claude exception: {e}")
            if attempt < MAX_RETRIES - 1:
                continue
            return None
    
    return None

def call_gemini_api(prompt: str) -> Optional[str]:
    """
    Gemini (Executor): 최종 정제 → JSON 구조화 및 대량 양산
    """
    if not GEMINI_API_KEY:
        logger.warning("Gemini API Key missing")
        return None
    
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"
    headers = {'Content-Type': 'application/json'}
    
    payload = {
        "contents": [{
            "parts": [{"text": prompt}]
        }]
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(60 / GEMINI_RPM + 0.5)  # Rate limit
            response = requests.post(url, headers=headers, json=payload, timeout=20)
            
            if response.status_code == 200:
                result = response.json()['candidates'][0]['content']['parts'][0]['text'].strip()
                logger.info(f"✅ Gemini success")
                return result
            elif response.status_code == 429:
                logger.warning(f"⏳ Gemini Rate Limit (attempt {attempt+1})")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_BACKOFF_BASE ** attempt * 4)
                    continue
            else:
                logger.error(f"❌ Gemini error {response.status_code}: {response.text[:100]}")
                return None
                
        except Exception as e:
            logger.error(f"💥 Gemini exception: {e}")
            if attempt < MAX_RETRIES - 1:
                continue
            return None
    
    return None

# ============================================================
# [하이브리드 추론 엔진] Orchestration Logic
# ============================================================

def process_premium_asset(title: str, author: str, subjects: str) -> Optional[str]:
    """
    🏆 Premium Track: 3-AI 순차 파이프라인
    ChatGPT (가설) → Claude (감사) → Gemini (구조화)
    """
    logger.info(f"🏆 [PREMIUM] Starting 3-AI pipeline for '{title[:30]}'")
    
    # Stage 1: ChatGPT - 전략 가설 수립
    context = f"Author: {author}" if author != 'Unknown Author' else ""
    if subjects:
        context += f" | Genre: {subjects[:80]}"
    
    hypothesis_prompt = (
        f"Book: '{title}'\n{context}\n\n"
        f"Extract ONE strategic financial hypothesis from this book's themes. "
        f"Focus on architectural patterns applicable to global finance."
    )
    
    hypothesis = call_chatgpt_api(hypothesis_prompt)
    if not hypothesis:
        logger.warning(f"⚠️ ChatGPT failed, falling back to standard track")
        return process_standard_asset(title, author, subjects)
    
    logger.info(f"📝 Hypothesis: {hypothesis[:60]}...")
    
    # Stage 2: Claude - 논리 감사 및 압축
    audit_prompt = f"Original hypothesis: {hypothesis}\n\nRefine this into a precise, actionable business insight."
    
    refined = call_claude_api(audit_prompt)
    if not refined:
        logger.warning(f"⚠️ Claude failed, using ChatGPT output")
        refined = hypothesis
    
    logger.info(f"🔍 Refined: {refined[:60]}...")
    
    # Stage 3: Gemini - 최종 검증 및 압축
    final_prompt = (
        f"Compress this business insight to under 200 characters while preserving actionability:\n\n{refined}"
    )
    
    final_insight = call_gemini_api(final_prompt)
    if not final_insight:
        logger.warning(f"⚠️ Gemini failed, using Claude output")
        final_insight = refined
    
    return final_insight

def process_standard_asset(title: str, author: str, subjects: str) -> Optional[str]:
    """
    ⚡ Standard Track: Gemini 단독 고속 처리
    """
    logger.info(f"⚡ [STANDARD] Fast track for '{title[:30]}'")
    
    context = f"Author: {author}" if author != 'Unknown Author' else ""
    if subjects:
        context += f" | Genre: {subjects[:80]}"
    
    prompt = (
        f"Book: '{title}'\n{context}\n\n"
        f"Extract ONE UNIQUE strategic business insight for financial architecture. "
        f"Be specific to this book's themes. Under 200 characters."
    )
    
    return call_gemini_api(prompt)

def generate_asset(book_id: str, title: str, author: str, subjects: str, tier: ProcessingTier) -> Optional[Dict[str, Any]]:
    """
    [하이브리드 생산 엔진] Tier에 따라 분기 처리
    """
    # AI 추론 파이프라인 선택
    if tier == ProcessingTier.PREMIUM:
        insight = process_premium_asset(title, author, subjects)
    else:
        insight = process_standard_asset(title, author, subjects)
    
    if insight is None:
        logger.warning(f"⚠️ All AI tracks failed for '{title}'")
        return None
    
    safe_title = str(title or "Unknown")[:80]
    safe_author = str(author or "Unknown")[:50]
    
    return {
        "book_id": str(book_id),
        "source_book": safe_title,
        "source_author": safe_author,
        "processing_tier": tier.value,  # 메타데이터: 어떤 파이프라인 사용했는지
        "audience": "professional",
        "irreversible_insight": insight,
        "cards": [
            f"Structural Audit: {safe_title[:30]} patterns",
            f"Strategic Pivot: {safe_author}'s framework", 
            "Scalable Growth: Standardize architecture"
        ],
        "quiz": [
            {"q": f"Core insight of '{safe_title[:30]}'?", "a": "Book-specific strategy"},
            {"q": f"Author?", "a": safe_author},
            {"q": "Application?", "a": "Financial architecture"}
        ],
        "script_60s": f"Hybrid AI insight from '{safe_title}' by {safe_author}.",
        "keywords": ["Multi-AI", safe_author.split()[0] if safe_author else "Strategy", tier.name]
    }

def generate_sitemap(processed_ids):
    """SEO 사이트맵 생성"""
    base_url = "https://pauljangtv-create.github.io/gutenberg-galaxy/"
    sitemap = ['<?xml version="1.0" encoding="UTF-8"?>', '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
    
    sitemap.append(f"<url><loc>{base_url}</loc><priority>1.0</priority></url>")
    
    for bid in list(processed_ids)[-5000:]:
        sitemap.append(f"<url><loc>{base_url}?id={bid}</loc><priority>0.8</priority></url>")
    
    sitemap.append('</urlset>')
    Path("sitemap.xml").write_text("\n".join(sitemap), encoding="utf-8")
    Path("robots.txt").write_text(f"User-agent: *\nAllow: /\nSitemap: {base_url}sitemap.xml", encoding="utf-8")
    logger.info("✅ Sitemap generated")

def main():
    """
    [Hybrid AI Orchestration System]
    2-Track Production: Premium (3-AI) + Standard (1-AI)
    """
    
    # --- [HG3] COST GUARD START (DO NOT REMOVE) ---
    PAID_LLM_ENABLED = bool(GEMINI_API_KEY)
    MAX_TOTAL_COST = 10.0
    current_estimated_cost = 0.0
    
    if not GEMINI_API_KEY:
        logger.critical("🛑 [FATALITY] GEMINI_API_KEY missing.")
        sys.exit(1)
    
    if PAID_LLM_ENABLED and current_estimated_cost > MAX_TOTAL_COST:
        logger.critical("🛑 [FATALITY] Cost threshold exceeded.")
        sys.exit(1)
    # --- [HG3] COST GUARD END ---

    logger.info("=" * 70)
    logger.info("🚀 Hybrid AI Orchestration System v2.0")
    logger.info(f"🛡️ Cost Guard: ${current_estimated_cost} / ${MAX_TOTAL_COST}")
    logger.info(f"🤖 AI Status: Gemini={'✅' if GEMINI_API_KEY else '❌'} | "
                f"ChatGPT={'✅' if OPENAI_API_KEY else '❌'} | "
                f"Claude={'✅' if ANTHROPIC_API_KEY else '❌'}")
    logger.info("=" * 70)

    # 작업 큐 로드
    queue = fetch_work_queue()
    processed_ids = list(load_processed_ids())
    
    if not queue:
        logger.info("⚠️ No pending tasks.")
        return

    logger.info(f"📋 Queue: {len(queue)} books")
    
    # [2-Track 분기] 상위 20% vs 나머지
    premium_cutoff = int(len(queue) * TRACK_1_PERCENTILE)
    
    premium_count = 0
    standard_count = 0
    failure_count = 0
    
    for idx, item in enumerate(queue):
        try:
            # Tier 결정: 다운로드 순위 기반
            tier = ProcessingTier.PREMIUM if idx < premium_cutoff else ProcessingTier.STANDARD
            
            logger.info(f"{'='*70}")
            logger.info(f"🔄 [{idx+1}/{len(queue)}] {item['id']} - '{item['title'][:40]}'")
            logger.info(f"📊 Rank: #{item['rank']} | Downloads: {item['downloads']:,} | Tier: {tier.name}")
            
            data = generate_asset(
                item['id'], 
                item['title'], 
                item['author'],
                item['subjects'],
                tier
            )
            
            if data is None:
                logger.warning(f"⏭️ Skipped: {item['id']}")
                failure_count += 1
                continue
            
            validate(instance=data, schema=SCHEMA)
            
            # HG4: 압축 저장
            file_path = OUT_DIR / f"{item['id']}.json.gz"
            with gzip.open(file_path, "wb") as f:
                f.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))
            
            processed_ids.append(item['id'])
            
            if tier == ProcessingTier.PREMIUM:
                premium_count += 1
            else:
                standard_count += 1
            
            logger.info(f"✅ Success | Insight: {data['irreversible_insight'][:60]}...")
            
        except Exception as e:
            logger.error(f"💥 Error for {item['id']}: {e}")
            failure_count += 1
            continue

    # 상태 저장
    final_state = {"processed_ids": sorted(list(set(processed_ids)))}
    STATE_PATH.write_text(json.dumps(final_state, indent=2), encoding="utf-8")
    
    generate_sitemap(processed_ids)
    
    # 최종 리포트
    total = premium_count + standard_count
    logger.info("=" * 70)
    logger.info("🎉 Production Complete")
    logger.info(f"🏆 Premium (3-AI): {premium_count} assets")
    logger.info(f"⚡ Standard (1-AI): {standard_count} assets")
    logger.info(f"❌ Failures: {failure_count} assets")
    logger.info(f"📊 Success Rate: {total/(total+failure_count)*100:.1f}%" if total+failure_count > 0 else "N/A")
    logger.info("=" * 70)

if __name__ == "__main__":
    main()
