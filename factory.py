import gzip, json, os, requests, csv, sys, time
from pathlib import Path
from jsonschema import validate
from typing import Optional, Dict, Any
import logging
from enum import Enum

# ============================================================
# [STABLE v2.2] Claude + Gemini (ChatGPT 제외)
# ============================================================

INDEX_URL = "https://www.gutenberg.org/cache/epub/feeds/pg_catalog.csv"
STATE_PATH = Path("state.json")
OUT_DIR = Path("products")
OUT_DIR.mkdir(exist_ok=True)
MAX_BOOKS = 20

# API 키 (ChatGPT 제거)
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")

# 2-Track System
HIGH_VALUE_THRESHOLD = 0.2

# Rate Limit Protection (안정적 설정)
GEMINI_DELAY = 6.0   # 10 RPM (15 RPM 여유)
CLAUDE_DELAY = 15.0  # 4 RPM (5 RPM 여유)
MAX_RETRIES = 3
RETRY_BACKOFF = 10

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

class ProcessingTier(Enum):
    PREMIUM = "2-AI (Claude→Gemini)"
    STANDARD = "1-AI (Gemini)"

try:
    SCHEMA = json.loads(Path("schema.json").read_text(encoding="utf-8"))
except:
    SCHEMA = {"type": "object", "required": ["book_id"]}

def load_processed_ids():
    if not STATE_PATH.exists(): 
        return set()
    try: 
        return set(str(bid) for bid in json.loads(STATE_PATH.read_text(encoding="utf-8")).get("processed_ids", []))
    except: 
        return set()

def fetch_work_queue():
    processed = load_processed_ids()
    
    try:
        resp = requests.get(INDEX_URL, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.critical(f"Index fetch failed: {e}")
        return []
    
    resp.encoding = 'utf-8'
    reader = csv.DictReader(resp.text.splitlines())
    
    fieldnames = {k.strip(): k for k in (reader.fieldnames or [])}
    text_key = fieldnames.get('Text#')
    title_key = fieldnames.get('Title')
    author_key = fieldnames.get('Authors')
    subjects_key = fieldnames.get('Subjects')
    
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
                "rank": idx + 1
            })
        if len(queue) >= MAX_BOOKS: 
            break
    return queue

def call_claude_api(prompt: str) -> Optional[str]:
    """Claude: 초기 분석"""
    if not ANTHROPIC_API_KEY:
        logger.warning("Anthropic API Key not available")
        return None
    
    url = "https://api.anthropic.com/v1/messages"
    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "Content-Type": "application/json"
    }
    
    payload = {
        "model": "claude-3-5-haiku-20241022",
        "max_tokens": 400,
        "messages": [
            {
                "role": "user",
                "content": prompt
            }
        ]
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"⏳ Claude waiting {CLAUDE_DELAY}s...")
            time.sleep(CLAUDE_DELAY)
            
            response = requests.post(url, headers=headers, json=payload, timeout=25)
            
            if response.status_code == 200:
                result = response.json()['content'][0]['text'].strip()
                logger.info(f"✅ Claude success")
                return result
            elif response.status_code == 429:
                logger.warning(f"⏳ Claude Rate Limit ({attempt+1}/{MAX_RETRIES})")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_BACKOFF * (2 ** attempt))
                    continue
            else:
                logger.error(f"❌ Claude HTTP {response.status_code}: {response.text[:100]}")
                return None
                
        except Exception as e:
            logger.error(f"💥 Claude error: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_BACKOFF)
                continue
            return None
    
    return None

def call_gemini_api(prompt: str) -> Optional[str]:
    """Gemini: 정제 또는 단독 생성"""
    if not GEMINI_API_KEY:
        logger.warning("Gemini API Key not available")
        return None
    
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"
    headers = {'Content-Type': 'application/json'}
    
    payload = {"contents": [{"parts": [{"text": prompt}]}]}
    
    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"⏳ Gemini waiting {GEMINI_DELAY}s...")
            time.sleep(GEMINI_DELAY)
            
            response = requests.post(url, headers=headers, json=payload, timeout=20)
            
            if response.status_code == 200:
                result = response.json()['candidates'][0]['content']['parts'][0]['text'].strip()
                logger.info(f"✅ Gemini success")
                return result
            elif response.status_code == 429:
                logger.warning(f"⏳ Gemini Rate Limit ({attempt+1}/{MAX_RETRIES})")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_BACKOFF * (2 ** attempt))
                    continue
            else:
                logger.error(f"❌ Gemini HTTP {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"💥 Gemini error: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_BACKOFF)
                continue
            return None
    
    return None

def process_premium_asset(title: str, author: str, subjects: str) -> tuple[Optional[str], ProcessingTier]:
    """🏆 Premium: Claude → Gemini"""
    logger.info(f"🏆 Premium (2-AI)")
    
    context = f"Book: '{title}' by {author}"
    if subjects:
        context += f" | Genre: {subjects[:80]}"
    
    # Stage 1: Claude - 초기 분석
    claude_prompt = (
        f"{context}\n\n"
        f"Task: Extract ONE strategic business insight from this book's themes "
        f"for global financial architecture optimization. "
        f"Be specific to this book's narrative or philosophy. "
        f"Keep under 250 characters."
    )
    
    analysis = call_claude_api(claude_prompt)
    
    if not analysis:
        logger.warning("⚠️ Claude failed, falling back to Gemini-only")
        result = process_standard_asset(title, author, subjects)
        return result, ProcessingTier.STANDARD if result else (None, ProcessingTier.STANDARD)
    
    logger.info(f"📝 Claude: {analysis[:60]}...")
    
    # Stage 2: Gemini - 압축 및 정제
    gemini_prompt = f"Compress this business insight to under 200 characters while keeping it actionable:\n\n{analysis}"
    
    final = call_gemini_api(gemini_prompt)
    
    if final:
        return final, ProcessingTier.PREMIUM
    
    return analysis, ProcessingTier.PREMIUM

def process_standard_asset(title: str, author: str, subjects: str) -> Optional[str]:
    """⚡ Standard: Gemini only"""
    logger.info(f"⚡ Standard (1-AI)")
    
    context = f"Book: '{title}' by {author}"
    if subjects:
        context += f" | Genre: {subjects[:80]}"
    
    prompt = (
        f"{context}\n\n"
        f"Extract ONE UNIQUE strategic business insight from this book "
        f"for financial architecture optimization. "
        f"Be specific to this book's themes. Under 200 characters."
    )
    
    return call_gemini_api(prompt)

def generate_asset(book_id: str, title: str, author: str, subjects: str, tier_priority: str) -> Optional[Dict[str, Any]]:
    """Asset generation with tier tracking"""
    
    if tier_priority == "PREMIUM":
        insight, actual_tier = process_premium_asset(title, author, subjects)
    else:
        insight = process_standard_asset(title, author, subjects)
        actual_tier = ProcessingTier.STANDARD
    
    if insight is None:
        return None
    
    safe_title = str(title or "Unknown")[:80]
    safe_author = str(author or "Unknown")[:50]
    
    return {
        "book_id": str(book_id),
        "source_book": safe_title,
        "source_author": safe_author,
        "processing_tier": actual_tier.value,
        "audience": "professional",
        "irreversible_insight": insight,
        "cards": [
            f"Audit: {safe_title[:30]}",
            f"Pivot: {safe_author}'s framework", 
            "Scale: Architecture"
        ],
        "quiz": [
            {"q": f"Insight from '{safe_title[:30]}'?", "a": "Book-specific"},
            {"q": "Author?", "a": safe_author},
            {"q": "Application?", "a": "Financial architecture"}
        ],
        "script_60s": f"AI insight from '{safe_title}' by {safe_author}.",
        "keywords": ["Multi-AI", safe_author.split()[0] if safe_author else "Strategy", actual_tier.name]
    }

def generate_sitemap(processed_ids):
    base_url = "https://pauljangtv-create.github.io/gutenberg-galaxy/"
    sitemap = ['<?xml version="1.0" encoding="UTF-8"?>', '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
    sitemap.append(f"<url><loc>{base_url}</loc><priority>1.0</priority></url>")
    for bid in list(processed_ids)[-5000:]:
        sitemap.append(f"<url><loc>{base_url}?id={bid}</loc><priority>0.8</priority></url>")
    sitemap.append('</urlset>')
    Path("sitemap.xml").write_text("\n".join(sitemap), encoding="utf-8")
    Path("robots.txt").write_text(f"User-agent: *\nAllow: /\nSitemap: {base_url}sitemap.xml", encoding="utf-8")

def main():
    # --- [HG3] COST GUARD START (DO NOT REMOVE) ---
    PAID_LLM_ENABLED = bool(GEMINI_API_KEY)
    MAX_TOTAL_COST = 10.0
    current_estimated_cost = 0.0
    
    if not GEMINI_API_KEY:
        logger.critical("🛑 GEMINI_API_KEY missing")
        sys.exit(1)
    
    if PAID_LLM_ENABLED and current_estimated_cost > MAX_TOTAL_COST:
        logger.critical("🛑 Cost exceeded")
        sys.exit(1)
    # --- [HG3] COST GUARD END ---

    logger.info("=" * 70)
    logger.info("🚀 Stable 2-AI System v2.2 (Claude + Gemini)")
    logger.info(f"🛡️ Cost: ${current_estimated_cost} / ${MAX_TOTAL_COST}")
    logger.info(f"🤖 AI Status:")
    logger.info(f"   Gemini: {'✅' if GEMINI_API_KEY else '❌'} ({GEMINI_DELAY}s delay)")
    logger.info(f"   Claude: {'✅' if ANTHROPIC_API_KEY else '❌'} ({CLAUDE_DELAY}s delay)")
    logger.info("=" * 70)

    queue = fetch_work_queue()
    processed_ids = list(load_processed_ids())
    
    if not queue:
        logger.info("No tasks")
        return

    logger.info(f"📋 Queue: {len(queue)} books")
    
    premium_cutoff = int(len(queue) * HIGH_VALUE_THRESHOLD)
    
    tier_counts = {"2-AI": 0, "1-AI": 0}
    failure_count = 0
    
    for idx, item in enumerate(queue):
        try:
            tier_priority = "PREMIUM" if idx < premium_cutoff else "STANDARD"
            
            logger.info("=" * 70)
            logger.info(f"🔄 [{idx+1}/{len(queue)}] {item['id']} - '{item['title'][:40]}'")
            logger.info(f"📊 Rank: #{item['rank']} | Priority: {tier_priority}")
            
            data = generate_asset(item['id'], item['title'], item['author'], item['subjects'], tier_priority)
            
            if data is None:
                logger.warning(f"⏭️ Skipped: {item['id']}")
                failure_count += 1
                continue
            
            validate(instance=data, schema=SCHEMA)
            
            file_path = OUT_DIR / f"{item['id']}.json.gz"
            with gzip.open(file_path, "wb") as f:
                f.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))
            
            processed_ids.append(item['id'])
            
            if "2-AI" in data['processing_tier']:
                tier_counts["2-AI"] += 1
            else:
                tier_counts["1-AI"] += 1
            
            logger.info(f"✅ Success ({data['processing_tier']})")
            logger.info(f"💡 {data['irreversible_insight'][:70]}...")
            
        except Exception as e:
            logger.error(f"💥 Error: {e}")
            failure_count += 1
            continue

    final_state = {"processed_ids": sorted(list(set(processed_ids)))}
    STATE_PATH.write_text(json.dumps(final_state, indent=2), encoding="utf-8")
    
    generate_sitemap(processed_ids)
    
    total_success = sum(tier_counts.values())
    logger.info("=" * 70)
    logger.info("🎉 Production Complete")
    logger.info(f"🏆 2-AI (Claude→Gemini): {tier_counts['2-AI']}")
    logger.info(f"⚡ 1-AI (Gemini): {tier_counts['1-AI']}")
    logger.info(f"❌ Failures: {failure_count}")
    logger.info(f"📊 Success Rate: {total_success/(total_success+failure_count)*100:.1f}%" if total_success+failure_count > 0 else "N/A")
    logger.info("=" * 70)

if __name__ == "__main__":
    main()
