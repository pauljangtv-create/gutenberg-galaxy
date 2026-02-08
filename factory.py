import gzip
import json
import os
import re
from pathlib import Path
from jsonschema import validate
from sharding_logic import get_next_shard, save_state

# 설정 로드
SCHEMA = json.loads(Path("schema.json").read_text(encoding="utf-8"))
OUT_DIR = Path("products")
OUT_DIR.mkdir(exist_ok=True)

# HG3: 비용 가드 (기본 OFF)
PAID_LLM_ENABLED = os.environ.get("PAID_LLM_ENABLED", "0") == "1"

def sample_text(text, chunk=4000):
    """C: 3구간 샘플링 (Head/Middle/Tail)"""
    text = text.strip()
    if len(text) <= chunk * 3: return text
    return f"{text[:chunk]}\n...\n{text[len(text)//2-chunk//2 : len(text)//2+chunk//2]}\n...\n{text[-chunk:]}"

def generate_fallback(book_id):
    """무료 폴백 모드: 비용 0원 유지용 템플릿"""
    return {
        "book_id": book_id,
        "audience": "professional",
        "irreversible_insight": "Strategic focus: identify non-reversible costs before action.",
        "cards": ["Define constraints", "Assess irreversible loss", "Act on smallest step"],
        "quiz": [{"q": "What is the first step?", "a": "Define constraints"}],
        "script_60s": "Focus on what you cannot recover.",
        "keywords": ["strategy", "decision-making", "efficiency"]
    }

def process_book(path):
    book_id = path.stem
    text = path.read_text(encoding="utf-8", errors="ignore")
    sampled = sample_text(text)
    
    # [생산 로직] 현재는 비용 0원을 위해 폴백 모드 우선 가동
    data = generate_fallback(book_id)
    
    # HG2: 저장 전 검증
    validate(instance=data, schema=SCHEMA)
    
    # HG4: gzip 압축 저장
    out_path = OUT_DIR / f"{book_id}.json.gz"
    with gzip.open(out_path, "wb") as f:
        f.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))
    
    return book_id

def main():
    targets = get_next_shard()
    processed_ids = []
    
    for path in targets:
        try:
            bid = process_book(path)
            processed_ids.append(bid)
            print(f"✅ Produced: {bid}")
        except Exception as e:
            print(f"❌ Failed {path.name}: {e}")
            
    if processed_ids:
        save_state(processed_ids)
        print(f"🚀 Batch complete: {len(processed_ids)} books processed.")

if __name__ == "__main__":
    main()
