import json
import os
from pathlib import Path

MAX_BOOKS = 200
STATE_PATH = Path("state.json")
# 경로를 더 확실하게 인식하도록 수정
SRC_DIR = Path(".") / "gutenberg_txt" 

def load_state():
    if not STATE_PATH.exists(): return set()
    try:
        data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        return set(data.get("processed_ids", []))
    except: return set()

def save_state(processed_ids):
    existing = load_state()
    merged = existing.union(processed_ids)
    STATE_PATH.write_text(json.dumps({"processed_ids": sorted(list(merged))}, indent=2))

def get_next_shard():
    processed = load_state()
    # 폴더가 없으면 생성하고 빈 리스트 반환
    if not SRC_DIR.exists():
        print(f"⚠️ Folder not found: {SRC_DIR.absolute()}")
        return []
        
    # 모든 txt 파일을 찾음 (대소문자 구분 없이)
    all_books = sorted(list(SRC_DIR.glob("*.txt")))
    print(f"🔍 Found {len(all_books)} total books in {SRC_DIR}")
    
    candidates = [p for p in all_books if p.stem not in processed]
    print(f"🎯 Candidates after filtering: {len(candidates)}")
    
    return candidates[:MAX_BOOKS]
