import json
import os
from pathlib import Path

# HG1: Daily hard cap for free tier safety
MAX_BOOKS = 200
STATE_PATH = Path("state.json")
SRC_DIR = Path("gutenberg_txt")

def load_state():
    """로드: 이미 처리된 도서 ID 목록을 가져옴"""
    if not STATE_PATH.exists():
        return set()
    try:
        data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        return set(data.get("processed_ids", []))
    except:
        return set()

def save_state(processed_ids):
    """저장: 처리 완료된 목록을 원자적으로 기록 (P1 패치 적용)"""
    existing = load_state()
    merged = existing.union(processed_ids)
    STATE_PATH.write_text(
        json.dumps({"processed_ids": sorted(list(merged))}, indent=2), 
        encoding="utf-8"
    )

def get_next_shard():
    """샤딩: 처리되지 않은 다음 200권의 경로 리스트 반환"""
    processed = load_state()
    if not SRC_DIR.exists():
        SRC_DIR.mkdir(exist_ok=True)
        return []
        
    all_books = sorted(list(SRC_DIR.glob("*.txt")))
    candidates = [p for p in all_books if p.stem not in processed]
    
    return candidates[:MAX_BOOKS]
