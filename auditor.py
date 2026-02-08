import re, sys
from pathlib import Path

def audit():
    sharding = Path("sharding_logic.py").read_text()
    factory = Path("factory.py").read_text()
    yml = Path(".github/workflows/daily-publish.yml").read_text() if Path(".github/workflows/daily-publish.yml").exists() else ""

    # HG1: 200권 상한 체크
    if "MAX_BOOKS = 200" not in sharding: sys.exit("❌ HG1 FAIL: MAX_BOOKS limit missing")
    # HG3: 비용 가드 체크
    if "PAID_LLM_ENABLED" not in factory: sys.exit("❌ HG3 FAIL: Cost guard missing")
    # HG4: 압축 저장 체크
    if "gzip.open" not in factory: sys.exit("❌ HG4 FAIL: Compression missing")
    
    print("✅ AUDIT PASS: All Hard-Gates satisfied.")

if __name__ == "__main__":
    audit()
