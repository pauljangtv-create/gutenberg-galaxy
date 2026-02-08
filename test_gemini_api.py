#!/usr/bin/env python3
"""
Gemini API 키 테스트 스크립트
실행: python test_gemini_api.py
"""

import os
import requests
import json

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

print("=" * 60)
print("🔍 Gemini API Key Diagnostic Test")
print("=" * 60)

if not GEMINI_API_KEY:
    print("❌ GEMINI_API_KEY environment variable not found!")
    print("💡 Set it with: export GEMINI_API_KEY='your-key-here'")
    exit(1)

print(f"✅ API Key found: {GEMINI_API_KEY[:10]}...{GEMINI_API_KEY[-5:]}")
print()

# Test 1: List available models
print("📋 Test 1: Checking available models...")
list_url = f"https://generativelanguage.googleapis.com/v1beta/models?key={GEMINI_API_KEY}"

try:
    response = requests.get(list_url, timeout=10)
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        models = response.json().get('models', [])
        print(f"✅ Found {len(models)} available models:")
        for model in models[:5]:  # Show first 5
            print(f"  - {model.get('name', 'N/A')}")
    else:
        print(f"❌ Error: {response.status_code}")
        print(f"Response: {response.text[:200]}")
except Exception as e:
    print(f"❌ Connection Error: {e}")

print()

# Test 2: Try different model endpoints
print("🧪 Test 2: Testing different model endpoints...")

test_models = [
    "gemini-pro",
    "gemini-1.5-pro",
    "gemini-1.5-flash",
    "gemini-1.5-flash-latest"
]

for model_name in test_models:
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={GEMINI_API_KEY}"
    
    payload = {
        "contents": [{
            "parts": [{
                "text": "Say hello in one word"
            }]
        }]
    }
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        
        if response.status_code == 200:
            result = response.json()
            text = result['candidates'][0]['content']['parts'][0]['text']
            print(f"✅ {model_name}: SUCCESS - '{text.strip()}'")
        else:
            print(f"❌ {model_name}: {response.status_code} - {response.text[:100]}")
    except Exception as e:
        print(f"❌ {model_name}: Exception - {str(e)[:100]}")

print()
print("=" * 60)
print("🏁 Diagnostic Complete")
print("=" * 60)
