"""
API 키 설정 도우미 스크립트
"""

import sys
import os
sys.path.append('config')

from api_keys import show_api_guide, API_KEYS

def setup_api_keys():
    """API 키 설정 도우미"""
    print("=" * 60)
    print("넷플릭스 데이터 수집을 위한 API 키 설정")
    print("=" * 60)
    
    show_api_guide()
    
    print("\n" + "=" * 60)
    print("API 키 설정 방법")
    print("=" * 60)
    
    print("\n1. TMDB API 키 설정 (추천 - 무료)")
    print("   - https://www.themoviedb.org/settings/api 방문")
    print("   - 계정 생성 후 API 키 발급")
    print("   - config/api_keys.py 파일에서 'your_tmdb_api_key_here' 부분을 실제 키로 교체")
    
    print("\n2. OMDb API 키 설정 (선택사항 - 무료)")
    print("   - http://www.omdbapi.com/apikey.aspx 방문")
    print("   - 이메일로 API 키 발급")
    print("   - config/api_keys.py 파일에서 'your_omdb_api_key_here' 부분을 실제 키로 교체")
    
    print("\n3. JustWatch API 키 설정 (선택사항 - 유료)")
    print("   - https://apis.justwatch.com/ 방문")
    print("   - 유료 구독 후 API 키 발급")
    print("   - config/api_keys.py 파일에서 'your_justwatch_api_key_here' 부분을 실제 키로 교체")
    
    print("\n" + "=" * 60)
    print("현재 설정 상태")
    print("=" * 60)
    
    for service, key in API_KEYS.items():
        status = "✅ 설정됨" if key and key != f'your_{service}_api_key_here' else "❌ 설정 안됨"
        print(f"{service.upper()}: {status}")
    
    print("\n" + "=" * 60)
    print("다음 단계")
    print("=" * 60)
    print("1. 위의 가이드를 따라 API 키를 설정하세요")
    print("2. python src/data_collection/api_data_collector.py 실행")
    print("3. API 키가 없어도 공개 데이터셋으로 데이터 수집이 가능합니다")

if __name__ == "__main__":
    setup_api_keys()



