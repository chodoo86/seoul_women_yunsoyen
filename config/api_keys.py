"""
API 키 설정 파일
실제 사용시에는 아래 키들을 설정하세요
"""

# API 키 설정
API_KEYS = {
    # TMDB API 키 (무료)
    # https://www.themoviedb.org/settings/api 에서 발급
    'tmdb': None,  # 'your_tmdb_api_key_here'
    
    # OMDb API 키 (무료)
    # http://www.omdbapi.com/apikey.aspx 에서 발급
    'omdb': None,  # 'your_omdb_api_key_here'
    
    # JustWatch API 키 (유료)
    # https://apis.justwatch.com/ 에서 발급
    'justwatch': None  # 'your_justwatch_api_key_here'
}

# API 키 사용 안내
API_GUIDE = {
    'tmdb': {
        'name': 'The Movie Database (TMDB)',
        'url': 'https://www.themoviedb.org/settings/api',
        'free': True,
        'description': '영화 및 TV 쇼 정보, 넷플릭스 콘텐츠 검색 가능'
    },
    'omdb': {
        'name': 'Open Movie Database (OMDb)',
        'url': 'http://www.omdbapi.com/apikey.aspx',
        'free': True,
        'description': 'IMDB 데이터 기반 영화 정보'
    },
    'justwatch': {
        'name': 'JustWatch API',
        'url': 'https://apis.justwatch.com/',
        'free': False,
        'description': '스트리밍 서비스별 콘텐츠 정보'
    }
}

def get_api_key(service: str) -> str:
    """API 키 반환"""
    return API_KEYS.get(service)

def show_api_guide():
    """API 키 발급 가이드 출력"""
    print("=" * 60)
    print("API 키 발급 가이드")
    print("=" * 60)
    
    for service, info in API_GUIDE.items():
        print(f"\n{info['name']} ({'무료' if info['free'] else '유료'})")
        print(f"URL: {info['url']}")
        print(f"설명: {info['description']}")
        print(f"현재 키: {'설정됨' if API_KEYS[service] else '설정 안됨'}")
        print("-" * 40)







