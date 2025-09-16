"""
TMDB API를 활용한 실제 넷플릭스 데이터 수집
https://developer.themoviedb.org/docs/getting-started 참고
"""

import requests
import pandas as pd
import sqlite3
import time
from typing import List, Dict, Optional

class TMDBCollector:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.themoviedb.org/3"
        self.db_path = "database/netflix_analysis.db"
        self.headers = {
            'Content-Type': 'application/json'
        }
        self.api_key = api_key
        
    def get_netflix_movies(self, page: int = 1) -> List[Dict]:
        """넷플릭스에서 제공하는 영화 데이터 수집"""
        print(f"넷플릭스 영화 데이터 수집 중... (페이지 {page})")
        
        params = {
            'api_key': self.api_key,
            'with_watch_providers': '8',  # 넷플릭스 ID
            'watch_region': 'US',
            'sort_by': 'popularity.desc',
            'page': page,
            'language': 'en-US'
        }
        
        try:
            response = requests.get(
                f"{self.base_url}/discover/movie",
                headers=self.headers,
                params=params
            )
            
            if response.status_code == 200:
                data = response.json()
                movies = []
                
                for movie in data['results']:
                    movies.append({
                        'title': movie['title'],
                        'type': 'Movie',
                        'release_year': int(movie['release_date'][:4]) if movie['release_date'] else None,
                        'tmdb_score': movie['vote_average'],
                        'popularity': movie['popularity'],
                        'vote_count': movie['vote_count'],
                        'overview': movie['overview'],
                        'tmdb_id': movie['id'],
                        'original_language': movie['original_language'],
                        'adult': movie['adult']
                    })
                
                print(f"✓ {len(movies)}개 영화 수집 완료")
                return movies
            else:
                print(f"✗ API 오류: {response.status_code}")
                return []
                
        except Exception as e:
            print(f"✗ 오류 발생: {str(e)}")
            return []
    
    def get_netflix_tv_shows(self, page: int = 1) -> List[Dict]:
        """넷플릭스에서 제공하는 TV 쇼 데이터 수집"""
        print(f"넷플릭스 TV 쇼 데이터 수집 중... (페이지 {page})")
        
        params = {
            'api_key': self.api_key,
            'with_watch_providers': '8',  # 넷플릭스 ID
            'watch_region': 'US',
            'sort_by': 'popularity.desc',
            'page': page,
            'language': 'en-US'
        }
        
        try:
            response = requests.get(
                f"{self.base_url}/discover/tv",
                headers=self.headers,
                params=params
            )
            
            if response.status_code == 200:
                data = response.json()
                tv_shows = []
                
                for show in data['results']:
                    tv_shows.append({
                        'title': show['name'],
                        'type': 'TV Show',
                        'release_year': int(show['first_air_date'][:4]) if show['first_air_date'] else None,
                        'tmdb_score': show['vote_average'],
                        'popularity': show['popularity'],
                        'vote_count': show['vote_count'],
                        'overview': show['overview'],
                        'tmdb_id': show['id'],
                        'original_language': show['original_language'],
                        'adult': show['adult']
                    })
                
                print(f"✓ {len(tv_shows)}개 TV 쇼 수집 완료")
                return tv_shows
            else:
                print(f"✗ API 오류: {response.status_code}")
                return []
                
        except Exception as e:
            print(f"✗ 오류 발생: {str(e)}")
            return []
    
    def get_movie_details(self, movie_id: int) -> Dict:
        """영화 상세 정보 수집"""
        try:
            response = requests.get(
                f"{self.base_url}/movie/{movie_id}",
                headers=self.headers,
                params={'api_key': self.api_key, 'language': 'en-US'}
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                return {}
        except Exception as e:
            print(f"✗ 영화 상세 정보 오류: {str(e)}")
            return {}
    
    def get_tv_details(self, tv_id: int) -> Dict:
        """TV 쇼 상세 정보 수집"""
        try:
            response = requests.get(
                f"{self.base_url}/tv/{tv_id}",
                headers=self.headers,
                params={'api_key': self.api_key, 'language': 'en-US'}
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                return {}
        except Exception as e:
            print(f"✗ TV 쇼 상세 정보 오류: {str(e)}")
            return {}
    
    def get_genres(self) -> Dict:
        """장르 정보 수집"""
        try:
            response = requests.get(
                f"{self.base_url}/genre/movie/list",
                headers=self.headers,
                params={'api_key': self.api_key, 'language': 'en-US'}
            )
            
            if response.status_code == 200:
                data = response.json()
                return {genre['id']: genre['name'] for genre in data['genres']}
            else:
                return {}
        except Exception as e:
            print(f"✗ 장르 정보 오류: {str(e)}")
            return {}
    
    def enhance_content_data(self, content_list: List[Dict]) -> List[Dict]:
        """콘텐츠 데이터에 상세 정보 추가"""
        print("콘텐츠 상세 정보 수집 중...")
        
        genres = self.get_genres()
        enhanced_content = []
        
        for i, content in enumerate(content_list):
            print(f"진행률: {i+1}/{len(content_list)}")
            
            if content['type'] == 'Movie':
                details = self.get_movie_details(content['tmdb_id'])
            else:
                details = self.get_tv_details(content['tmdb_id'])
            
            if details:
                # 장르 정보 추가
                genre_ids = details.get('genres', [])
                genre_names = [genres.get(g['id'], g['name']) for g in genre_ids if g.get('id')]
                content['genre'] = ', '.join(genre_names)
                
                # 추가 정보
                content['runtime'] = details.get('runtime', 0)
                content['budget'] = details.get('budget', 0)
                content['revenue'] = details.get('revenue', 0)
                content['status'] = details.get('status', '')
                content['tagline'] = details.get('tagline', '')
                
                # 제작사 정보
                production_companies = details.get('production_companies', [])
                content['production_companies'] = ', '.join([p['name'] for p in production_companies])
                
                # 국가 정보
                production_countries = details.get('production_countries', [])
                content['production_countries'] = ', '.join([c['name'] for c in production_countries])
                
                # 언어 정보
                spoken_languages = details.get('spoken_languages', [])
                content['spoken_languages'] = ', '.join([l['name'] for l in spoken_languages])
            
            enhanced_content.append(content)
            
            # Rate limiting을 위한 지연
            time.sleep(0.1)
        
        return enhanced_content
    
    def save_to_database(self, content_data: List[Dict]):
        """데이터베이스에 저장"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 테이블 생성
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS netflix_content (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            type TEXT,
            genre TEXT,
            release_year INTEGER,
            tmdb_score REAL,
            popularity REAL,
            vote_count INTEGER,
            overview TEXT,
            tmdb_id INTEGER,
            original_language TEXT,
            adult BOOLEAN,
            runtime INTEGER,
            budget INTEGER,
            revenue INTEGER,
            status TEXT,
            tagline TEXT,
            production_companies TEXT,
            production_countries TEXT,
            spoken_languages TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # 기존 데이터 삭제
        cursor.execute("DELETE FROM netflix_content")
        
        # 데이터 저장
        for item in content_data:
            cursor.execute('''
            INSERT INTO netflix_content 
            (title, type, genre, release_year, tmdb_score, popularity, vote_count,
             overview, tmdb_id, original_language, adult, runtime, budget, revenue,
             status, tagline, production_companies, production_countries, spoken_languages)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                item.get('title', ''),
                item.get('type', ''),
                item.get('genre', ''),
                item.get('release_year'),
                item.get('tmdb_score', 0),
                item.get('popularity', 0),
                item.get('vote_count', 0),
                item.get('overview', ''),
                item.get('tmdb_id', 0),
                item.get('original_language', ''),
                item.get('adult', False),
                item.get('runtime', 0),
                item.get('budget', 0),
                item.get('revenue', 0),
                item.get('status', ''),
                item.get('tagline', ''),
                item.get('production_companies', ''),
                item.get('production_countries', ''),
                item.get('spoken_languages', '')
            ))
        
        conn.commit()
        conn.close()
        print(f"✓ {len(content_data)}개 콘텐츠가 데이터베이스에 저장되었습니다.")
    
    def collect_netflix_data(self, max_pages: int = 25):
        """넷플릭스 데이터 수집 메인 함수"""
        print("=" * 60)
        print("TMDB API를 활용한 넷플릭스 데이터 수집")
        print("=" * 60)
        
        all_content = []
        
        # 영화 데이터 수집
        for page in range(1, max_pages + 1):
            movies = self.get_netflix_movies(page)
            all_content.extend(movies)
            time.sleep(1)  # Rate limiting
        
        # TV 쇼 데이터 수집
        for page in range(1, max_pages + 1):
            tv_shows = self.get_netflix_tv_shows(page)
            all_content.extend(tv_shows)
            time.sleep(1)  # Rate limiting
        
        print(f"\n총 {len(all_content)}개 콘텐츠 수집 완료")
        
        # 상세 정보 추가
        enhanced_content = self.enhance_content_data(all_content)
        
        # 데이터베이스에 저장
        self.save_to_database(enhanced_content)
        
        return enhanced_content

if __name__ == "__main__":
    # API 키가 필요한 경우
    #api_key = input("TMDB API 키를 입력하세요 (없으면 Enter): ").strip()
    api_key = "c3d39497c4cd3856150953982cd5f353"

    if not api_key:
        print("API 키가 없습니다. 공개 데이터셋을 사용합니다.")
        # 공개 데이터셋 사용 로직
    else:
        collector = TMDBCollector(api_key)
        collector.collect_netflix_data()
