"""
향상된 TMDB API 데이터 수집기
- 더 많은 페이지 수집
- 다양한 정렬 기준
- 추가 데이터 소스
"""

import requests
import pandas as pd
import sqlite3
import time
import random
from typing import List, Dict, Optional

class EnhancedTMDBCollector:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.themoviedb.org/3"
        self.db_path = "database/netflix_analysis.db"
        self.headers = {
            'Content-Type': 'application/json'
        }
        
    def get_netflix_content_by_sort(self, content_type: str, sort_by: str, max_pages: int = 10) -> List[Dict]:
        """다양한 정렬 기준으로 넷플릭스 콘텐츠 수집"""
        print(f"넷플릭스 {content_type} 수집 중... (정렬: {sort_by}, 페이지: {max_pages})")
        
        all_content = []
        
        for page in range(1, max_pages + 1):
            params = {
                'api_key': self.api_key,
                'with_watch_providers': '8',  # 넷플릭스 ID
                'watch_region': 'US',
                'sort_by': sort_by,
                'page': page,
                'language': 'en-US'
            }
            
            try:
                if content_type == 'movie':
                    response = requests.get(f"{self.base_url}/discover/movie", headers=self.headers, params=params)
                else:
                    response = requests.get(f"{self.base_url}/discover/tv", headers=self.headers, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    content_list = []
                    
                    for item in data['results']:
                        content = {
                            'title': item.get('title', item.get('name', '')),
                            'type': 'Movie' if content_type == 'movie' else 'TV Show',
                            'release_year': int(item.get('release_date', item.get('first_air_date', ''))[:4]) if item.get('release_date', item.get('first_air_date', '')) else None,
                            'tmdb_score': item.get('vote_average', 0),
                            'popularity': item.get('popularity', 0),
                            'vote_count': item.get('vote_count', 0),
                            'overview': item.get('overview', ''),
                            'tmdb_id': item.get('id', 0),
                            'original_language': item.get('original_language', ''),
                            'adult': item.get('adult', False)
                        }
                        content_list.append(content)
                    
                    all_content.extend(content_list)
                    print(f"  페이지 {page}: {len(content_list)}개 수집")
                else:
                    print(f"  페이지 {page}: API 오류 {response.status_code}")
                
                # Rate limiting
                time.sleep(0.5)
                
            except Exception as e:
                print(f"  페이지 {page}: 오류 {str(e)}")
        
        print(f"✓ {content_type} 총 {len(all_content)}개 수집 완료")
        return all_content
    
    def get_trending_content(self, max_pages: int = 5) -> List[Dict]:
        """트렌딩 콘텐츠 수집"""
        print(f"트렌딩 콘텐츠 수집 중... (페이지: {max_pages})")
        
        all_content = []
        
        # 트렌딩 영화
        for page in range(1, max_pages + 1):
            params = {
                'api_key': self.api_key,
                'page': page,
                'language': 'en-US'
            }
            
            try:
                response = requests.get(f"{self.base_url}/trending/movie/week", headers=self.headers, params=params)
                if response.status_code == 200:
                    data = response.json()
                    for item in data['results']:
                        content = {
                            'title': item.get('title', ''),
                            'type': 'Movie',
                            'release_year': int(item.get('release_date', '')[:4]) if item.get('release_date') else None,
                            'tmdb_score': item.get('vote_average', 0),
                            'popularity': item.get('popularity', 0),
                            'vote_count': item.get('vote_count', 0),
                            'overview': item.get('overview', ''),
                            'tmdb_id': item.get('id', 0),
                            'original_language': item.get('original_language', ''),
                            'adult': item.get('adult', False)
                        }
                        all_content.append(content)
                
                time.sleep(0.5)
            except Exception as e:
                print(f"트렌딩 영화 페이지 {page} 오류: {str(e)}")
        
        # 트렌딩 TV 쇼
        for page in range(1, max_pages + 1):
            params = {
                'api_key': self.api_key,
                'page': page,
                'language': 'en-US'
            }
            
            try:
                response = requests.get(f"{self.base_url}/trending/tv/week", headers=self.headers, params=params)
                if response.status_code == 200:
                    data = response.json()
                    for item in data['results']:
                        content = {
                            'title': item.get('name', ''),
                            'type': 'TV Show',
                            'release_year': int(item.get('first_air_date', '')[:4]) if item.get('first_air_date') else None,
                            'tmdb_score': item.get('vote_average', 0),
                            'popularity': item.get('popularity', 0),
                            'vote_count': item.get('vote_count', 0),
                            'overview': item.get('overview', ''),
                            'tmdb_id': item.get('id', 0),
                            'original_language': item.get('original_language', ''),
                            'adult': item.get('adult', False)
                        }
                        all_content.append(content)
                
                time.sleep(0.5)
            except Exception as e:
                print(f"트렌딩 TV 페이지 {page} 오류: {str(e)}")
        
        print(f"✓ 트렌딩 콘텐츠 총 {len(all_content)}개 수집 완료")
        return all_content
    
    def get_popular_content(self, max_pages: int = 5) -> List[Dict]:
        """인기 콘텐츠 수집"""
        print(f"인기 콘텐츠 수집 중... (페이지: {max_pages})")
        
        all_content = []
        
        # 인기 영화
        for page in range(1, max_pages + 1):
            params = {
                'api_key': self.api_key,
                'page': page,
                'language': 'en-US'
            }
            
            try:
                response = requests.get(f"{self.base_url}/movie/popular", headers=self.headers, params=params)
                if response.status_code == 200:
                    data = response.json()
                    for item in data['results']:
                        content = {
                            'title': item.get('title', ''),
                            'type': 'Movie',
                            'release_year': int(item.get('release_date', '')[:4]) if item.get('release_date') else None,
                            'tmdb_score': item.get('vote_average', 0),
                            'popularity': item.get('popularity', 0),
                            'vote_count': item.get('vote_count', 0),
                            'overview': item.get('overview', ''),
                            'tmdb_id': item.get('id', 0),
                            'original_language': item.get('original_language', ''),
                            'adult': item.get('adult', False)
                        }
                        all_content.append(content)
                
                time.sleep(0.5)
            except Exception as e:
                print(f"인기 영화 페이지 {page} 오류: {str(e)}")
        
        # 인기 TV 쇼
        for page in range(1, max_pages + 1):
            params = {
                'api_key': self.api_key,
                'page': page,
                'language': 'en-US'
            }
            
            try:
                response = requests.get(f"{self.base_url}/tv/popular", headers=self.headers, params=params)
                if response.status_code == 200:
                    data = response.json()
                    for item in data['results']:
                        content = {
                            'title': item.get('name', ''),
                            'type': 'TV Show',
                            'release_year': int(item.get('first_air_date', '')[:4]) if item.get('first_air_date') else None,
                            'tmdb_score': item.get('vote_average', 0),
                            'popularity': item.get('popularity', 0),
                            'vote_count': item.get('vote_count', 0),
                            'overview': item.get('overview', ''),
                            'tmdb_id': item.get('id', 0),
                            'original_language': item.get('original_language', ''),
                            'adult': item.get('adult', False)
                        }
                        all_content.append(content)
                
                time.sleep(0.5)
            except Exception as e:
                print(f"인기 TV 페이지 {page} 오류: {str(e)}")
        
        print(f"✓ 인기 콘텐츠 총 {len(all_content)}개 수집 완료")
        return all_content
    
    def get_content_by_genre(self, genre_id: int, content_type: str, max_pages: int = 3) -> List[Dict]:
        """장르별 콘텐츠 수집"""
        print(f"장르 {genre_id} {content_type} 수집 중... (페이지: {max_pages})")
        
        all_content = []
        
        for page in range(1, max_pages + 1):
            params = {
                'api_key': self.api_key,
                'with_genres': str(genre_id),
                'page': page,
                'language': 'en-US'
            }
            
            try:
                if content_type == 'movie':
                    response = requests.get(f"{self.base_url}/discover/movie", headers=self.headers, params=params)
                else:
                    response = requests.get(f"{self.base_url}/discover/tv", headers=self.headers, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    for item in data['results']:
                        content = {
                            'title': item.get('title', item.get('name', '')),
                            'type': 'Movie' if content_type == 'movie' else 'TV Show',
                            'release_year': int(item.get('release_date', item.get('first_air_date', ''))[:4]) if item.get('release_date', item.get('first_air_date', '')) else None,
                            'tmdb_score': item.get('vote_average', 0),
                            'popularity': item.get('popularity', 0),
                            'vote_count': item.get('vote_count', 0),
                            'overview': item.get('overview', ''),
                            'tmdb_id': item.get('id', 0),
                            'original_language': item.get('original_language', ''),
                            'adult': item.get('adult', False)
                        }
                        all_content.append(content)
                
                time.sleep(0.5)
            except Exception as e:
                print(f"장르 {genre_id} 페이지 {page} 오류: {str(e)}")
        
        print(f"✓ 장르 {genre_id} {content_type} 총 {len(all_content)}개 수집 완료")
        return all_content
    
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
            if i % 50 == 0:
                print(f"진행률: {i+1}/{len(content_list)}")
            
            try:
                if content['type'] == 'Movie':
                    response = requests.get(
                        f"{self.base_url}/movie/{content['tmdb_id']}",
                        headers=self.headers,
                        params={'api_key': self.api_key, 'language': 'en-US'}
                    )
                else:
                    response = requests.get(
                        f"{self.base_url}/tv/{content['tmdb_id']}",
                        headers=self.headers,
                        params={'api_key': self.api_key, 'language': 'en-US'}
                    )
                
                if response.status_code == 200:
                    details = response.json()
                    
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
                else:
                    # 기본값 설정
                    content['genre'] = ''
                    content['runtime'] = 0
                    content['budget'] = 0
                    content['revenue'] = 0
                    content['status'] = ''
                    content['tagline'] = ''
                    content['production_companies'] = ''
                    content['production_countries'] = ''
                    content['spoken_languages'] = ''
                
                enhanced_content.append(content)
                
                # Rate limiting
                time.sleep(0.1)
                
            except Exception as e:
                print(f"상세 정보 수집 오류 (ID: {content['tmdb_id']}): {str(e)}")
                # 기본값으로 추가
                content['genre'] = ''
                content['runtime'] = 0
                content['budget'] = 0
                content['revenue'] = 0
                content['status'] = ''
                content['tagline'] = ''
                content['production_companies'] = ''
                content['production_countries'] = ''
                content['spoken_languages'] = ''
                enhanced_content.append(content)
        
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
    
    def collect_comprehensive_data(self):
        """종합적인 데이터 수집"""
        print("=" * 60)
        print("향상된 TMDB API를 활용한 대규모 데이터 수집")
        print("=" * 60)
        
        all_content = []
        
        # 1. 넷플릭스 콘텐츠 (다양한 정렬 기준)
        print("\n1. 넷플릭스 콘텐츠 수집...")
        sort_criteria = ['popularity.desc', 'vote_average.desc', 'release_date.desc', 'vote_count.desc']
        
        for sort_by in sort_criteria:
            # 영화
            movies = self.get_netflix_content_by_sort('movie', sort_by, 5)
            all_content.extend(movies)
            
            # TV 쇼
            tv_shows = self.get_netflix_content_by_sort('tv', sort_by, 5)
            all_content.extend(tv_shows)
        
        # 2. 트렌딩 콘텐츠
        print("\n2. 트렌딩 콘텐츠 수집...")
        trending_content = self.get_trending_content(5)
        all_content.extend(trending_content)
        
        # 3. 인기 콘텐츠
        print("\n3. 인기 콘텐츠 수집...")
        popular_content = self.get_popular_content(5)
        all_content.extend(popular_content)
        
        # 4. 장르별 콘텐츠 (주요 장르)
        print("\n4. 장르별 콘텐츠 수집...")
        popular_genres = [28, 12, 16, 35, 80, 18, 10751, 14, 36, 27]  # Action, Adventure, Animation, Comedy, Crime, Drama, Family, Fantasy, History, Horror
        
        for genre_id in popular_genres:
            # 영화
            genre_movies = self.get_content_by_genre(genre_id, 'movie', 2)
            all_content.extend(genre_movies)
            
            # TV 쇼
            genre_tv = self.get_content_by_genre(genre_id, 'tv', 2)
            all_content.extend(genre_tv)
        
        # 중복 제거
        print(f"\n중복 제거 전: {len(all_content)}개")
        unique_content = []
        seen_ids = set()
        
        for content in all_content:
            if content['tmdb_id'] not in seen_ids:
                unique_content.append(content)
                seen_ids.add(content['tmdb_id'])
        
        print(f"중복 제거 후: {len(unique_content)}개")
        
        # 상세 정보 추가
        print(f"\n상세 정보 수집 중...")
        enhanced_content = self.enhance_content_data(unique_content)
        
        # 데이터베이스에 저장
        self.save_to_database(enhanced_content)
        
        return enhanced_content

if __name__ == "__main__":
    api_key = "c3d39497c4cd3856150953982cd5f353"
    collector = EnhancedTMDBCollector(api_key)
    collector.collect_comprehensive_data()







