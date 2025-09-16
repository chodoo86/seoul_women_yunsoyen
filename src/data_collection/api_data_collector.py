"""
API를 활용한 실제 넷플릭스 데이터 수집 모듈
- TMDB API (The Movie Database)
- OMDb API (Open Movie Database)
- JustWatch API
- 실제 공개 데이터셋 다운로드
"""

import requests
import pandas as pd
import json
import sqlite3
import time
import os
from datetime import datetime
import zipfile
from typing import List, Dict, Optional

class APIDataCollector:
    def __init__(self):
        self.db_path = "database/netflix_analysis.db"
        self.data_dir = "data/raw"
        self.api_keys = self.load_api_keys()
        self.setup_database()
        
    def load_api_keys(self) -> Dict[str, Optional[str]]:
        """API 키 로드"""
        # config/api_keys.py에서 로드
        try:
            import sys
            sys.path.append('../../config')
            from api_keys import API_KEYS
            return API_KEYS
        except ImportError:
            # 환경변수에서 로드 (대안)
            return {
                'tmdb': os.getenv('TMDB_API_KEY'),
                'omdb': os.getenv('OMDB_API_KEY'),
                'justwatch': os.getenv('JUSTWATCH_API_KEY')
            }
    
    def setup_database(self):
        """데이터베이스 초기화"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 넷플릭스 콘텐츠 테이블
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS netflix_content (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            type TEXT,
            genre TEXT,
            release_year INTEGER,
            rating TEXT,
            duration TEXT,
            description TEXT,
            cast TEXT,
            director TEXT,
            country TEXT,
            language TEXT,
            imdb_score REAL,
            tmdb_score REAL,
            popularity REAL,
            vote_count INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # 사용자 선호도 테이블
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_preferences (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT,
            content_id INTEGER,
            region TEXT,
            age_group TEXT,
            gender TEXT,
            ethnicity TEXT,
            rating REAL,
            watch_time INTEGER,
            completion_rate REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (content_id) REFERENCES netflix_content (id)
        )
        ''')
        
        conn.commit()
        conn.close()
        print("✓ 데이터베이스가 초기화되었습니다.")
    
    def collect_from_tmdb_api(self) -> List[Dict]:
        """TMDB API에서 넷플릭스 콘텐츠 데이터 수집"""
        print("TMDB API에서 데이터 수집 중...")
        
        if not self.api_keys['tmdb']:
            print("⚠️  TMDB API 키가 없습니다.")
            print("   무료 API 키를 발급받으세요: https://www.themoviedb.org/settings/api")
            return []
        
        tmdb_url = "https://api.themoviedb.org/3"
        headers = {
            'Authorization': f'Bearer {self.api_keys["tmdb"]}',
            'Content-Type': 'application/json'
        }
        
        content_data = []
        
        try:
            # 넷플릭스 콘텐츠 검색 (Discover API 사용)
            params = {
                'with_watch_providers': '8',  # 넷플릭스 ID
                'watch_region': 'US',
                'sort_by': 'popularity.desc',
                'page': 1
            }
            
            # 영화 데이터 수집
            movie_response = requests.get(f"{tmdb_url}/discover/movie", 
                                        headers=headers, params=params)
            if movie_response.status_code == 200:
                movies = movie_response.json()['results']
                for movie in movies[:20]:  # 상위 20개
                    content_data.append({
                        'title': movie['title'],
                        'type': 'Movie',
                        'release_year': int(movie['release_date'][:4]) if movie['release_date'] else None,
                        'tmdb_score': movie['vote_average'],
                        'popularity': movie['popularity'],
                        'vote_count': movie['vote_count'],
                        'overview': movie['overview'],
                        'tmdb_id': movie['id']
                    })
            
            # TV 쇼 데이터 수집
            tv_response = requests.get(f"{tmdb_url}/discover/tv", 
                                     headers=headers, params=params)
            if tv_response.status_code == 200:
                tv_shows = tv_response.json()['results']
                for show in tv_shows[:20]:  # 상위 20개
                    content_data.append({
                        'title': show['name'],
                        'type': 'TV Show',
                        'release_year': int(show['first_air_date'][:4]) if show['first_air_date'] else None,
                        'tmdb_score': show['vote_average'],
                        'popularity': show['popularity'],
                        'vote_count': show['vote_count'],
                        'overview': show['overview'],
                        'tmdb_id': show['id']
                    })
            
            print(f"✓ TMDB에서 {len(content_data)}개 콘텐츠 수집 완료")
            
        except Exception as e:
            print(f"✗ TMDB API 오류: {str(e)}")
        
        return content_data
    
    def collect_from_omdb_api(self, content_data: List[Dict]) -> List[Dict]:
        """OMDb API에서 추가 정보 수집"""
        print("OMDb API에서 추가 정보 수집 중...")
        
        if not self.api_keys['omdb']:
            print("⚠️  OMDb API 키가 없습니다.")
            print("   무료 API 키를 발급받으세요: http://www.omdbapi.com/apikey.aspx")
            return content_data
        
        omdb_url = "http://www.omdbapi.com/"
        
        for i, content in enumerate(content_data):
            try:
                params = {
                    'apikey': self.api_keys['omdb'],
                    't': content['title'],
                    'y': content['release_year']
                }
                
                response = requests.get(omdb_url, params=params)
                if response.status_code == 200:
                    data = response.json()
                    if data.get('Response') == 'True':
                        # OMDb 데이터로 업데이트
                        content['imdb_score'] = float(data.get('imdbRating', 0)) if data.get('imdbRating') != 'N/A' else 0
                        content['genre'] = data.get('Genre', '')
                        content['director'] = data.get('Director', '')
                        content['cast'] = data.get('Actors', '')
                        content['country'] = data.get('Country', '')
                        content['language'] = data.get('Language', '')
                        content['rating'] = data.get('Rated', '')
                        content['duration'] = data.get('Runtime', '')
                        content['description'] = data.get('Plot', '')
                
                # API 호출 제한을 위한 지연
                time.sleep(0.1)
                
            except Exception as e:
                print(f"✗ OMDb API 오류 (제목: {content['title']}): {str(e)}")
        
        print(f"✓ OMDb에서 {len(content_data)}개 콘텐츠 정보 업데이트 완료")
        return content_data
    
    def collect_from_public_datasets(self) -> List[Dict]:
        """공개 데이터셋에서 데이터 수집"""
        print("공개 데이터셋에서 데이터 수집 중...")
        
        # Kaggle 데이터셋 URL들
        kaggle_datasets = [
            {
                'name': 'netflix-titles',
                'url': 'https://raw.githubusercontent.com/ruchi798/Netflix-Tv-Shows-and-Movies/master/netflix_titles.csv',
                'description': 'Netflix Titles Dataset'
            }
        ]
        
        collected_data = []
        
        for dataset in kaggle_datasets:
            try:
                print(f"다운로드 중: {dataset['name']}")
                response = requests.get(dataset['url'], timeout=30)
                
                if response.status_code == 200:
                    df = pd.read_csv(pd.StringIO(response.text))
                    
                    # DataFrame을 딕셔너리 리스트로 변환
                    for _, row in df.iterrows():
                        collected_data.append({
                            'title': row.get('title', ''),
                            'type': row.get('type', ''),
                            'genre': row.get('listed_in', ''),
                            'release_year': row.get('release_year', None),
                            'rating': row.get('rating', ''),
                            'duration': row.get('duration', ''),
                            'description': row.get('description', ''),
                            'cast': row.get('cast', ''),
                            'director': row.get('director', ''),
                            'country': row.get('country', ''),
                            'language': row.get('language', ''),
                            'imdb_score': 0,  # 기본값
                            'tmdb_score': 0,  # 기본값
                            'popularity': 0,  # 기본값
                            'vote_count': 0   # 기본값
                        })
                    
                    print(f"✓ {dataset['name']} 다운로드 완료: {len(df)} 행")
                else:
                    print(f"✗ {dataset['name']} 다운로드 실패: {response.status_code}")
                    
            except Exception as e:
                print(f"✗ {dataset['name']} 다운로드 오류: {str(e)}")
        
        return collected_data
    
    def collect_from_justwatch_api(self) -> List[Dict]:
        """JustWatch API에서 넷플릭스 콘텐츠 수집"""
        print("JustWatch API에서 데이터 수집 중...")
        
        if not self.api_keys['justwatch']:
            print("⚠️  JustWatch API 키가 없습니다.")
            print("   API 키를 발급받으세요: https://apis.justwatch.com/")
            return []
        
        justwatch_url = "https://apis.justwatch.com/content/titles"
        
        try:
            params = {
                'providers': '8',  # 넷플릭스 ID
                'page_size': 50,
                'page': 1
            }
            
            response = requests.get(justwatch_url, params=params)
            if response.status_code == 200:
                data = response.json()
                content_data = []
                
                for item in data.get('items', [])[:20]:  # 상위 20개
                    content_data.append({
                        'title': item.get('title', ''),
                        'type': 'Movie' if item.get('object_type') == 'movie' else 'TV Show',
                        'release_year': item.get('original_release_year'),
                        'tmdb_score': item.get('tmdb_popularity', 0),
                        'popularity': item.get('popularity', 0),
                        'vote_count': item.get('vote_count', 0),
                        'overview': item.get('short_description', ''),
                        'justwatch_id': item.get('id')
                    })
                
                print(f"✓ JustWatch에서 {len(content_data)}개 콘텐츠 수집 완료")
                return content_data
            else:
                print(f"✗ JustWatch API 오류: {response.status_code}")
                return []
                
        except Exception as e:
            print(f"✗ JustWatch API 오류: {str(e)}")
            return []
    
    def save_to_database(self, content_data: List[Dict]):
        """수집된 데이터를 데이터베이스에 저장"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 기존 데이터 삭제
        cursor.execute("DELETE FROM netflix_content")
        
        # 새로운 데이터 저장
        for item in content_data:
            cursor.execute('''
            INSERT INTO netflix_content 
            (title, type, genre, release_year, rating, duration, description, 
             cast, director, country, language, imdb_score, tmdb_score, popularity, vote_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                item.get('title', ''),
                item.get('type', ''),
                item.get('genre', ''),
                item.get('release_year'),
                item.get('rating', ''),
                item.get('duration', ''),
                item.get('description', ''),
                item.get('cast', ''),
                item.get('director', ''),
                item.get('country', ''),
                item.get('language', ''),
                item.get('imdb_score', 0),
                item.get('tmdb_score', 0),
                item.get('popularity', 0),
                item.get('vote_count', 0)
            ))
        
        conn.commit()
        conn.close()
        print(f"✓ {len(content_data)}개 콘텐츠가 데이터베이스에 저장되었습니다.")
    
    def generate_user_preferences(self, content_data: List[Dict], num_users: int = 1000) -> List[Dict]:
        """사용자 선호도 데이터 생성"""
        print(f"{num_users}명의 사용자 선호도 데이터 생성 중...")
        
        regions = ['North America', 'Europe', 'Asia', 'South America', 'Africa', 'Oceania']
        age_groups = ['18-24', '25-34', '35-44', '45-54', '55-64', '65+']
        genders = ['Male', 'Female', 'Other']
        ethnicities = ['White', 'Black', 'Hispanic', 'Asian', 'Other']
        
        preferences = []
        
        for i in range(num_users):
            user_id = f"user_{i+1:04d}"
            region = random.choice(regions)
            age_group = random.choice(age_groups)
            gender = random.choice(genders)
            ethnicity = random.choice(ethnicities)
            
            # 각 사용자가 2-5개의 콘텐츠를 시청
            num_watched = random.randint(2, 5)
            watched_content = random.sample(content_data, min(num_watched, len(content_data)))
            
            for content in watched_content:
                # 평점 계산 (TMDB 점수 기반)
                base_rating = content.get('tmdb_score', 5) / 2  # 5점 만점으로 변환
                rating = max(1.0, min(5.0, base_rating + random.uniform(-1.0, 1.0)))
                
                # 시청 시간
                watch_time = random.randint(30, 180)
                
                # 완료율
                completion_rate = random.uniform(0.3, 1.0)
                
                preferences.append({
                    'user_id': user_id,
                    'content_id': content_data.index(content) + 1,
                    'region': region,
                    'age_group': age_group,
                    'gender': gender,
                    'ethnicity': ethnicity,
                    'rating': round(rating, 1),
                    'watch_time': watch_time,
                    'completion_rate': round(completion_rate, 2)
                })
        
        # 데이터베이스에 저장
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("DELETE FROM user_preferences")
        
        for item in preferences:
            cursor.execute('''
            INSERT INTO user_preferences 
            (user_id, content_id, region, age_group, gender, ethnicity, 
             rating, watch_time, completion_rate)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                item['user_id'], item['content_id'], item['region'],
                item['age_group'], item['gender'], item['ethnicity'],
                item['rating'], item['watch_time'], item['completion_rate']
            ))
        
        conn.commit()
        conn.close()
        
        print(f"✓ {len(preferences)}개 선호도 데이터가 생성되었습니다.")
        return preferences
    
    def collect_all_data(self):
        """모든 소스에서 데이터 수집"""
        print("=" * 60)
        print("API를 활용한 실제 넷플릭스 데이터 수집")
        print("=" * 60)
        
        all_content = []
        
        # 1. TMDB API에서 데이터 수집
        tmdb_data = self.collect_from_tmdb_api()
        if tmdb_data:
            all_content.extend(tmdb_data)
        
        # 2. OMDb API로 추가 정보 수집
        if all_content and self.api_keys['omdb']:
            all_content = self.collect_from_omdb_api(all_content)
        
        # 3. 공개 데이터셋에서 데이터 수집
        public_data = self.collect_from_public_datasets()
        if public_data:
            all_content.extend(public_data)
        
        # 4. JustWatch API에서 데이터 수집
        justwatch_data = self.collect_from_justwatch_api()
        if justwatch_data:
            all_content.extend(justwatch_data)
        
        # 5. 데이터베이스에 저장
        if all_content:
            self.save_to_database(all_content)
            
            # 6. 사용자 선호도 데이터 생성
            self.generate_user_preferences(all_content, 1000)
            
            print(f"\n✅ 총 {len(all_content)}개의 실제 콘텐츠 데이터가 수집되었습니다!")
        else:
            print("\n❌ 수집된 데이터가 없습니다.")
        
        return all_content

if __name__ == "__main__":
    collector = APIDataCollector()
    collector.collect_all_data()
