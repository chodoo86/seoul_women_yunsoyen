"""
넷플릭스 데이터 전처리 및 정제 모듈
- 데이터 클렌징
- 특성 엔지니어링
- 데이터 변환
"""

import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime
import re

class NetflixDataProcessor:
    def __init__(self, db_path="../../database/netflix_analysis.db"):
        self.db_path = db_path
        
    def load_data(self):
        """데이터베이스에서 데이터 로드"""
        conn = sqlite3.connect(self.db_path)
        
        # 콘텐츠 데이터 로드
        content_df = pd.read_sql_query("""
            SELECT * FROM netflix_content
        """, conn)
        
        # 사용자 선호도 데이터 로드
        preferences_df = pd.read_sql_query("""
            SELECT * FROM user_preferences
        """, conn)
        
        conn.close()
        
        return content_df, preferences_df
    
    def clean_content_data(self, df):
        """콘텐츠 데이터 클렌징"""
        print("콘텐츠 데이터 클렌징 중...")
        
        # 중복 제거
        df = df.drop_duplicates(subset=['title'])
        
        # 결측값 처리
        df['genre'] = df['genre'].fillna('Unknown')
        df['country'] = df['country'].fillna('Unknown')
        df['language'] = df['language'].fillna('Unknown')
        
        # 장르 분리 (여러 장르가 있는 경우)
        df['genres'] = df['genre'].str.split(', ')
        
        # 연도 범위 정리
        df = df[(df['release_year'] >= 1990) & (df['release_year'] <= 2024)]
        
        # 평점 정규화 (0-1 범위)
        df['imdb_score_norm'] = df['imdb_score'] / 10.0
        df['netflix_score_norm'] = df['netflix_score'] / 5.0
        
        print(f"클렌징 완료: {len(df)}개 콘텐츠")
        return df
    
    def clean_preferences_data(self, df):
        """사용자 선호도 데이터 클렌징"""
        print("사용자 선호도 데이터 클렌징 중...")
        
        # 이상치 제거 (시청 시간이 0보다 작거나 300분 이상인 경우)
        df = df[(df['watch_time'] > 0) & (df['watch_time'] <= 300)]
        
        # 완료율 정규화
        df = df[(df['completion_rate'] >= 0) & (df['completion_rate'] <= 1)]
        
        # 평점 정규화
        df = df[(df['rating'] >= 1.0) & (df['rating'] <= 5.0)]
        
        print(f"클렌징 완료: {len(df)}개 선호도 기록")
        return df
    
    def create_analytical_features(self, content_df, preferences_df):
        """분석용 특성 생성"""
        print("분석용 특성 생성 중...")
        
        # 콘텐츠별 통계
        content_stats = preferences_df.groupby('content_id').agg({
            'rating': ['mean', 'std', 'count'],
            'watch_time': ['mean', 'std'],
            'completion_rate': ['mean', 'std']
        }).round(2)
        
        content_stats.columns = ['avg_rating', 'rating_std', 'rating_count', 
                               'avg_watch_time', 'watch_time_std', 
                               'avg_completion_rate', 'completion_rate_std']
        
        content_stats = content_stats.reset_index()
        
        # 콘텐츠 데이터와 통계 병합
        content_enhanced = content_df.merge(content_stats, on='content_id', how='left')
        
        # 지역별 선호도 분석
        region_preferences = preferences_df.groupby(['content_id', 'region']).agg({
            'rating': 'mean',
            'completion_rate': 'mean',
            'user_id': 'count'
        }).round(2)
        
        region_preferences.columns = ['region_avg_rating', 'region_completion_rate', 'region_user_count']
        region_preferences = region_preferences.reset_index()
        
        # 나이대별 선호도 분석
        age_preferences = preferences_df.groupby(['content_id', 'age_group']).agg({
            'rating': 'mean',
            'completion_rate': 'mean',
            'user_id': 'count'
        }).round(2)
        
        age_preferences.columns = ['age_avg_rating', 'age_completion_rate', 'age_user_count']
        age_preferences = age_preferences.reset_index()
        
        # 인종별 선호도 분석
        ethnicity_preferences = preferences_df.groupby(['content_id', 'ethnicity']).agg({
            'rating': 'mean',
            'completion_rate': 'mean',
            'user_id': 'count'
        }).round(2)
        
        ethnicity_preferences.columns = ['ethnicity_avg_rating', 'ethnicity_completion_rate', 'ethnicity_user_count']
        ethnicity_preferences = ethnicity_preferences.reset_index()
        
        return content_enhanced, region_preferences, age_preferences, ethnicity_preferences
    
    def save_processed_data(self, content_df, preferences_df, region_prefs, age_prefs, ethnicity_prefs):
        """전처리된 데이터 저장"""
        conn = sqlite3.connect(self.db_path)
        
        # 전처리된 데이터를 새로운 테이블에 저장
        content_df.to_sql('processed_content', conn, if_exists='replace', index=False)
        preferences_df.to_sql('processed_preferences', conn, if_exists='replace', index=False)
        region_prefs.to_sql('region_preferences', conn, if_exists='replace', index=False)
        age_prefs.to_sql('age_preferences', conn, if_exists='replace', index=False)
        ethnicity_prefs.to_sql('ethnicity_preferences', conn, if_exists='replace', index=False)
        
        conn.close()
        print("전처리된 데이터가 저장되었습니다.")
    
    def process_data(self):
        """메인 데이터 처리 함수"""
        print("데이터 전처리를 시작합니다...")
        
        # 데이터 로드
        content_df, preferences_df = self.load_data()
        
        # 데이터 클렌징
        content_clean = self.clean_content_data(content_df)
        preferences_clean = self.clean_preferences_data(preferences_df)
        
        # 분석용 특성 생성
        content_enhanced, region_prefs, age_prefs, ethnicity_prefs = self.create_analytical_features(
            content_clean, preferences_clean
        )
        
        # 전처리된 데이터 저장
        self.save_processed_data(content_enhanced, preferences_clean, 
                               region_prefs, age_prefs, ethnicity_prefs)
        
        print("데이터 전처리가 완료되었습니다!")
        
        return content_enhanced, preferences_clean, region_prefs, age_prefs, ethnicity_prefs

if __name__ == "__main__":
    processor = NetflixDataProcessor()
    processor.process_data()

