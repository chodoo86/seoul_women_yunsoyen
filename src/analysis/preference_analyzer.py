"""
넷플릭스 콘텐츠 선호도 분석 모듈
- 지역별 선호도 분석
- 나이대별 선호도 분석
- 인종별 선호도 분석
- 장르별 선호도 분석
"""

import pandas as pd
import numpy as np
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

class NetflixPreferenceAnalyzer:
    def __init__(self, db_path="../../database/netflix_analysis.db"):
        self.db_path = db_path
        
    def load_processed_data(self):
        """전처리된 데이터 로드"""
        conn = sqlite3.connect(self.db_path)
        
        content_df = pd.read_sql_query("SELECT * FROM processed_content", conn)
        preferences_df = pd.read_sql_query("SELECT * FROM processed_preferences", conn)
        region_prefs = pd.read_sql_query("SELECT * FROM region_preferences", conn)
        age_prefs = pd.read_sql_query("SELECT * FROM age_preferences", conn)
        ethnicity_prefs = pd.read_sql_query("SELECT * FROM ethnicity_preferences", conn)
        
        conn.close()
        
        return content_df, preferences_df, region_prefs, age_prefs, ethnicity_prefs
    
    def analyze_region_preferences(self, content_df, region_prefs):
        """지역별 선호도 분석"""
        print("지역별 선호도 분석 중...")
        
        # 지역별 평균 평점과 완료율
        region_analysis = region_prefs.groupby('region').agg({
            'region_avg_rating': 'mean',
            'region_completion_rate': 'mean',
            'region_user_count': 'sum'
        }).round(2)
        
        # 상위 콘텐츠 (지역별)
        top_content_by_region = region_prefs.nlargest(10, 'region_avg_rating')
        
        # 지역별 장르 선호도
        region_genre_prefs = region_prefs.merge(
            content_df[['content_id', 'genre']], on='content_id'
        )
        
        region_genre_analysis = region_genre_prefs.groupby(['region', 'genre']).agg({
            'region_avg_rating': 'mean',
            'region_completion_rate': 'mean'
        }).round(2)
        
        return region_analysis, top_content_by_region, region_genre_analysis
    
    def analyze_age_preferences(self, content_df, age_prefs):
        """나이대별 선호도 분석"""
        print("나이대별 선호도 분석 중...")
        
        # 나이대별 평균 평점과 완료율
        age_analysis = age_prefs.groupby('age_group').agg({
            'age_avg_rating': 'mean',
            'age_completion_rate': 'mean',
            'age_user_count': 'sum'
        }).round(2)
        
        # 상위 콘텐츠 (나이대별)
        top_content_by_age = age_prefs.nlargest(10, 'age_avg_rating')
        
        # 나이대별 장르 선호도
        age_genre_prefs = age_prefs.merge(
            content_df[['content_id', 'genre']], on='content_id'
        )
        
        age_genre_analysis = age_genre_prefs.groupby(['age_group', 'genre']).agg({
            'age_avg_rating': 'mean',
            'age_completion_rate': 'mean'
        }).round(2)
        
        return age_analysis, top_content_by_age, age_genre_analysis
    
    def analyze_ethnicity_preferences(self, content_df, ethnicity_prefs):
        """인종별 선호도 분석"""
        print("인종별 선호도 분석 중...")
        
        # 인종별 평균 평점과 완료율
        ethnicity_analysis = ethnicity_prefs.groupby('ethnicity').agg({
            'ethnicity_avg_rating': 'mean',
            'ethnicity_completion_rate': 'mean',
            'ethnicity_user_count': 'sum'
        }).round(2)
        
        # 상위 콘텐츠 (인종별)
        top_content_by_ethnicity = ethnicity_prefs.nlargest(10, 'ethnicity_avg_rating')
        
        # 인종별 장르 선호도
        ethnicity_genre_prefs = ethnicity_prefs.merge(
            content_df[['content_id', 'genre']], on='content_id'
        )
        
        ethnicity_genre_analysis = ethnicity_genre_prefs.groupby(['ethnicity', 'genre']).agg({
            'ethnicity_avg_rating': 'mean',
            'ethnicity_completion_rate': 'mean'
        }).round(2)
        
        return ethnicity_analysis, top_content_by_ethnicity, ethnicity_genre_analysis
    
    def analyze_genre_preferences(self, content_df, preferences_df):
        """장르별 선호도 분석"""
        print("장르별 선호도 분석 중...")
        
        # 장르별 통계
        genre_stats = content_df.groupby('genre').agg({
            'avg_rating': 'mean',
            'avg_completion_rate': 'mean',
            'rating_count': 'sum',
            'imdb_score': 'mean',
            'netflix_score': 'mean'
        }).round(2)
        
        # 장르별 지역 선호도
        genre_region_prefs = preferences_df.merge(
            content_df[['content_id', 'genre']], on='content_id'
        )
        
        genre_region_analysis = genre_region_prefs.groupby(['genre', 'region']).agg({
            'rating': 'mean',
            'completion_rate': 'mean'
        }).round(2)
        
        return genre_stats, genre_region_analysis
    
    def create_visualizations(self, region_analysis, age_analysis, ethnicity_analysis, genre_stats):
        """시각화 생성"""
        print("시각화 생성 중...")
        
        # 1. 지역별 선호도 히트맵
        fig1 = px.bar(
            region_analysis.reset_index(), 
            x='region', 
            y='region_avg_rating',
            title='지역별 평균 평점',
            color='region_avg_rating',
            color_continuous_scale='Viridis'
        )
        fig1.write_html("../../reports/region_preferences.html")
        
        # 2. 나이대별 선호도
        fig2 = px.bar(
            age_analysis.reset_index(),
            x='age_group',
            y='age_avg_rating',
            title='나이대별 평균 평점',
            color='age_avg_rating',
            color_continuous_scale='Plasma'
        )
        fig2.write_html("../../reports/age_preferences.html")
        
        # 3. 인종별 선호도
        fig3 = px.bar(
            ethnicity_analysis.reset_index(),
            x='ethnicity',
            y='ethnicity_avg_rating',
            title='인종별 평균 평점',
            color='ethnicity_avg_rating',
            color_continuous_scale='Inferno'
        )
        fig3.write_html("../../reports/ethnicity_preferences.html")
        
        # 4. 장르별 선호도
        fig4 = px.bar(
            genre_stats.reset_index(),
            x='genre',
            y='avg_rating',
            title='장르별 평균 평점',
            color='avg_rating',
            color_continuous_scale='Cividis'
        )
        fig4.write_html("../../reports/genre_preferences.html")
        
        print("시각화가 reports 폴더에 저장되었습니다.")
    
    def generate_insights(self, region_analysis, age_analysis, ethnicity_analysis, genre_stats):
        """인사이트 생성"""
        print("인사이트 생성 중...")
        
        insights = []
        
        # 지역별 인사이트
        top_region = region_analysis['region_avg_rating'].idxmax()
        top_region_score = region_analysis.loc[top_region, 'region_avg_rating']
        insights.append(f"가장 높은 평점을 받는 지역: {top_region} ({top_region_score:.2f})")
        
        # 나이대별 인사이트
        top_age = age_analysis['age_avg_rating'].idxmax()
        top_age_score = age_analysis.loc[top_age, 'age_avg_rating']
        insights.append(f"가장 높은 평점을 받는 나이대: {top_age} ({top_age_score:.2f})")
        
        # 인종별 인사이트
        top_ethnicity = ethnicity_analysis['ethnicity_avg_rating'].idxmax()
        top_ethnicity_score = ethnicity_analysis.loc[top_ethnicity, 'ethnicity_avg_rating']
        insights.append(f"가장 높은 평점을 받는 인종: {top_ethnicity} ({top_ethnicity_score:.2f})")
        
        # 장르별 인사이트
        top_genre = genre_stats['avg_rating'].idxmax()
        top_genre_score = genre_stats.loc[top_genre, 'avg_rating']
        insights.append(f"가장 높은 평점을 받는 장르: {top_genre} ({top_genre_score:.2f})")
        
        return insights
    
    def run_analysis(self):
        """메인 분석 함수"""
        print("넷플릭스 선호도 분석을 시작합니다...")
        
        # 데이터 로드
        content_df, preferences_df, region_prefs, age_prefs, ethnicity_prefs = self.load_processed_data()
        
        # 각 분석 수행
        region_analysis, top_content_by_region, region_genre_analysis = self.analyze_region_preferences(content_df, region_prefs)
        age_analysis, top_content_by_age, age_genre_analysis = self.analyze_age_preferences(content_df, age_prefs)
        ethnicity_analysis, top_content_by_ethnicity, ethnicity_genre_analysis = self.analyze_ethnicity_preferences(content_df, ethnicity_prefs)
        genre_stats, genre_region_analysis = self.analyze_genre_preferences(content_df, preferences_df)
        
        # 시각화 생성
        self.create_visualizations(region_analysis, age_analysis, ethnicity_analysis, genre_stats)
        
        # 인사이트 생성
        insights = self.generate_insights(region_analysis, age_analysis, ethnicity_analysis, genre_stats)
        
        # 결과 출력
        print("\n=== 분석 결과 ===")
        for insight in insights:
            print(f"• {insight}")
        
        print("\n분석이 완료되었습니다!")
        
        return {
            'region_analysis': region_analysis,
            'age_analysis': age_analysis,
            'ethnicity_analysis': ethnicity_analysis,
            'genre_stats': genre_stats,
            'insights': insights
        }

if __name__ == "__main__":
    analyzer = NetflixPreferenceAnalyzer()
    analyzer.run_analysis()

