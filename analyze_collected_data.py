"""
TMDB API로 수집된 데이터 분석
"""

import sqlite3
import pandas as pd
import numpy as np

def analyze_collected_data():
    """수집된 데이터 분석"""
    conn = sqlite3.connect('database/netflix_analysis.db')
    
    print("=" * 60)
    print("TMDB API를 활용한 넷플릭스 데이터 수집 결과 분석")
    print("=" * 60)
    
    # 1. 기본 정보
    df = pd.read_sql_query("SELECT * FROM netflix_content", conn)
    
    print(f"\n📊 데이터 수집 결과:")
    print(f"• 총 콘텐츠 수: {len(df):,}개")
    print(f"• 컬럼 수: {len(df.columns)}개")
    print(f"• 데이터 크기: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
    
    # 2. 콘텐츠 타입별 분포
    print(f"\n🎬 콘텐츠 타입별 분포:")
    type_counts = df['type'].value_counts()
    for content_type, count in type_counts.items():
        print(f"• {content_type}: {count}개 ({count/len(df)*100:.1f}%)")
    
    # 3. 연도별 분포
    print(f"\n📅 연도별 분포:")
    year_counts = df['release_year'].value_counts().sort_index()
    print(f"• 최신 연도: {df['release_year'].max()}")
    print(f"• 가장 오래된 연도: {df['release_year'].min()}")
    print(f"• 평균 연도: {df['release_year'].mean():.1f}")
    
    # 4. 평점 분석
    print(f"\n⭐ 평점 분석:")
    print(f"• 평균 TMDB 점수: {df['tmdb_score'].mean():.2f}/10")
    print(f"• 최고 점수: {df['tmdb_score'].max():.2f}")
    print(f"• 최저 점수: {df['tmdb_score'].min():.2f}")
    print(f"• 중간값: {df['tmdb_score'].median():.2f}")
    
    # 5. 인기도 분석
    print(f"\n🔥 인기도 분석:")
    print(f"• 평균 인기도: {df['popularity'].mean():.2f}")
    print(f"• 최고 인기도: {df['popularity'].max():.2f}")
    print(f"• 최저 인기도: {df['popularity'].min():.2f}")
    
    # 6. 언어별 분포
    print(f"\n🌍 언어별 분포 (상위 10개):")
    lang_counts = df['original_language'].value_counts().head(10)
    for lang, count in lang_counts.items():
        print(f"• {lang}: {count}개")
    
    # 7. 장르 분석
    print(f"\n🎭 장르 분석:")
    # 장르를 분리하여 분석
    all_genres = []
    for genres in df['genre'].dropna():
        if genres:
            all_genres.extend([g.strip() for g in genres.split(',')])
    
    genre_counts = pd.Series(all_genres).value_counts().head(10)
    print("상위 10개 장르:")
    for genre, count in genre_counts.items():
        print(f"• {genre}: {count}개")
    
    # 8. 런타임 분석 (영화만)
    movies = df[df['type'] == 'Movie']
    if len(movies) > 0:
        print(f"\n⏱️ 영화 런타임 분석:")
        print(f"• 평균 런타임: {movies['runtime'].mean():.0f}분")
        print(f"• 최장 런타임: {movies['runtime'].max()}분")
        print(f"• 최단 런타임: {movies['runtime'].min()}분")
    
    # 9. 예산 분석 (영화만)
    if len(movies) > 0:
        print(f"\n💰 영화 예산 분석:")
        budget_movies = movies[movies['budget'] > 0]
        if len(budget_movies) > 0:
            print(f"• 평균 예산: ${budget_movies['budget'].mean():,.0f}")
            print(f"• 최고 예산: ${budget_movies['budget'].max():,}")
            print(f"• 예산 정보가 있는 영화: {len(budget_movies)}개")
    
    # 10. 상위 콘텐츠
    print(f"\n🏆 상위 콘텐츠 (TMDB 점수 기준):")
    top_content = df.nlargest(5, 'tmdb_score')[['title', 'type', 'tmdb_score', 'release_year', 'genre']]
    for idx, row in top_content.iterrows():
        print(f"• {row['title']} ({row['type']}) - {row['tmdb_score']:.1f}점 ({row['release_year']})")
    
    # 11. 컬럼별 상세 정보
    print(f"\n📋 컬럼별 상세 정보:")
    print(f"• id: 고유 식별자")
    print(f"• title: 콘텐츠 제목")
    print(f"• type: 콘텐츠 타입 (Movie/TV Show)")
    print(f"• genre: 장르 (쉼표로 구분)")
    print(f"• release_year: 출시 연도")
    print(f"• tmdb_score: TMDB 평점 (0-10)")
    print(f"• popularity: TMDB 인기도 점수")
    print(f"• vote_count: 투표 수")
    print(f"• overview: 줄거리")
    print(f"• tmdb_id: TMDB 고유 ID")
    print(f"• original_language: 원어")
    print(f"• adult: 성인 콘텐츠 여부")
    print(f"• runtime: 런타임 (분)")
    print(f"• budget: 제작비 (달러)")
    print(f"• revenue: 수익 (달러)")
    print(f"• status: 제작 상태")
    print(f"• tagline: 태그라인")
    print(f"• production_companies: 제작사")
    print(f"• production_countries: 제작 국가")
    print(f"• spoken_languages: 사용 언어")
    print(f"• created_at: 데이터 수집 시간")
    
    # 12. 데이터 품질 확인
    print(f"\n🔍 데이터 품질 확인:")
    print(f"• 결측값이 있는 컬럼:")
    missing_data = df.isnull().sum()
    for col, missing_count in missing_data.items():
        if missing_count > 0:
            print(f"  - {col}: {missing_count}개 ({missing_count/len(df)*100:.1f}%)")
    
    if missing_data.sum() == 0:
        print("  - 결측값 없음 ✅")
    
    conn.close()
    
    print(f"\n✅ 데이터 수집 및 분석 완료!")
    print(f"총 {len(df)}개의 실제 넷플릭스 콘텐츠 데이터가 성공적으로 수집되었습니다.")

if __name__ == "__main__":
    analyze_collected_data()







