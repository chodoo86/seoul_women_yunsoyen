"""
넷플릭스 콘텐츠 선호도 분석 프로젝트 메인 실행 파일
- 데이터 수집 → 전처리 → 분석 → 시각화 → 파워BI 연동
"""

import sys
import os
sys.path.append('src')

from data_collection.netflix_data_collector import NetflixDataCollector
from data_processing.data_processor import NetflixDataProcessor
from analysis.preference_analyzer import NetflixPreferenceAnalyzer
from visualization.powerbi_connector import PowerBIConnector

def main():
    print("=" * 60)
    print("넷플릭스 콘텐츠 선호도 분석 프로젝트")
    print("지역/인종/나이대별 선호도 분석")
    print("=" * 60)
    
    try:
        # 1. 데이터 수집
        print("\n1단계: 데이터 수집")
        print("-" * 30)
        collector = NetflixDataCollector()
        collector.collect_data()
        
        # 2. 데이터 전처리
        print("\n2단계: 데이터 전처리")
        print("-" * 30)
        processor = NetflixDataProcessor()
        processor.process_data()
        
        # 3. 선호도 분석
        print("\n3단계: 선호도 분석")
        print("-" * 30)
        analyzer = NetflixPreferenceAnalyzer()
        analysis_results = analyzer.run_analysis()
        
        # 4. 파워BI 연동
        print("\n4단계: 파워BI 대시보드 연동")
        print("-" * 30)
        connector = PowerBIConnector()
        connector.export_all()
        
        # 5. 결과 요약
        print("\n" + "=" * 60)
        print("프로젝트 완료!")
        print("=" * 60)
        print("\n📊 생성된 파일들:")
        print("• database/netflix_analysis.db - SQLite 데이터베이스")
        print("• reports/*.html - 시각화 차트들")
        print("• reports/powerbi/ - 파워BI용 데이터 및 설정")
        print("\n📈 주요 인사이트:")
        for insight in analysis_results['insights']:
            print(f"• {insight}")
        
        print("\n🚀 다음 단계:")
        print("1. reports/powerbi/ 폴더의 파일들을 파워BI Desktop에서 열기")
        print("2. powerbi_instructions.md 파일을 참고하여 대시보드 구축")
        print("3. HTML 차트들을 웹브라우저에서 확인")
        
    except Exception as e:
        print(f"\n❌ 오류 발생: {str(e)}")
        print("프로젝트 실행 중 문제가 발생했습니다.")

if __name__ == "__main__":
    main()







