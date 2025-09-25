# 넷플릭스 콘텐츠 선호도 분석 프로젝트

## 프로젝트 개요
지역, 인종, 나이대별로 어떤 넷플릭스 콘텐츠가 선호되는지 분석하는 종합적인 데이터 분석 프로젝트입니다.

## 기술 스택
- **Python**: 데이터 수집, 전처리, 분석
- **SQLite**: 데이터 저장 및 관리
- **PowerBI**: 대시보드 및 시각화
- **Pandas, NumPy**: 데이터 처리
- **Matplotlib, Seaborn, Plotly**: 시각화
- **Scikit-learn**: 머신러닝 분석

## 프로젝트 구조
```
101_윤소연/
├── data/
│   ├── raw/           # 원본 데이터
│   └── processed/     # 전처리된 데이터
├── src/
│   ├── data_collection/   # 데이터 수집 모듈
│   ├── data_processing/   # 데이터 전처리 모듈
│   ├── analysis/          # 분석 모듈
│   └── visualization/     # 시각화 모듈
├── database/          # SQLite 데이터베이스
├── reports/           # 분석 결과 및 차트
├── notebooks/         # Jupyter 노트북
├── config/            # 설정 파일
├── main.py            # 메인 실행 파일
└── requirements.txt   # 패키지 의존성
```

## 설치 및 실행

### 1. 가상환경 설정
```bash
# 가상환경 생성 (이미 완료됨)
python -m venv venv

# 가상환경 활성화
# Windows
.\venv\Scripts\Activate.ps1
# macOS/Linux
source venv/bin/activate
```

### 2. 패키지 설치
```bash
pip install -r requirements.txt
```

### 3. 프로젝트 실행
```bash
python main.py
```

## 분석 내용

### 1. 데이터 수집
- 넷플릭스 콘텐츠 정보 (제목, 장르, 평점, 출시년도 등)
- 사용자 선호도 데이터 (지역, 나이대, 인종별)
- 시뮬레이션 데이터를 통한 대규모 데이터셋 생성

### 2. 데이터 전처리
- 데이터 클렌징 및 정제
- 특성 엔지니어링
- 분석용 특성 생성

### 3. 선호도 분석
- **지역별 분석**: 각 지역에서 선호하는 콘텐츠와 장르
- **나이대별 분석**: 연령대별 콘텐츠 선호도 패턴
- **인종별 분석**: 인종별 문화적 선호도 분석
- **장르별 분석**: 장르별 인기도 및 선호도

### 4. 시각화
- 인터랙티브 HTML 차트 생성
- 파워BI 대시보드 연동
- 다양한 시각화 도구 활용

## 파워BI 대시보드 구축

### 1. 데이터 연결
1. 파워BI Desktop 실행
2. `reports/powerbi/` 폴더의 CSV 파일들 연결
3. 테이블 간 관계 설정

### 2. 시각화 구성
- 지역별 선호도 차트
- 나이대별 선호도 차트
- 인종별 선호도 차트
- 장르별 인기도 차트
- 평점 분포 히스토그램

### 3. 필터 설정
- 지역별 필터
- 나이대별 필터
- 인종별 필터
- 장르별 필터
- 평점 범위 필터

## 주요 인사이트

프로젝트 실행 후 다음과 같은 인사이트를 얻을 수 있습니다:

1. **지역별 선호도**: 어떤 지역에서 어떤 콘텐츠가 인기 있는지
2. **나이대별 패턴**: 연령대별로 선호하는 장르와 콘텐츠 유형
3. **인종별 문화적 선호도**: 인종별로 다른 콘텐츠 선호도 패턴
4. **장르별 성공 요인**: 어떤 장르가 전반적으로 높은 평점을 받는지

## 파일 설명

- `main.py`: 전체 프로젝트 실행 파일
- `src/data_collection/netflix_data_collector.py`: 데이터 수집 모듈
- `src/data_processing/data_processor.py`: 데이터 전처리 모듈
- `src/analysis/preference_analyzer.py`: 선호도 분석 모듈
- `src/visualization/powerbi_connector.py`: 파워BI 연동 모듈

## 결과 파일

- `database/netflix_analysis.db`: SQLite 데이터베이스
- `reports/*.html`: 시각화 차트들
- `reports/powerbi/`: 파워BI용 데이터 및 설정 파일들

## 향후 개선 사항

1. **실제 API 연동**: 넷플릭스 공식 API 또는 웹스크래핑을 통한 실시간 데이터 수집
2. **머신러닝 모델**: 콘텐츠 추천 시스템 구축
3. **실시간 업데이트**: 정기적인 데이터 업데이트 자동화
4. **고급 시각화**: 3D 차트, 지도 시각화 등

## 문의사항

프로젝트 관련 문의사항이 있으시면 언제든지 연락주세요.







