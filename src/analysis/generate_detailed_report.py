"""
Generate a detailed analysis report with both code and computed results.
Inputs: Gold views created by src/pipelines/etl.py
Output: reports/detailed_analysis_report.md
"""

import os
import sqlite3
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
DB_PATH = os.path.join(ROOT, 'database', 'netflix_analysis.db')
REPORT_PATH = os.path.join(ROOT, 'reports', 'detailed_analysis_report.md')


def to_table(df: pd.DataFrame, max_rows: int = 20) -> str:
    if df is None or df.empty:
        return "(no rows)"
    return df.head(max_rows).to_markdown(index=False)


def main():
    conn = sqlite3.connect(DB_PATH)

    sections = []

    # 1) KPI overview
    q1 = "SELECT * FROM vw_kpi_overview"
    df1 = pd.read_sql_query(q1, conn)
    sections.append("### KPI 개요\n\n" + "```sql\n" + q1 + "\n```\n\n" + to_table(df1))

    # 2) Yearly stats
    q2 = "SELECT * FROM vw_yearly_stats ORDER BY release_year"
    df2 = pd.read_sql_query(q2, conn)
    sections.append("### 연도별 통계\n\n" + "```sql\n" + q2 + "\n```\n\n" + to_table(df2, 30))

    # 3) Genre stats
    q3 = "SELECT * FROM vw_genre_stats ORDER BY n DESC"
    df3 = pd.read_sql_query(q3, conn)
    sections.append("### 장르 통계\n\n" + "```sql\n" + q3 + "\n```\n\n" + to_table(df3, 30))

    # 4) Language stats
    q4 = "SELECT * FROM vw_language_stats ORDER BY n DESC"
    df4 = pd.read_sql_query(q4, conn)
    sections.append("### 언어 통계\n\n" + "```sql\n" + q4 + "\n```\n\n" + to_table(df4, 20))

    # 5) Top titles
    q5 = "SELECT title, type, tmdb_score, popularity, vote_count, release_year FROM vw_top_titles LIMIT 30"
    df5 = pd.read_sql_query(q5, conn)
    sections.append("### 상위 타이틀\n\n" + "```sql\n" + q5 + "\n```\n\n" + to_table(df5, 30))

    # 6) Movie finance
    q6 = "SELECT title, budget, revenue, roi FROM vw_movie_finance LIMIT 30"
    df6 = pd.read_sql_query(q6, conn)
    sections.append("### 영화 재무 요약(ROI)\n\n" + "```sql\n" + q6 + "\n```\n\n" + to_table(df6, 30))

    conn.close()

    header = "### 상세 분석 리포트 (SQL 코드와 결과 포함)\n\n"
    intro = (
        "본 리포트는 Gold 뷰를 기반으로 주요 지표, 연도/장르/언어 통계, 상위 타이틀, 영화 재무 요약을 제공합니다.\n"
        "각 섹션에는 실행된 SQL과 결과 테이블이 포함됩니다.\n\n"
    )

    with open(REPORT_PATH, 'w', encoding='utf-8') as f:
        f.write(header)
        f.write(intro)
        f.write("\n\n".join(sections))

    print(f"Report written to {REPORT_PATH}")


if __name__ == '__main__':
    main()








