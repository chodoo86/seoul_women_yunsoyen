"""
Layered ETL pipeline (Python + SQL)
- Bronze/Staging: seed from existing netflix_content if present (or future API collection)
- Silver: normalized dims, facts, bridges
- Gold: reporting views
- Export: CSVs for Power BI
"""

import os
import sqlite3
import pandas as pd
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'database', 'netflix_analysis.db')
REPORTS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'reports')
PBI_DIR = os.path.join(REPORTS_DIR, 'powerbi')


class ETLPipeline:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        os.makedirs(PBI_DIR, exist_ok=True)

    def _connect(self):
        return sqlite3.connect(self.db_path)

    def stage_from_existing(self):
        """Create staging tables from existing netflix_content if available."""
        with self._connect() as conn:
            cur = conn.cursor()
            # Create staging tables
            cur.execute("""
            CREATE TABLE IF NOT EXISTS stg_tmdb_all (
                tmdb_id INTEGER,
                title TEXT,
                type TEXT,
                release_year INTEGER,
                tmdb_score REAL,
                popularity REAL,
                vote_count INTEGER,
                original_language TEXT,
                adult INTEGER,
                genre TEXT,
                runtime INTEGER,
                budget INTEGER,
                revenue INTEGER,
                production_countries TEXT,
                collected_at TEXT
            )
            """)

            # Seed from netflix_content if it exists
            try:
                df = pd.read_sql_query("SELECT * FROM netflix_content", conn)
            except Exception:
                df = pd.DataFrame()

            if not df.empty:
                df_stage = df[[
                    'tmdb_id','title','type','release_year','tmdb_score','popularity','vote_count',
                    'original_language','adult','genre','runtime','budget','revenue','production_countries'
                ]].copy()
                df_stage['collected_at'] = datetime.utcnow().isoformat()
                # Clear staging to keep latest snapshot
                cur.execute("DELETE FROM stg_tmdb_all")
                df_stage.to_sql('stg_tmdb_all', conn, if_exists='append', index=False)

    def build_silver(self):
        """Create dim/fact/bridge tables and populate from staging."""
        with self._connect() as conn:
            cur = conn.cursor()

            # Dims and facts
            cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_title (
                title_id INTEGER PRIMARY KEY AUTOINCREMENT,
                tmdb_id INTEGER UNIQUE NOT NULL,
                title TEXT NOT NULL,
                type TEXT,
                release_year INTEGER,
                original_language TEXT,
                adult INTEGER DEFAULT 0
            )
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_genre (
                genre TEXT PRIMARY KEY
            )
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS bridge_title_genre (
                title_id INTEGER,
                genre TEXT,
                PRIMARY KEY (title_id, genre)
            )
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS fact_title_metrics (
                title_id INTEGER PRIMARY KEY,
                tmdb_score REAL,
                popularity REAL,
                vote_count INTEGER,
                runtime INTEGER,
                budget INTEGER,
                revenue INTEGER,
                release_year INTEGER
            )
            """)

            # Upsert into dim_title
            cur.execute("DELETE FROM dim_title")
            cur.execute("""
            INSERT INTO dim_title (tmdb_id, title, type, release_year, original_language, adult)
            SELECT DISTINCT tmdb_id, title, type, release_year, original_language, COALESCE(adult,0)
            FROM stg_tmdb_all
            WHERE tmdb_id IS NOT NULL
            """)

            # Build fact_title_metrics
            cur.execute("DELETE FROM fact_title_metrics")
            cur.execute("""
            INSERT INTO fact_title_metrics (title_id, tmdb_score, popularity, vote_count, runtime, budget, revenue, release_year)
            SELECT d.title_id, s.tmdb_score, s.popularity, s.vote_count, COALESCE(s.runtime,0), COALESCE(s.budget,0), COALESCE(s.revenue,0), s.release_year
            FROM stg_tmdb_all s
            JOIN dim_title d ON d.tmdb_id = s.tmdb_id
            """)

            # Populate genres and bridge via Python split
            cur.execute("DELETE FROM dim_genre")
            cur.execute("DELETE FROM bridge_title_genre")
            df = pd.read_sql_query("""
                SELECT d.title_id, s.genre
                FROM stg_tmdb_all s JOIN dim_title d ON d.tmdb_id=s.tmdb_id
            """, conn)
            if not df.empty:
                rows_bridge = []
                genres_unique = set()
                for _, row in df.iterrows():
                    g = (row['genre'] or '').strip()
                    if not g:
                        continue
                    parts = [p.strip() for p in g.split(',') if p and p.strip()]
                    for part in parts:
                        genres_unique.add(part)
                        rows_bridge.append({'title_id': int(row['title_id']), 'genre': part})
                if genres_unique:
                    pd.DataFrame({'genre': sorted(genres_unique)}).to_sql('dim_genre', conn, if_exists='append', index=False)
                if rows_bridge:
                    pd.DataFrame(rows_bridge).to_sql('bridge_title_genre', conn, if_exists='append', index=False)

    def build_gold_views(self):
        with self._connect() as conn:
            cur = conn.cursor()
            # Overview KPIs
            cur.execute("""
            CREATE VIEW IF NOT EXISTS vw_kpi_overview AS
            SELECT 
              COUNT(*) AS title_count,
              ROUND(AVG(f.tmdb_score),2) AS avg_tmdb_score,
              ROUND(AVG(f.popularity),2) AS avg_popularity
            FROM fact_title_metrics f
            """)

            # Yearly stats
            cur.execute("""
            CREATE VIEW IF NOT EXISTS vw_yearly_stats AS
            SELECT f.release_year, COUNT(*) AS n,
                   ROUND(AVG(f.tmdb_score),2) AS avg_score,
                   ROUND(AVG(f.popularity),2) AS avg_pop
            FROM fact_title_metrics f
            WHERE f.release_year IS NOT NULL
            GROUP BY f.release_year
            """)

            # Genre stats
            cur.execute("""
            CREATE VIEW IF NOT EXISTS vw_genre_stats AS
            SELECT g.genre, COUNT(*) AS n,
                   ROUND(AVG(f.tmdb_score),2) AS avg_score,
                   ROUND(AVG(f.popularity),2) AS avg_pop
            FROM bridge_title_genre bg
            JOIN dim_title t ON t.title_id=bg.title_id
            JOIN fact_title_metrics f ON f.title_id=t.title_id
            JOIN dim_genre g ON g.genre=bg.genre
            GROUP BY g.genre
            """)

            # Language stats
            cur.execute("""
            CREATE VIEW IF NOT EXISTS vw_language_stats AS
            SELECT t.original_language, COUNT(*) AS n,
                   ROUND(AVG(f.tmdb_score),2) AS avg_score
            FROM dim_title t JOIN fact_title_metrics f ON f.title_id=t.title_id
            GROUP BY t.original_language
            """)

            # Top titles by score/popularity
            cur.execute("""
            CREATE VIEW IF NOT EXISTS vw_top_titles AS
            SELECT t.title, t.type, f.tmdb_score, f.popularity, f.vote_count, f.release_year
            FROM dim_title t JOIN fact_title_metrics f ON f.title_id=t.title_id
            ORDER BY f.tmdb_score DESC, f.vote_count DESC
            """)

            # Movie finance summary (where non-zero)
            cur.execute("""
            CREATE VIEW IF NOT EXISTS vw_movie_finance AS
            SELECT t.title, f.budget, f.revenue,
                   CASE WHEN f.budget>0 THEN ROUND(1.0*f.revenue/f.budget,2) END AS roi
            FROM dim_title t JOIN fact_title_metrics f ON f.title_id=t.title_id
            WHERE t.type='Movie' AND f.budget>0 AND f.revenue>0
            ORDER BY roi DESC
            """)

    def export_gold_to_csv(self):
        with self._connect() as conn:
            views = ['vw_kpi_overview','vw_yearly_stats','vw_genre_stats','vw_language_stats','vw_top_titles','vw_movie_finance']
            for v in views:
                try:
                    df = pd.read_sql_query(f"SELECT * FROM {v}", conn)
                    out = os.path.join(PBI_DIR, f"{v}.csv")
                    df.to_csv(out, index=False)
                except Exception:
                    pass

    def run(self):
        self.stage_from_existing()
        self.build_silver()
        self.build_gold_views()
        self.export_gold_to_csv()


if __name__ == '__main__':
    ETLPipeline().run()








