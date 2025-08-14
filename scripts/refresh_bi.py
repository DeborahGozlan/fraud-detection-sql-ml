"""
scripts/refresh_bi.py
Refresh all BI materialized views in `clean` schema.
"""

import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load config
load_dotenv()
DB_USER = os.getenv("DB_USER") or os.getenv("PGUSER")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_HOST = os.getenv("DB_HOST") or os.getenv("PGHOST", "localhost")
DB_PORT = os.getenv("DB_PORT") or os.getenv("PGPORT", "5432")
DB_NAME = os.getenv("DB_NAME") or os.getenv("PGDATABASE")

if DB_PASSWORD:
    DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
else:
    DATABASE_URL = f"postgresql+psycopg2://{DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)

# Refresh order (dependencies if any)
VIEWS = [
    "clean.ads",
    "clean.raw_clicks",
    "clean.ad_performance",
    "clean.ad_connections"
]

def main():
    with engine.begin() as conn:
        for view in VIEWS:
            print(f"🔄 Refreshing {view}...")
            conn.execute(text(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view};"))
    print("✅ All materialized views refreshed.")

if __name__ == "__main__":
    main()
