"""
scripts/create_clean_schema.py
Create `clean` schema + BI-ready materialized views.
"""

import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# 1) Load config
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

# 2) SQL: schema + materialized views
SQL = r"""
-- 1) Schema
CREATE SCHEMA IF NOT EXISTS clean;

-- 2) Drop MVs if they exist
DROP MATERIALIZED VIEW IF EXISTS clean.ads;
DROP MATERIALIZED VIEW IF EXISTS clean.raw_clicks;
DROP MATERIALIZED VIEW IF EXISTS clean.ad_performance;
DROP MATERIALIZED VIEW IF EXISTS clean.ad_connections;

-- 3) Materialized Views
CREATE MATERIALIZED VIEW clean.ads AS
SELECT DISTINCT
    ad_id,
    INITCAP(TRIM(advertiser))      AS advertiser,
    INITCAP(TRIM(campaign_name))   AS campaign_name,
    INITCAP(TRIM(category))        AS category,
    created_at
FROM ads
WHERE ad_id IS NOT NULL;

CREATE MATERIALIZED VIEW clean.raw_clicks AS
SELECT
    ad_id,
    NULLIF(TRIM(LOWER(device_type)), '') AS device_type,
    ip_address,
    click_time,
    referrer_url,
    user_agent
FROM raw_clicks
WHERE click_time IS NOT NULL
  AND ip_address::text ~ '^(?:\d{1,3}\.){3}\d{1,3}$';

CREATE MATERIALIZED VIEW clean.ad_performance AS
SELECT
    ad_id,
    date,
    impressions,
    clicks,
    ctr,
    conversions,
    conversion_rate,
    bounce_rate,
    fraud
FROM ad_performance
WHERE impressions >= 0 AND clicks >= 0;

CREATE MATERIALIZED VIEW clean.ad_connections AS
SELECT
    ad_id,
    ip_address,
    connection_datetime,
    CASE
        WHEN email ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
        THEN LOWER(TRIM(email))
        ELSE NULL
    END AS email
FROM ad_connections;
"""

def main():
    with engine.begin() as conn:
        conn.execute(text(SQL))
    print("✅ clean schema & materialized views created.")

if __name__ == "__main__":
    main()
