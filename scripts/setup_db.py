from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text

# ---------- Charger .env ----------
load_dotenv()

USER = os.getenv("PGUSER")
HOST = os.getenv("PGHOST", "localhost")
PORT = os.getenv("PGPORT", "5432")
DBNAME = os.getenv("PGDATABASE", "fraud_detection")

# ---------- Connexion à la base par défaut (postgres) ----------
default_engine = create_engine(f"postgresql+psycopg2://{USER}@{HOST}:{PORT}/postgres")

with default_engine.connect() as conn:
    conn.execute(text("COMMIT"))
    conn.execute(text(f"DROP DATABASE IF EXISTS {DBNAME}"))
    conn.execute(text(f"CREATE DATABASE {DBNAME}"))
    print(f"✅ Base '{DBNAME}' recréée avec succès")

# ---------- Connexion à la base fraud_detection ----------
engine = create_engine(f"postgresql+psycopg2://{USER}@{HOST}:{PORT}/{DBNAME}")

# ---------- Création des tables ----------
schema_sql = """
-- Table: ads
CREATE TABLE IF NOT EXISTS ads (
    ad_id VARCHAR(50) PRIMARY KEY,
    advertiser VARCHAR(255),
    campaign_name VARCHAR(255),
    category VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: raw_clicks
CREATE TABLE IF NOT EXISTS raw_clicks (
    click_id SERIAL PRIMARY KEY,
    ad_id VARCHAR(50) REFERENCES ads(ad_id),
    ip_address INET,
    device_type VARCHAR(50),
    click_time TIMESTAMP,
    referrer_url TEXT,
    user_agent TEXT
);

-- Table: ad_performance
CREATE TABLE IF NOT EXISTS ad_performance (
    perf_id SERIAL PRIMARY KEY,
    ad_id VARCHAR(50) REFERENCES ads(ad_id),
    date DATE,
    impressions INT,
    clicks INT,
    ctr NUMERIC(5,4),
    conversions INT,
    conversion_rate NUMERIC(5,4),
    bounce_rate NUMERIC(5,4),
    fraud BOOLEAN
);

-- Table: ad_connections
CREATE TABLE IF NOT EXISTS ad_connections (
    conn_id SERIAL PRIMARY KEY,
    ad_id VARCHAR(50) REFERENCES ads(ad_id),
    ip_address INET,
    connection_datetime TIMESTAMP,
    email TEXT
);
"""

with engine.connect() as conn:
    conn.execute(text(schema_sql))
    print("✅ Tables créées avec succès")
