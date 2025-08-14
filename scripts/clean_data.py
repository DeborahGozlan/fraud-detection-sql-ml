"""
clean_data.py
-------------
Reads from raw tables, cleans messy data, and stores the cleaned results
into a `clean` schema in PostgreSQL.
"""

import os
import re
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load .env
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD") or ""
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

# Create clean schema if it doesn't exist
with engine.connect() as conn:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS clean;"))
    conn.commit()

# --- Helper functions ---
def clean_email(email):
    """Standardize messy emails"""
    if not isinstance(email, str):
        return None
    email = email.strip().lower()
    email = re.sub(r"\s*\(at\)\s*|\s*\[at\]\s*", "@", email)
    email = re.sub(r"\s*@\s*", "@", email)
    email = re.sub(r"[^a-z0-9@._-]+", "", email)
    if not re.match(r"^[a-z0-9._%-]+@[a-z0-9.-]+\.[a-z]{2,}$", email):
        return None
    return email

def clean_device_type(device):
    """Fix typos and casing for device_type"""
    if not isinstance(device, str) or not device.strip():
        return None
    device_lower = device.strip().lower()
    if "mobile" in device_lower:
        return "Mobile"
    if "desk" in device_lower:
        return "Desktop"
    return device.strip().title()

def clean_ip(ip):
    """Return None if clearly invalid"""
    if not isinstance(ip, str):
        return None
    ip = ip.strip()
    if ip == "999.999.999.999":
        return None
    return ip

# --- 1. Clean raw_clicks ---
print("📦 Cleaning raw_clicks...")
df_clicks = pd.read_sql("SELECT * FROM raw_clicks", engine)

df_clicks["email"] = None  # raw_clicks doesn't have email, but keep placeholder
df_clicks["device_type"] = df_clicks["device_type"].apply(clean_device_type)
df_clicks["ip_address"] = df_clicks["ip_address"].apply(clean_ip)

# Convert click_time to datetime, drop invalid
df_clicks["click_time"] = pd.to_datetime(df_clicks["click_time"], errors="coerce")
df_clicks = df_clicks.dropna(subset=["click_time"])

# Save to clean schema
df_clicks.to_sql("clicks", engine, schema="clean", if_exists="replace", index=False)

# --- 2. Clean ad_connections ---
print("📦 Cleaning ad_connections...")
df_conn = pd.read_sql("SELECT * FROM ad_connections", engine)
df_conn["email"] = df_conn["email"].apply(clean_email)
df_conn["ip_address"] = df_conn["ip_address"].apply(clean_ip)

df_conn.to_sql("connections", engine, schema="clean", if_exists="replace", index=False)

# --- 3. Copy ads and ad_performance as-is (no cleaning needed here) ---
print("📦 Copying ads and ad_performance to clean schema...")
df_ads = pd.read_sql("SELECT * FROM ads", engine)
df_perf = pd.read_sql("SELECT * FROM ad_performance", engine)

df_ads.to_sql("ads", engine, schema="clean", if_exists="replace", index=False)
df_perf.to_sql("ad_performance", engine, schema="clean", if_exists="replace", index=False)

print("✅ Data cleaned and saved to 'clean' schema.")
