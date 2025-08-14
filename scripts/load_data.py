# 📦 1. Import des modules nécessaires et chargement des variables d'environnement
# - os : gestion des chemins et variables d'environnement
# - pandas / numpy : manipulation de données
# - faker : génération de données fictives
# - datetime : gestion des dates
# - sqlalchemy : connexion à PostgreSQL
# - dotenv : lecture du fichier .env
# - random : tirages aléatoires simples

import os
import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from dotenv import load_dotenv
import random

# Charger la configuration depuis le fichier .env
load_dotenv()

# Variables de connexion
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD", "")
HOST = os.getenv("DB_HOST", "localhost")
PORT = os.getenv("DB_PORT", "5432")
DBNAME = os.getenv("DB_NAME")
KAGGLE_FILE = os.getenv(
    "KAGGLE_FILE", 
    "/Users/deborahgozlan/Documents/fraud_project/train_sample.csv"
)

# Instancier Faker (pour créer des données factices réalistes)
fake = Faker()

# 🔌 2. Connexion à la base PostgreSQL via SQLAlchemy
# Le format de l'URL est : postgresql+psycopg2://utilisateur@hôte:port/base
engine = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}")

# 🛠 3. Création d'une table "ads" factice
# Chaque pub (ad) a : un id, un annonceur, un nom de campagne, une catégorie, et une date de création
print("📦 Insertion dans ads...")

ads_data = []
for i in range(1, 11):  # 10 annonces fictives
    ads_data.append({
        "ad_id": f"AD{i:03d}",
        "advertiser": fake.company(),
        "campaign_name": f"Campaign {i}",
        "category": random.choice(["Retail", "Tech", "Finance", "Travel"]),
        "created_at": fake.date_time_between(start_date="-1y", end_date="now")
    })

df_ads = pd.DataFrame(ads_data)

# Envoi dans PostgreSQL
df_ads.to_sql("ads", engine, if_exists="append", index=False)

# 📂 4. Chargement du jeu TalkingData et conversion de l'IP entière en IPv4 (INET-compatible)
print("📦 Chargement TalkingData train_sample.csv dans raw_clicks...")

df_clicks = pd.read_csv(KAGGLE_FILE)

# Fonction : convertir l'entier en IPv4 dotted-quad
def int_to_ipv4(n: int) -> str:
    n = int(n) % (2**32)
    return ".".join(str((n >> (8*i)) & 255) for i in [3,2,1,0])

df_clicks["ip_address"] = df_clicks["ip"].apply(int_to_ipv4)

# Attribution d'un ad_id existant
df_clicks["ad_id"] = np.random.choice(df_ads["ad_id"], size=len(df_clicks))

# Autres colonnes
df_clicks["device_type"] = df_clicks["device"].astype(str)
df_clicks["referrer_url"] = [fake.url() for _ in range(len(df_clicks))]
df_clicks["user_agent"]   = [fake.user_agent() for _ in range(len(df_clicks))]

# Sous-ensemble final
df_clicks_final = df_clicks[["ad_id", "ip_address", "device_type", "click_time", "referrer_url", "user_agent"]]

# 🎭 5. Simuler des données réelles imparfaites (valeurs manquantes, typos, doublons...)
# A) Doublons logiques (5%)
df_clicks_final = pd.concat(
    [df_clicks_final, df_clicks_final.sample(frac=0.05, random_state=42)],
    ignore_index=True
)

# B) device_type : typos / casse / NULL
typos_mobile  = ["Mobile", "MOBILE", "moblie", "moible", " mobile "]
typos_desktop = ["Desktop", "DESKTOP", "deskotp", " deskTop ", "desk top"]

idx_mobile_like = df_clicks_final["device_type"].str.lower() == "1"
idx_other       = ~idx_mobile_like

df_clicks_final.loc[idx_mobile_like.sample(frac=0.10, random_state=101).index, "device_type"] = np.random.choice(typos_mobile)
if idx_other.any():
    sel = df_clicks_final.loc[idx_other].sample(frac=0.10, random_state=102).index
    df_clicks_final.loc[sel, "device_type"] = np.random.choice(typos_desktop)

df_clicks_final.loc[df_clicks_final.sample(frac=0.02, random_state=103).index, "device_type"] = None

# C) referrer_url : ajout de paramètres UTM + majuscules partielles
def add_utm(u: str) -> str:
    if not isinstance(u, str) or not u:
        return u
    sep = "&" if "?" in u else "?"
    return f"{u}{sep}utm_source=NEWSLETTER&utm_medium=Email&utm_campaign=Q3"

df_clicks_final.loc[df_clicks_final.sample(frac=0.08, random_state=104).index, "referrer_url"] = \
    df_clicks_final["referrer_url"].apply(add_utm)

df_clicks_final.loc[df_clicks_final.sample(frac=0.05, random_state=105).index, "referrer_url"] = \
    df_clicks_final["referrer_url"].str.replace("www.", "WWW.", regex=False)

# D) user_agent bruité
df_clicks_final.loc[df_clicks_final.sample(frac=0.01, random_state=106).index, "user_agent"] = "🤖bot/1.0"
df_clicks_final.loc[df_clicks_final.sample(frac=0.005, random_state=107).index, "user_agent"] = ""

# E) click_time “bizarre” (futur et passé lointain)
df_clicks_final.loc[df_clicks_final.sample(frac=0.01, random_state=108).index, "click_time"] = \
    pd.Timestamp.now() + pd.Timedelta(days=7)
df_clicks_final.loc[df_clicks_final.sample(frac=0.005, random_state=109).index, "click_time"] = \
    pd.Timestamp.now() - pd.Timedelta(days=3650)

# F) ip_address “edge” mais valide
df_clicks_final.loc[df_clicks_final.sample(frac=0.01, random_state=110).index, "ip_address"] = "255.255.255.255"

# Envoi dans PostgreSQL
df_clicks_final.to_sql("raw_clicks", engine, if_exists="append", index=False)

# 📊 6. Génération de la table ad_performance (statistiques journalières sur 30 jours)
print("📦 Génération ad_performance...")

performance_data = []
for ad_id in df_ads["ad_id"]:
    for days_ago in range(30):
        impressions = random.randint(100, 5000)
        clicks = random.randint(0, impressions)
        ctr = clicks / impressions if impressions else 0
        conversions = random.randint(0, clicks)
        conv_rate = conversions / clicks if clicks else 0
        bounce_rate = random.uniform(0, 1)
        fraud_flag = random.choice([True, False, False, False])  # ~25% fraude
        performance_data.append({
            "ad_id": ad_id,
            "date": datetime.now().date() - timedelta(days=days_ago),
            "impressions": impressions,
            "clicks": clicks,
            "ctr": round(ctr, 4),
            "conversions": conversions,
            "conversion_rate": round(conv_rate, 4),
            "bounce_rate": round(bounce_rate, 4),
            "fraud": fraud_flag
        })

df_perf = pd.DataFrame(performance_data)
df_perf.to_sql("ad_performance", engine, if_exists="append", index=False)

# 📧 7. Génération de la table ad_connections avec emails volontairement "sales"
print("📦 Génération ad_connections (avec emails bruités)...")

conn_data = []
for _ in range(500):
    base_email = fake.email()
    conn_data.append({
        "ad_id": np.random.choice(df_ads["ad_id"]),
        "ip_address": fake.ipv4(),
        "connection_datetime": fake.date_time_between(start_date="-30d", end_date="now"),
        "email": base_email
    })

df_conn = pd.DataFrame(conn_data)

# Ajouter du désordre dans les emails
df_conn.loc[df_conn.sample(frac=0.05, random_state=201).index, "email"] = \
    df_conn["email"].str.replace("@", " (at) ", regex=False)
df_conn.loc[df_conn.sample(frac=0.03, random_state=202).index, "email"] = \
    df_conn["email"].str.replace("@", " [at] ", regex=False)
df_conn.loc[df_conn.sample(frac=0.02, random_state=203).index, "email"] = \
    " " + df_conn["email"] + " "
df_conn.loc[df_conn.sample(frac=0.01, random_state=204).index, "email"] = \
    df_conn["email"].str.replace(r"[^A-Za-z0-9@._-]", "_", regex=True)

# Envoi dans PostgreSQL
df_conn.to_sql("ad_connections", engine, if_exists="append", index=False)

print("✅ Données chargées avec succès !")
