# %% [markdown]
# # Fraud Analysis — TalkingData + Simulé
# - Lit .env et fraud_results.csv
# - Joint aux raw_clicks pour la composante temporelle
# - Produit 3 visualisations et sauvegarde des PNG dans `reports/`

# %%
import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
from pathlib import Path

# Chargement config
load_dotenv()
USER = os.getenv("PGUSER")
HOST = os.getenv("PGHOST", "localhost")
PORT = os.getenv("PGPORT", "5432")
DB   = os.getenv("PGDATABASE")

engine = create_engine(f"postgresql+psycopg2://{USER}@{HOST}:{PORT}/{DB}")

# Dossiers
Path("reports").mkdir(exist_ok=True)

# %%
# Charger les résultats de la requête de fraude
df_fraud = pd.read_csv("fraud_results.csv")

# Sanity checks
print("Fraud rows:", len(df_fraud))
print(df_fraud.head())
# %%
# Récupérer les clics bruts pour les IP suspectes afin d'analyser le temps
# (on prend les IP trouvées par la requête)
sus_ips = df_fraud["ip_address"].dropna().unique().tolist()

# Si dataset volumineux, on peut limiter:
# sus_ips = sus_ips[:5000]

query_raw = """
SELECT ad_id, ip_address, device_type, click_time
FROM raw_clicks
WHERE ip_address = ANY(%s)
"""

df_raw = pd.read_sql_query(query_raw, engine, params=(sus_ips,))
# Parser le temps
df_raw["click_time"] = pd.to_datetime(df_raw["click_time"], errors="coerce")
df_raw = df_raw.dropna(subset=["click_time"])
print("Suspicious raw clicks:", len(df_raw))
# %% 
# 1) Top 15 IPs suspectes par volume (depuis fraud_results.csv)
top_ips = (
    df_fraud.groupby("ip_address", as_index=False)["total_clicks_ip"]
    .sum()
    .sort_values("total_clicks_ip", ascending=False)
    .head(15)
)

plt.figure(figsize=(10,6))
plt.bar(top_ips["ip_address"].astype(str), top_ips["total_clicks_ip"])
plt.xticks(rotation=70, ha="right")
plt.title("Top 15 IPs suspectes (volume de clics)")
plt.xlabel("IP")
plt.ylabel("Clics suspects (agrégés)")
plt.tight_layout()
plt.savefig("reports/top_ips.png", dpi=150)
plt.show()
# %%
# 2) Top 10 ads par volume suspect (agrégation des total_clicks_ip)
top_ads = (
    df_fraud.groupby("ad_id", as_index=False)["total_clicks_ip"]
    .sum()
    .sort_values("total_clicks_ip", ascending=False)
    .head(10)
)

plt.figure(figsize=(8,5))
plt.bar(top_ads["ad_id"].astype(str), top_ads["total_clicks_ip"])
plt.title("Top 10 Ads par volume de clics suspects")
plt.xlabel("ad_id")
plt.ylabel("Clics suspects (agrégés)")
plt.tight_layout()
plt.savefig("reports/top_ads.png", dpi=150)
plt.show()
# %%
# 3) Évolution temporelle des clics suspects (agrégés par heure)
ts = (
    df_raw.set_index("click_time")
          .resample("H")["ip_address"]
          .count()
          .rename("sus_clicks_per_hour")
          .reset_index()
)

plt.figure(figsize=(10,4))
plt.plot(ts["click_time"], ts["sus_clicks_per_hour"])
plt.title("Évolution horaire des clics suspects")
plt.xlabel("Heure")
plt.ylabel("Clics suspects")
plt.tight_layout()
plt.savefig("reports/suspicious_clicks_timeseries.png", dpi=150)
plt.show()
# %%
# Export tableaux utiles pour le repo / annexe
top_ips.to_csv("reports/top_ips.csv", index=False)
top_ads.to_csv("reports/top_ads.csv", index=False)
ts.to_csv("reports/suspicious_clicks_timeseries.csv", index=False)

print("✅ Graphes enregistrés dans ./reports :")
print(" - top_ips.png")
print(" - top_ads.png")
print(" - suspicious_clicks_timeseries.png")
