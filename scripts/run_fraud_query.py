import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# ---------- Charger config ----------
load_dotenv()

USER = os.getenv("PGUSER")
HOST = os.getenv("PGHOST", "localhost")
PORT = os.getenv("PGPORT", "5432")
DBNAME = os.getenv("PGDATABASE")

# ---------- Connexion ----------
engine = create_engine(f"postgresql+psycopg2://{USER}@{HOST}:{PORT}/{DBNAME}")

# ---------- Lire la requête depuis le fichier ----------
QUERY_FILE = "queries/fraud_detection_query.sql"

with open(QUERY_FILE, "r") as f:
    query = f.read()

# ---------- Exécuter la requête ----------
print("📦 Exécution de la requête de détection de fraude...")
df_results = pd.read_sql_query(query, engine)

# ---------- Afficher un aperçu ----------
print("\n✅ Résultats obtenus :")
print(df_results.head(20))  # affiche les 20 premières lignes

# ---------- Sauvegarder en CSV ----------
OUTPUT_FILE = "fraud_results.csv"
df_results.to_csv(OUTPUT_FILE, index=False)
print(f"\n💾 Résultats sauvegardés dans {OUTPUT_FILE}")
