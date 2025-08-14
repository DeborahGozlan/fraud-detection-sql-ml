from dotenv import load_dotenv
import os
import psycopg2

# Charger le fichier .env
load_dotenv()

# Lire les variables d'environnement
USER = os.getenv("PGUSER")
PASSWORD = os.getenv("PGPASSWORD")
HOST = os.getenv("PGHOST")
PORT = os.getenv("PGPORT")
DBNAME = os.getenv("PGDATABASE")

print(f"🔍 Tentative de connexion à {USER}@{HOST}:{PORT}/{DBNAME}")

try:
    conn = psycopg2.connect(
        dbname=DBNAME,
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT
    )
    print("✅ Connexion réussie !")
    conn.close()
except Exception as e:
    print("❌ Erreur de connexion :", e)
