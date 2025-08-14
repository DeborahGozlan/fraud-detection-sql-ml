#!/usr/bin/env python3
import os, sys, subprocess, shutil
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
SQL = ROOT / "sql"

# --- Helpers ---------------------------------------------------------------
def run(cmd, cwd=None, env=None):
    print(f"\n$ {' '.join(cmd)}")
    res = subprocess.run(cmd, cwd=cwd, env=env)
    if res.returncode != 0:
        raise SystemExit(f"❌ Command failed with code {res.returncode}: {' '.join(cmd)}")
    return res

def require_file(path: Path, hint: str = ""):
    if not path.exists():
        msg = f"❌ Missing: {path}"
        if hint:
            msg += f"\n   Hint: {hint}"
        raise SystemExit(msg)

# --- Load env --------------------------------------------------------------
print("▶ Loading .env")
load_dotenv(dotenv_path=ROOT / ".env")

PGUSER = os.getenv("DB_USER") or os.getenv("PGUSER") or "postgres"
PGPASSWORD = os.getenv("DB_PASSWORD") or os.getenv("PGPASSWORD") or ""
PGHOST = os.getenv("DB_HOST") or os.getenv("PGHOST") or "localhost"
PGPORT = os.getenv("DB_PORT") or os.getenv("PGPORT") or "5432"
PGDATABASE = os.getenv("DB_NAME") or os.getenv("PGDATABASE") or "fraud_detection"

print(f"   PGUSER={PGUSER}  PGHOST={PGHOST}  PGPORT={PGPORT}  PGDATABASE={PGDATABASE}")

# --- Find psql -------------------------------------------------------------
psql = shutil.which("psql")
if not psql:
    # common Postgres.app path
    candidate = "/Applications/Postgres.app/Contents/Versions/17/bin/psql"
    if Path(candidate).exists():
        psql = candidate
    else:
        print("⚠️  Could not find `psql` in PATH.")
        print("   If you use Postgres.app, try adding this to your shell profile:")
        print('   export PATH="/Applications/Postgres.app/Contents/Versions/17/bin:$PATH"')
        raise SystemExit("❌ psql not found")

# --- Step 0: Quick DB connection check ------------------------------------
print("\n▶ Checking DB connection")
env = os.environ.copy()
if PGPASSWORD:
    env["PGPASSWORD"] = PGPASSWORD

run([psql, "-h", PGHOST, "-p", str(PGPORT), "-U", PGUSER, "-d", PGDATABASE, "-c", "SELECT 1;"], env=env)
print("✅ DB reachable")

# --- Step 1: load_data -----------------------------------------------------
load_py = SCRIPTS / "load_data.py"
require_file(load_py, "Create it or adjust the path.")
print("\n▶ Running scripts/load_data.py")
run([sys.executable, str(load_py)])

# --- Step 2: clean_data ----------------------------------------------------
clean_py = SCRIPTS / "clean_data.py"
require_file(clean_py, "We expect this to build the `clean` schema tables.")
print("\n▶ Running scripts/clean_data.py")
run([sys.executable, str(clean_py)])

# --- Step 3: BI views (clean-based) ---------------------------------------
bi_sql = SQL / "create_bi_views.sql"
require_file(bi_sql, "We replaced mv_* with clean.* earlier.")
print("\n▶ Applying sql/create_bi_views.sql")
run([psql, "-h", PGHOST, "-p", str(PGPORT), "-U", PGUSER, "-d", PGDATABASE, "-f", str(bi_sql)], env=env)

# --- Step 4 (optional): Fraud-feature views if file exists -----------------
bi_feat_sql = SQL / "create_bi_feature_views.sql"
if bi_feat_sql.exists():
    print("\n▶ Applying sql/create_bi_feature_views.sql (optional)")
    run([psql, "-h", PGHOST, "-p", str(PGPORT), "-U", PGUSER, "-d", PGDATABASE, "-f", str(bi_feat_sql)], env=env)
else:
    print("ℹ️  Skipping create_bi_feature_views.sql (not found). Run after your feature notebook creates bi.fraud_features.")

print("\n🎉 All steps completed successfully.")

# --- Step 5: Train baseline model ------------------------------------------
train_py = SCRIPTS / "train_baseline.py"
if train_py.exists():
    print("\n▶ Training baseline model")
    run([sys.executable, str(train_py)])
else:
    print("ℹ️  Skipping train_baseline.py (not found).")
