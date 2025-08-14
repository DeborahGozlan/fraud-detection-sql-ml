#!/usr/bin/env python3
"""
Train a fast baseline model (RandomForest) for fraud detection.

- Loads features from Postgres (clean schema)
- Joins nearest ad_performance row per (ad_id, day)
- Adds category_norm from clean.ads
- Trains RF with OneHotEncoder
- Evaluates (ROC-AUC, PR-AUC, classification_report)
- Saves model + metrics to disk
- Saves predictions to DB (bi.model_predictions)

Run:
  source venv/bin/activate
  python scripts/train_baseline.py
"""

import os, json
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    roc_auc_score, average_precision_score, classification_report,
    confusion_matrix, precision_recall_curve
)
import joblib

# ------------------ Config & DB ------------------
ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

DB_USER = os.getenv("DB_USER") or os.getenv("PGUSER") or "postgres"
DB_PASSWORD = os.getenv("DB_PASSWORD") or os.getenv("PGPASSWORD") or ""
DB_HOST = os.getenv("DB_HOST") or os.getenv("PGHOST") or "localhost"
DB_PORT = os.getenv("DB_PORT") or os.getenv("PGPORT") or "5432"
DB_NAME = os.getenv("DB_NAME") or os.getenv("PGDATABASE") or "fraud_detection"

if DB_PASSWORD:
    DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
else:
    DATABASE_URL = f"postgresql+psycopg2://{DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)

# ------------------ Make sure output dirs exist ------------------
Path(ROOT / "models").mkdir(exist_ok=True)
Path(ROOT / "reports").mkdir(exist_ok=True)

# ------------------ Load features from DB ------------------
# Use day grain for clicks, then nearest performance row per ad_id/day
QUERY = """
WITH clicks_day AS (
  SELECT
    ad_id,
    (click_time::date) AS as_of_date,
    COUNT(*) AS clicks_day
  FROM clean.clicks
  GROUP BY ad_id, as_of_date
),
joined AS (
  SELECT
    cd.*,
    ap.impressions, ap.clicks AS perf_clicks,
    ap.conversions, ap.ctr, ap.conversion_rate, ap.bounce_rate,
    ap.fraud,
    ROW_NUMBER() OVER (
      PARTITION BY cd.ad_id, cd.as_of_date
      ORDER BY ABS(ap.date - cd.as_of_date)
    ) AS rn
  FROM clicks_day cd
  LEFT JOIN clean.ad_performance ap
    ON ap.ad_id = cd.ad_id
)
SELECT * FROM joined WHERE rn = 1
LIMIT 100000; -- keep it laptop-friendly
"""

print("▶ Loading data from Postgres ...")
df = pd.read_sql(text(QUERY), engine)
print(f"   Loaded {len(df):,} rows")

# Add category_norm from ads
ads = pd.read_sql("SELECT ad_id, category FROM clean.ads;", engine)
ads["category_norm"] = (
    ads["category"].fillna("unknown").astype(str).str.strip()
       .str.lower().str.replace(r"\s+", " ", regex=True)
)
df = df.merge(ads[["ad_id", "category_norm"]], on="ad_id", how="left")

# ------------------ Label & features ------------------
# Primary label from ad_performance.fraud (fallback if missing)
if "fraud" in df.columns:
    df["label"] = df["fraud"].fillna(0).astype(int)
else:
    df["label"] = ((df.get("clicks_day", 0) >= 50) & (df.get("conversions", 0) == 0)).astype(int)

candidate_num = [
    "clicks_day", "impressions", "perf_clicks", "conversions",
    "ctr", "conversion_rate", "bounce_rate"
]
num_cols = [c for c in candidate_num if c in df.columns]

df["category_norm"] = df["category_norm"].fillna("unknown").astype(str)
cat_cols = ["category_norm"]

# Fill NA for numeric
for c in num_cols:
    df[c] = df[c].astype(float).fillna(0.0)

# Drop rows without label
df = df.dropna(subset=["label"])
df["label"] = df["label"].astype(int)

if df["label"].nunique() < 2:
    # Demo fallback: ensure both classes exist
    df["_score_demo"] = df["clicks_day"].fillna(0) - 10 * df.get("conversions", 0).fillna(0)
    cutoff = df["_score_demo"].quantile(0.98)
    df.loc[df["_score_demo"] >= cutoff, "label"] = 1
    df.drop(columns=["_score_demo"], inplace=True, errors="ignore")
    print("⚠️  Only one class found; applied demo fallback to create positives.")

print("Label counts:", df["label"].value_counts(dropna=False).to_dict())

# ------------------ Train / Test split ------------------
feature_cols = num_cols + cat_cols
X = df[feature_cols].copy()
y = df["label"].values

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.25, random_state=42, stratify=y
)

# ------------------ Pipeline ------------------
preprocess = ColumnTransformer(
    transformers=[("cat", OneHotEncoder(handle_unknown="ignore"), cat_cols)],
    remainder="passthrough",
    verbose_feature_names_out=False
)

rf_clf = Pipeline(steps=[
    ("prep", preprocess),
    ("clf", RandomForestClassifier(
        n_estimators=300,
        max_depth=None,
        n_jobs=-1,
        class_weight="balanced_subsample",
        random_state=42
    ))
])

# ------------------ Train & Evaluate ------------------
print("▶ Training RandomForest ...")
rf_clf.fit(X_train, y_train)

# Scores
if hasattr(rf_clf.named_steps["clf"], "predict_proba"):
    proba_rf = rf_clf.predict_proba(X_test)[:, 1]
else:
    proba_rf = rf_clf.decision_function(X_test)

y_pred_05 = (proba_rf >= 0.5).astype(int)

metrics = {
    "roc_auc": float(roc_auc_score(y_test, proba_rf)) if len(np.unique(y_test))>1 else None,
    "pr_auc": float(average_precision_score(y_test, proba_rf)) if len(np.unique(y_test))>1 else None,
    "report": classification_report(y_test, y_pred_05, output_dict=True),
    "confusion_matrix": confusion_matrix(y_test, y_pred_05).tolist(),
    "n_train": int(len(X_train)),
    "n_test": int(len(X_test)),
    "features_used": feature_cols,
}

# Threshold tuning (target recall 0.80)
p, r, t = precision_recall_curve(y_test, proba_rf)
p_t, r_t, t_t = p[:-1], r[:-1], t        # align with thresholds
TARGET_RECALL = 0.80
mask = r_t >= TARGET_RECALL
if mask.any():
    best_local_idx = np.argmax(p_t[mask])
    global_idx = np.flatnonzero(mask)[best_local_idx]
    best_thr = float(t_t[global_idx])
    best_prec = float(p_t[global_idx]); best_rec = float(r_t[global_idx])
    metrics["threshold"] = {"target_recall": TARGET_RECALL, "value": best_thr,
                            "precision": best_prec, "recall": best_rec}
else:
    f1 = 2 * p_t * r_t / (p_t + r_t + 1e-9)
    global_idx = int(np.nanargmax(f1))
    best_thr = float(t_t[global_idx])
    metrics["threshold"] = {"strategy": "f1_opt", "value": best_thr,
                            "precision": float(p_t[global_idx]), "recall": float(r_t[global_idx])}

y_pred_best = (proba_rf >= best_thr).astype(int)
metrics["report_at_best_thr"] = classification_report(y_test, y_pred_best, output_dict=True)
metrics["confusion_matrix_at_best_thr"] = confusion_matrix(y_test, y_pred_best).tolist()

# ------------------ Save artifacts ------------------
print("▶ Saving artifacts ...")
joblib.dump(rf_clf, ROOT / "models" / "rf_pipeline.joblib")
with open(ROOT / "reports" / "rf_metrics.json", "w") as f:
    json.dump(metrics, f, indent=2)

# ------------------ Save predictions to DB ------------------
print("▶ Writing predictions to DB (bi.model_predictions) ...")
pred_frame = pd.DataFrame({
    "pred_proba": proba_rf,
    "pred_label_05": y_pred_05,
    "pred_label_best": y_pred_best
}, index=X_test.index)

meta_cols = [c for c in ["as_of_date", "ad_id", "ip_address"] if c in df.columns]
if meta_cols:
    pred_frame = df.loc[X_test.index, meta_cols].join(pred_frame)

with engine.begin() as conn:
    conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS bi;")
pred_frame.to_sql("model_predictions", engine, schema="bi", if_exists="replace", index=False)

print("✅ Done. Artifacts in models/ & reports/, predictions in bi.model_predictions")
