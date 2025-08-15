#!/usr/bin/env python3
"""
Enrich TalkingData train_sample.csv with simulated columns for graph-based fraud detection.

Adds (all in English):
- user_id                : simulated user identifier
- ad_id                  : simulated ad / campaign id
- email                  : simulated email (with suspicious variants reuse)
- country                : simulated country
- device_fingerprint     : stable fingerprint; reused within fraud clusters
- connection_type        : wifi / 4g / 5g
- fraud_cluster_id       : >0 for rows belonging to a simulated fraud ring (else 0)
- is_synthetic_fraud     : boolean flag

Output: ./data/train_sample_enriched.csv
"""

import os
import numpy as np
import pandas as pd
from pathlib import Path
import hashlib
import random

# -------------------- Config --------------------
ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
INPUT_CSV  = Path(os.getenv("KAGGLE_FILE", DATA / "train_sample.csv"))
OUTPUT_CSV = DATA / "train_sample_enriched.csv"

SEED = 42
np.random.seed(SEED)
random.seed(SEED)

# fraud injection knobs (lightweight, laptop-friendly)
FRACTION_FRAUD_IPS = 0.05     # ~5% of unique IPs become "hubs"
CLUSTER_MIN_SIZE   = 2        # min number of IPs per cluster (can be 1 as well)
CLUSTER_MAX_SIZE   = 5        # max IP count grouped into a ring
EMAIL_VARIANTS_PER_CLUSTER = 30  # pool size of email handles to reuse
FRAUD_TARGET_ADS   = [f"AD{n:03d}" for n in range(1, 6)]  # fraud often focuses on few ads
ALL_ADS            = [f"AD{n:03d}" for n in range(1, 21)]

COUNTRIES = ["US", "FR", "IN", "CN", "BR", "GB", "DE", "ES", "IT", "MX"]
CONNECTION_TYPES = ["wifi", "4g", "5g"]

# -------------------- Helpers --------------------
def stable_hash_to_int(*vals, modulo=10**9):
    s = "|".join(map(str, vals))
    return int(hashlib.sha256(s.encode()).hexdigest(), 16) % modulo

def gen_user_id(ip, device, os_, jitter=1000000):
    h = stable_hash_to_int(ip, device, os_, modulo=jitter)
    return f"U{h:07d}"

def gen_fingerprint(device, os_, app, channel):
    base = f"{device}-{os_}-{app}-{channel}"
    return hashlib.md5(base.encode()).hexdigest()[:16]  # short and stable

def email_from_handle(handle, domain):
    return f"{handle}@{domain}"

def plus_variant(handle):
    # e.g. "frauder01" -> "frauder01+promo3"
    return f"{handle}+promo{np.random.randint(1,9)}"

def dot_variant(handle):
    # insert a dot at a random position if not already dotted
    if "." in handle or len(handle) < 3:
        return handle
    pos = np.random.randint(1, len(handle)-1)
    return handle[:pos] + "." + handle[pos:]

def underscore_variant(handle):
    if "_" in handle or len(handle) < 3:
        return handle
    pos = np.random.randint(1, len(handle)-1)
    return handle[:pos] + "_" + handle[pos:]

def random_handle(base="user", span=999999):
    return f"{base}{np.random.randint(0, span):06d}"

# -------------------- Load --------------------
print(f"Reading: {INPUT_CSV}")
df = pd.read_csv(INPUT_CSV)
n = len(df)
print(f"Rows: {n:,}")

# talkingdata columns: ip, app, device, os, channel, click_time, attributed_time, is_attributed

# -------------------- Base enrichment (non-fraud default) --------------------
# ad_id: general distribution (fraud will bias later)
df["ad_id"] = np.random.choice(ALL_ADS, size=n, replace=True)

# user_id: stable-ish from core fields (will be overridden in fraud clusters)
df["user_id"] = [
    gen_user_id(ip=row["ip"], device=row["device"], os_=row["os"])
    for _, row in df.iterrows()
]

# country: random, will show inconsistent geography for some fraud rows
df["country"] = np.random.choice(COUNTRIES, size=n, replace=True)

# device_fingerprint: stable mapping; fraud clusters will reuse one fingerprint
df["device_fingerprint"] = [
    gen_fingerprint(row["device"], row["os"], row["app"], row["channel"])
    for _, row in df.iterrows()
]

# connection_type
df["connection_type"] = np.random.choice(CONNECTION_TYPES, size=n, replace=True)

# baseline emails (non-fraud): derived from user_id + domain
domains = ["gmail.com", "yahoo.com", "outlook.com", "proton.me"]
df["email"] = [
    email_from_handle(f"user{str(uid)[-6:]}", np.random.choice(domains))
    for uid in df["user_id"].values
]

# -------------------- Fraud rings injection --------------------
# Pick a subset of IPs to act as "hubs"
unique_ips = df["ip"].unique()
num_hub_ips = max(1, int(len(unique_ips) * FRACTION_FRAUD_IPS))
hub_ips = set(np.random.choice(unique_ips, size=num_hub_ips, replace=False))

# Group hub IPs into small clusters (to simulate rings using multiple IPs)
hub_ips_list = list(hub_ips)
clusters = []
cursor = 0
cid = 1
while cursor < len(hub_ips_list):
    size = np.random.randint(CLUSTER_MIN_SIZE, CLUSTER_MAX_SIZE + 1)
    block = hub_ips_list[cursor: cursor + size]
    clusters.append({"cluster_id": cid, "ips": block})
    cid += 1
    cursor += size

# Prebuild suspicious email handle pools per cluster (to reuse with variants)
def build_email_pool(base_root="frauder", pool_size=EMAIL_VARIANTS_PER_CLUSTER):
    base_handles = [f"{base_root}{i:03d}" for i in range(pool_size)]
    pool = []
    for h in base_handles:
        # create a few variants for each base handle
        pool.append(h)
        pool.append(plus_variant(h))
        pool.append(dot_variant(h))
        pool.append(underscore_variant(h))
    return list(set(pool))

cluster_email_pools = {c["cluster_id"]: build_email_pool() for c in clusters}

# For each cluster, choose a small set of target ads & one shared fingerprint
cluster_target_ads = {
    c["cluster_id"]: np.random.choice(FRAUD_TARGET_ADS, size=np.random.randint(1, 4), replace=False).tolist()
    for c in clusters
}
cluster_fingerprints = {
    c["cluster_id"]: f"fp_{stable_hash_to_int(*c['ips'], modulo=10**8):08d}"
    for c in clusters
}

# Mark all rows with cluster assignment and override fields to simulate fraud
df["fraud_cluster_id"] = 0
df["is_synthetic_fraud"] = False

for c in clusters:
    cid = c["cluster_id"]
    ips = set(c["ips"])
    mask = df["ip"].isin(ips)
    if not mask.any():
        continue

    # Assign cluster id
    df.loc[mask, "fraud_cluster_id"] = cid
    df.loc[mask, "is_synthetic_fraud"] = True

    # Bias ads to a small set (fraud tends to concentrate)
    df.loc[mask, "ad_id"] = np.random.choice(cluster_target_ads[cid], size=mask.sum(), replace=True)

    # Shared fingerprint across the cluster (bot/script reuse)
    df.loc[mask, "device_fingerprint"] = cluster_fingerprints[cid]

    # Reuse email handles within cluster
    handles = np.random.choice(cluster_email_pools[cid], size=mask.sum(), replace=True)
    doms = np.random.choice(["gmail.com", "outlook.com", "mail.ru", "yahoo.com"], size=mask.sum(), replace=True)
    df.loc[mask, "email"] = [email_from_handle(h, d) for h, d in zip(handles, doms)]

    # Shuffle user_id a bit: same IP may rotate many user_ids (sockpuppets)
    # A mix of repeated and new IDs
    base_users = [random_handle("u") for _ in range(max(50, mask.sum() // 10))]
    df.loc[mask, "user_id"] = np.random.choice(
        base_users + df.loc[mask, "user_id"].tolist(), size=mask.sum(), replace=True
    )

    # Optional geographic inconsistency: same email appearing in multiple countries
    # (do it on a subset to avoid making everything messy)
    sub_idx = df.loc[mask].sample(frac=0.30, random_state=SEED).index
    df.loc[sub_idx, "country"] = np.random.choice(COUNTRIES, size=len(sub_idx), replace=True)

# -------------------- Save --------------------
DATA.mkdir(exist_ok=True, parents=True)
df.to_csv(OUTPUT_CSV, index=False)
print(f"✅ Saved enriched sample: {OUTPUT_CSV}")

# -------------------- Quick summary --------------------
total_clusters = (df["fraud_cluster_id"] > 0).sum()
print("\n---- Summary ----")
print("Rows in fraud clusters:", int((df['fraud_cluster_id'] > 0).sum()))
print("Unique fraud clusters :", int(df.loc[df['fraud_cluster_id'] > 0, 'fraud_cluster_id'].nunique()))
print("Example emails in fraud clusters:")
print(df.loc[df["fraud_cluster_id"] > 0, "email"].head(5).to_list())
