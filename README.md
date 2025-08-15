# Fraud Detection in Digital Advertising Using SQL, Machine Learning, and Graph Theory

## I. Overview
This project detects fraudulent clicks and suspicious advertising behavior using:
- **SQL-first ingestion & cleaning**
- **Supervised & unsupervised machine learning**
- **Graph theory** for fraud community detection

It is designed to simulate **real-world messy data**, clean and prepare it in **SQL and Python**, engineer fraud-relevant features, and apply multiple fraud detection techniques before presenting insights in **Tableau/Looker dashboards**.

about the database: 
original database from kaggle:
contains 8 variables ip, app, device, os, channel, click_time, attributed_time, is_attributed
only technical
enrich database that we are adding with the fallowing variables: 
Column Name	Type	Description
user_id	int / string	Simulated unique user ID
ad_id	int / string	Simulated ad / campaign ID
email	string	Simulated email (some duplicated / modified for fraud)
country	string	Simulated country of click
device_fingerprint	string	Unique ID based on device+OS+browser
connection_type	string	e.g., wifi, 4g, 5g (useful for fraud detection)

💡 Fraud simulation rules :

5% of IPs will be linked to multiple user_ids.

Some emails will appear in different countries within short time intervals.

Certain device fingerprints will be reused by many user_ids.

our que Louvain trouve de vraies communautés :

Relier :

IP ↔ Email

IP ↔ Device

Email ↔ Campaign

Device ↔ App

Injecter des hubs frauduleux :

1 IP reliée à 20 comptes différents.

1 e-mail reliée à plusieurs devices.

## II. Goals
- Build a SQL-first data ingestion & cleaning pipeline
- Simulate realistic messy data to showcase cleaning expertise
- Apply supervised & unsupervised ML for fraud detection
- Use graph theory to detect fraudster communities
- Build Tableau & Looker dashboards for fraud monitoring

## III. Tools & Technologies
- **SQL (PostgreSQL)** for ingestion, cleaning, KPI queries
- **Python**: pandas, scikit-learn, fuzzywuzzy, networkx, re (regex)
- **ML**: Logistic Regression, Random Forest, XGBoost, Isolation Forest, DBSCAN
- **Graph Theory**: Louvain algorithm for community detection
- **Visualization**: Tableau, Looker

## IV. Data Cleaning
We simulate real-world messy data to reflect multiple sources:
- Multiple date formats
- Text casing inconsistencies
- Numeric values stored as text
- Typos in categories
- Messy free-text fields (`contact_email`, `customer_notes`)

**Advanced Regex Example (Email Cleaning)**:
```python
import re

def clean_email(email):
    if not isinstance(email, str):
        return None
    email = email.strip().lower()
    email = re.sub(r"\s*\(at\)\s*|\s*\[at\]\s*", "@", email)
    email = re.sub(r"\s*@\s*", "@", email)
    email = re.sub(r"[^a-z0-9@._-]+", "", email)
    if not re.match(r"^[a-z0-9._%-]+@[a-z0-9.-]+\.[a-z]{2,}$", email):
        return None
    return email
```

## V. Live Project Resources
- [Project_Plan.pdf](Project_Plan.pdf)
- Tableau Fraud Dashboard (coming soon)
- Looker Fraud Dashboard (coming soon)

## VI. Deliverables
- PostgreSQL database (raw, messy, cleaned data)
- Jupyter notebooks (SQL + Python integration)
- Machine Learning models
- Tableau & Looker dashboards
- Excel data dictionary
- PDF project summary

## VII. Author
**Deborah Gozlan**  
Fraud Data Analyst  
[LinkedIn Profile](https://www.linkedin.com) (update link)
