# Fraud Detection in Digital Advertising Using SQL, Machine Learning, and Graph Theory

## I. Overview
This project detects fraudulent clicks and suspicious advertising behavior using:
- **SQL-first ingestion & cleaning**
- **Supervised & unsupervised machine learning**
- **Graph theory** for fraud community detection

It is designed to simulate **real-world messy data**, clean and prepare it in **SQL and Python**, engineer fraud-relevant features, and apply multiple fraud detection techniques before presenting insights in **Tableau/Looker dashboards**.

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
