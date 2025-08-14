-- Ensure BI schema exists
CREATE SCHEMA IF NOT EXISTS bi;

-- 1️⃣ Daily fraud risk summary
CREATE OR REPLACE VIEW bi.daily_fraud_summary AS
SELECT
    CURRENT_DATE AS snapshot_date,
    COUNT(*) AS total_ips,
    SUM(CASE WHEN fraud_suspected THEN 1 ELSE 0 END) AS suspected_ips,
    ROUND(100.0 * SUM(CASE WHEN fraud_suspected THEN 1 ELSE 0 END) / COUNT(*), 2) AS suspected_ips_pct,
    AVG(fraud_risk_score) AS avg_risk_score
FROM bi.fraud_features;

-- 2️⃣ High-risk IPs detail
CREATE OR REPLACE VIEW bi.high_risk_ips AS
SELECT
    ip_address,
    total_clicks,
    total_conversions,
    click_to_conv_ratio,
    avg_time_between_clicks_sec,
    unique_devices_per_ip,
    avg_email_risk_score,
    fraud_risk_score,
    fraud_suspected
FROM bi.fraud_features
WHERE fraud_suspected = TRUE
ORDER BY fraud_risk_score DESC, total_clicks DESC;

-- 3️⃣ Device distribution for high-risk IPs
CREATE OR REPLACE VIEW bi.high_risk_device_distribution AS
SELECT
    f.ip_address,
    d.device_type,
    COUNT(*) AS clicks_from_device
FROM bi.fraud_features f
JOIN clean.raw_clicks d
    ON f.ip_address = d.ip_address
WHERE f.fraud_suspected = TRUE
GROUP BY f.ip_address, d.device_type
ORDER BY clicks_from_device DESC;

-- 4️⃣ Fraud risk trend (daily snapshot over time)
-- This will work once you set up a daily job to snapshot fraud_features
CREATE OR REPLACE VIEW bi.fraud_trend AS
SELECT
    snapshot_date,
    AVG(fraud_risk_score) AS avg_risk_score,
    SUM(CASE WHEN fraud_suspected THEN 1 ELSE 0 END) AS suspected_ips
FROM bi.daily_fraud_summary
GROUP BY snapshot_date
ORDER BY snapshot_date;

-- 5️⃣ Campaign-level fraud impact
CREATE OR REPLACE VIEW bi.campaign_fraud_impact AS
SELECT
    a.campaign_name,
    COUNT(DISTINCT f.ip_address) AS unique_ips,
    SUM(CASE WHEN f.fraud_suspected THEN 1 ELSE 0 END) AS suspected_ips,
    ROUND(100.0 * SUM(CASE WHEN f.fraud_suspected THEN 1 ELSE 0 END) / COUNT(DISTINCT f.ip_address), 2) AS suspected_pct
FROM clean.ads a
JOIN clean.raw_clicks rc
    ON a.ad_id = rc.ad_id
JOIN bi.fraud_features f
    ON rc.ip_address = f.ip_address
GROUP BY a.campaign_name
ORDER BY suspected_pct DESC;
