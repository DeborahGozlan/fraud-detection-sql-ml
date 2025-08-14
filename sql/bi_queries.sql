-- =====================================================
-- BI Queries for Fraud Detection Project
-- Clean schema: ready for Tableau/Looker
-- =====================================================

-- 1️⃣ Daily Clicks, Impressions & CTR per Campaign
SELECT
    a.campaign_name,
    p.date,
    SUM(p.impressions) AS total_impressions,
    SUM(p.clicks) AS total_clicks,
    ROUND(SUM(p.clicks)::NUMERIC / NULLIF(SUM(p.impressions),0), 4) AS ctr
FROM clean.ads a
JOIN clean.ad_performance p USING (ad_id)
GROUP BY a.campaign_name, p.date
ORDER BY p.date, a.campaign_name;

-- 2️⃣ Fraud Rate by Campaign
SELECT
    a.campaign_name,
    ROUND(100.0 * SUM(CASE WHEN p.fraud THEN 1 ELSE 0 END) / COUNT(*), 2) AS fraud_rate_pct
FROM clean.ads a
JOIN clean.ad_performance p USING (ad_id)
GROUP BY a.campaign_name
ORDER BY fraud_rate_pct DESC;

-- 3️⃣ Top 10 IPs by Click Volume (possible click fraud)
SELECT
    r.ip_address,
    COUNT(*) AS total_clicks,
    COUNT(DISTINCT r.ad_id) AS ads_clicked
FROM clean.raw_clicks r
GROUP BY r.ip_address
ORDER BY total_clicks DESC
LIMIT 10;

-- 4️⃣ Device Type Distribution
SELECT
    LOWER(TRIM(device_type)) AS device_type_clean,
    COUNT(*) AS clicks
FROM clean.raw_clicks
GROUP BY device_type_clean
ORDER BY clicks DESC;

-- 5️⃣ Suspicious Connections: Same IP, Multiple Ads in Short Time
SELECT
    ip_address,
    COUNT(DISTINCT ad_id) AS ads_clicked,
    MIN(connection_datetime) AS first_seen,
    MAX(connection_datetime) AS last_seen,
    EXTRACT(EPOCH FROM (MAX(connection_datetime) - MIN(connection_datetime)))/60 AS minutes_span
FROM clean.ad_connections
GROUP BY ip_address
HAVING COUNT(DISTINCT ad_id) > 3
   AND EXTRACT(EPOCH FROM (MAX(connection_datetime) - MIN(connection_datetime)))/60 < 10
ORDER BY ads_clicked DESC, minutes_span ASC;

-- 6️⃣ Conversion Funnel: From Clicks to Conversions
SELECT
    a.campaign_name,
    SUM(p.clicks) AS total_clicks,
    SUM(p.conversions) AS total_conversions,
    ROUND(100.0 * SUM(p.conversions) / NULLIF(SUM(p.clicks),0), 2) AS conversion_rate_pct
FROM clean.ads a
JOIN clean.ad_performance p USING (ad_id)
GROUP BY a.campaign_name
ORDER BY conversion_rate_pct DESC;

-- 7️⃣ Bounce Rate Analysis
SELECT
    a.campaign_name,
    ROUND(100.0 * AVG(p.bounce_rate), 2) AS avg_bounce_rate_pct
FROM clean.ads a
JOIN clean.ad_performance p USING (ad_id)
GROUP BY a.campaign_name
ORDER BY avg_bounce_rate_pct DESC;
