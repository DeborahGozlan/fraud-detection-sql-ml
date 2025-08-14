-- 1) Table cible (features jour/par IP/par annonce)
CREATE TABLE IF NOT EXISTS fraud_signals (
    as_of_date        date,
    ad_id             varchar(50),
    ip_address        inet,
    clicks_day        int,
    uniq_devices_day  int,
    uniq_referrers_day int,
    burst_max_5min    int,
    uniq_ads_ip_24h   int,
    ctr_7d            numeric(6,4),
    conv_rate_7d      numeric(6,4),
    night_click_rate  numeric(6,4),
    invalid_ip_flag   boolean,
    missing_device_flag boolean,
    suspicious_ctr_flag boolean,
    PRIMARY KEY (as_of_date, ad_id, ip_address)
);

-- 2) Fenêtre temporelle configurable
WITH
-- A. base des clics du jour
clicks_day AS (
    SELECT
        date_trunc('day', rc.click_time)::date AS as_of_date,
        rc.ad_id,
        rc.ip_address,
        COUNT(*) AS clicks_day,
        COUNT(DISTINCT rc.device_type) AS uniq_devices_day,
        COUNT(DISTINCT rc.referrer_url) AS uniq_referrers_day,
        -- fenêtre glissante 5 minutes pour capturer les bursts
        MAX(cnt_5min) AS burst_max_5min
    FROM (
        SELECT
            rc.*,
            COUNT(*) OVER (
                PARTITION BY rc.ad_id, rc.ip_address
                ORDER BY rc.click_time
                RANGE BETWEEN INTERVAL '5 minutes' PRECEDING AND CURRENT ROW
            ) AS cnt_5min
        FROM raw_clicks rc
    ) rc
    GROUP BY 1,2,3
),

-- B. nombre d'annonces différentes touchées par la même IP dans les 24 dernières heures
uniq_ads_ip_24h AS (
    SELECT
        date_trunc('day', rc.click_time)::date AS as_of_date,
        rc.ip_address,
        COUNT(DISTINCT rc.ad_id) AS uniq_ads_ip_24h
    FROM raw_clicks rc
    WHERE rc.click_time >= NOW() - INTERVAL '24 hours'
    GROUP BY 1,2
),

-- C. agrégats de performance à 7 jours (CTR et conv rate)
perf_7d AS (
    SELECT
        ap.ad_id,
        AVG(ap.ctr) AS ctr_7d,
        AVG(ap.conversion_rate) AS conv_rate_7d
    FROM ad_performance ap
    WHERE ap.date >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY ap.ad_id
),

-- D. ratio de clics la nuit (00:00–06:00) par IP/ad sur la journée
night_share AS (
    SELECT
        date_trunc('day', rc.click_time)::date AS as_of_date,
        rc.ad_id,
        rc.ip_address,
        AVG( CASE WHEN EXTRACT(HOUR FROM rc.click_time) BETWEEN 0 AND 5 THEN 1.0 ELSE 0.0 END ) AS night_click_rate
    FROM raw_clicks rc
    GROUP BY 1,2,3
)

-- 3) Upsert vers fraud_signals
INSERT INTO fraud_signals AS fs (
    as_of_date, ad_id, ip_address,
    clicks_day, uniq_devices_day, uniq_referrers_day, burst_max_5min,
    uniq_ads_ip_24h, ctr_7d, conv_rate_7d, night_click_rate,
    invalid_ip_flag, missing_device_flag, suspicious_ctr_flag
)
SELECT
    cd.as_of_date,
    cd.ad_id,
    cd.ip_address,
    cd.clicks_day,
    cd.uniq_devices_day,
    cd.uniq_referrers_day,
    cd.burst_max_5min,
    COALESCE(u24.uniq_ads_ip_24h, 1) AS uniq_ads_ip_24h,
    COALESCE(p7.ctr_7d, 0.0) AS ctr_7d,
    COALESCE(p7.conv_rate_7d, 0.0) AS conv_rate_7d,
    COALESCE(ns.night_click_rate, 0.0) AS night_click_rate,
    -- flags simples et lisibles
    (cd.clicks_day > 0 AND host(cd.ip_address) = '255.255.255.255') AS invalid_ip_flag,
    (cd.uniq_devices_day = 0) AS missing_device_flag,
    (COALESCE(p7.ctr_7d,0.0) > 0.5) AS suspicious_ctr_flag
FROM clicks_day cd
LEFT JOIN uniq_ads_ip_24h u24
  ON u24.as_of_date = cd.as_of_date AND u24.ip_address = cd.ip_address
LEFT JOIN perf_7d p7
  ON p7.ad_id = cd.ad_id
LEFT JOIN night_share ns
  ON ns.as_of_date = cd.as_of_date AND ns.ad_id = cd.ad_id AND ns.ip_address = cd.ip_address
ON CONFLICT (as_of_date, ad_id, ip_address) DO UPDATE SET
    clicks_day = EXCLUDED.clicks_day,
    uniq_devices_day = EXCLUDED.uniq_devices_day,
    uniq_referrers_day = EXCLUDED.uniq_referrers_day,
    burst_max_5min = EXCLUDED.burst_max_5min,
    uniq_ads_ip_24h = EXCLUDED.uniq_ads_ip_24h,
    ctr_7d = EXCLUDED.ctr_7d,
    conv_rate_7d = EXCLUDED.conv_rate_7d,
    night_click_rate = EXCLUDED.night_click_rate,
    invalid_ip_flag = EXCLUDED.invalid_ip_flag,
    missing_device_flag = EXCLUDED.missing_device_flag,
    suspicious_ctr_flag = EXCLUDED.suspicious_ctr_flag
;
CREATE INDEX IF NOT EXISTS idx_rc_ad_ip_time ON raw_clicks(ad_id, ip_address, click_time);
CREATE INDEX IF NOT EXISTS idx_rc_ip_time     ON raw_clicks(ip_address, click_time);
CREATE INDEX IF NOT EXISTS idx_ap_ad_date     ON ad_performance(ad_id, date);
CREATE INDEX IF NOT EXISTS idx_fs_ad_ip_date  ON fraud_signals(ad_id, ip_address, as_of_date);
