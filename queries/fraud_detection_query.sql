WITH ip_activity AS (
    SELECT 
        ip_address,
        COUNT(*) AS total_clicks
    FROM raw_clicks
    GROUP BY ip_address
),
click_bursts AS (
    SELECT 
        ad_id,
        ip_address,
        COUNT(*) AS clicks_in_minute
    FROM raw_clicks
	WHERE click_time::timestamp >= NOW() - INTERVAL '1 minute'
    GROUP BY ad_id, ip_address
),
performance_flags AS (
    SELECT
        ad_id,
        CASE WHEN ctr > 0.5 THEN 1 ELSE 0 END AS suspicious_ctr
    FROM ad_performance
)
SELECT
    rc.ad_id,
    rc.ip_address,
    COUNT(*) AS total_clicks_ip,
    MAX(pf.suspicious_ctr) AS suspicious_ctr,
    CASE WHEN ia.total_clicks > 10 THEN 'High IP usage' END AS reason_ip,
    CASE WHEN cb.clicks_in_minute > 5 THEN 'Burst clicks' END AS reason_burst,
    CASE WHEN rc.device_type IS NULL THEN 'Missing device info' END AS reason_missing_device,
    CASE WHEN rc.ip_address = '999.999.999.999' THEN 'Invalid IP' END AS reason_invalid_ip
FROM raw_clicks rc
LEFT JOIN ip_activity ia ON rc.ip_address = ia.ip_address
LEFT JOIN click_bursts cb ON rc.ip_address = cb.ip_address AND rc.ad_id = cb.ad_id
LEFT JOIN performance_flags pf ON rc.ad_id = pf.ad_id
GROUP BY rc.ad_id, rc.ip_address, ia.total_clicks, cb.clicks_in_minute, rc.device_type
HAVING 
    ia.total_clicks > 10
    OR cb.clicks_in_minute > 5
    OR rc.device_type IS NULL
    OR rc.ip_address = '999.999.999.999'
    OR MAX(pf.suspicious_ctr) = 1
ORDER BY total_clicks_ip DESC;
