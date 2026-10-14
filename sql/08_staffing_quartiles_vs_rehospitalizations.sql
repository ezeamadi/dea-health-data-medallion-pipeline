SELECT 
    quartile, COUNT(*) AS facilities,
    ROUND(AVG(hprd_total), 2) AS avg_hprd,
    ROUND(AVG(rehosp_short_stay_adj), 2) AS avg_rehosp_pct
FROM (
    SELECT 
        hprd_total, 
        rehosp_short_stay_adj, 
        NTILE(4) OVER (ORDER BY hprd_total) AS quartile
    FROM gdrive_gold.gold_facility_quality_staffing
    WHERE census_days >= 45 AND NOT hprd_implausibly_low
        AND hprd_total IS NOT NULL AND rehosp_short_stay_adj IS NOT NULL
) t
GROUP BY quartile 
ORDER BY quartile