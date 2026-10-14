SELECT ccn,
       MAX(provider_name) AS provider_name,
       MAX(state) AS state,
       ROUND(SUM(contract_hrs), 0) AS contract_hrs_total,
       ROUND(100.0 * CAST(SUM(contract_hrs) AS DOUBLE) / SUM(total_nurse_hrs), 1) AS contract_share_pct
FROM gdrive_gold.gold_staffing_facility_monthly
GROUP BY ccn
ORDER BY 4 DESC
LIMIT 10