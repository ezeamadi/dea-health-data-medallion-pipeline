SELECT 
    ccn, 
    provider_name, 
    state, 
    avg_daily_census, 
    hprd_total, 
    hprd_total_rank_national
FROM gdrive_gold.gold_facility_quality_staffing
WHERE hprd_total_rank_national <= 10
ORDER BY hprd_total_rank_national