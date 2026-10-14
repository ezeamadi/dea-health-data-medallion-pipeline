SELECT 
    SUM(CASE WHEN hprd_implausibly_low THEN 1 ELSE 0 END) AS flagged_facilities,
    ROUND(
        100.0 * CAST(SUM(CASE WHEN hprd_implausibly_low 
            THEN avg_daily_census * census_days END) AS DOUBLE)
            / 
            CAST(SUM(avg_daily_census * census_days) AS DOUBLE
        ), 3) AS pct_of_resident_days
FROM gdrive_gold.gold_facility_quality_staffing
WHERE census_days >= 45 AND avg_daily_census IS NOT NULL