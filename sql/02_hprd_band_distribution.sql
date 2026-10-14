SELECT CASE WHEN hprd_total < 0.5 THEN 'a: under 0.5'
            WHEN hprd_total < 1   THEN 'b: 0.5 to 1'
            WHEN hprd_total < 2   THEN 'c: 1 to 2'
            WHEN hprd_total < 3   THEN 'd: 2 to 3'
            ELSE 'e: 3 or more' END AS band,
       COUNT(*) AS facilities
FROM gdrive_gold.gold_facility_quality_staffing
WHERE census_days >= 45 AND hprd_total IS NOT NULL
GROUP BY 1
ORDER BY 1