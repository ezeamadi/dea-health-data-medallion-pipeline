SELECT ccn, 
    provider_name, 
    state, 
    avg_daily_census, 
    hprd_total, 
    cms_reported_total_hprd,
    staffing_rating, 
    staffing_rating_footnote, 
    reported_staffing_footnote
FROM gdrive_gold.gold_facility_quality_staffing
WHERE hprd_implausibly_low 
ORDER BY hprd_total