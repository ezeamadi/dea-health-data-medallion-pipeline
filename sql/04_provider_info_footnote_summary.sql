SELECT staffing_rating_footnote, 
    reported_staffing_footnote, 
    COUNT(*) AS facilities,
    MIN(reported_total_nurse_staffing_hours_per_resident_per_day) AS min_cms_hprd,
    MAX(reported_total_nurse_staffing_hours_per_resident_per_day) AS max_cms_hprd
FROM gdrive_silver.nh_provider_info
WHERE processing_date = (
    SELECT MAX(processing_date) 
    FROM gdrive_silver.nh_provider_info
)
GROUP BY 1, 2 
ORDER BY facilities DESC