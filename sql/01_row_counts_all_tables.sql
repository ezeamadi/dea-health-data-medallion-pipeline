SELECT t, n, expected, n - expected AS diff FROM (
  SELECT 'silver.pbj_daily_nurse_staffing' AS t, COUNT(*) AS n, 1325324 AS expected FROM gdrive_silver.pbj_daily_nurse_staffing
  UNION ALL SELECT 'silver.snf_qrp_provider_data', COUNT(*), 711072 FROM gdrive_silver.snf_qrp_provider_data
  UNION ALL SELECT 'silver.nh_health_citations', COUNT(*), 402424 FROM gdrive_silver.nh_health_citations
  UNION ALL SELECT 'silver.nh_quality_msr_mds', COUNT(*), 266652 FROM gdrive_silver.nh_quality_msr_mds
  UNION ALL SELECT 'silver.nh_fire_safety_citations', COUNT(*), 200003 FROM gdrive_silver.nh_fire_safety_citations
  UNION ALL SELECT 'silver.nh_survey_dates', COUNT(*), 159723 FROM gdrive_silver.nh_survey_dates
  UNION ALL SELECT 'silver.nh_ownership', COUNT(*), 145354 FROM gdrive_silver.nh_ownership
  UNION ALL SELECT 'silver.nh_quality_msr_claims', COUNT(*), 59256 FROM gdrive_silver.nh_quality_msr_claims
  UNION ALL SELECT 'silver.nh_survey_summary', COUNT(*), 44243 FROM gdrive_silver.nh_survey_summary
  UNION ALL SELECT 'silver.swing_bed_snf_data', COUNT(*), 38064 FROM gdrive_silver.swing_bed_snf_data
  UNION ALL SELECT 'silver.nh_penalties', COUNT(*), 28505 FROM gdrive_silver.nh_penalties
  UNION ALL SELECT 'silver.nh_provider_info', COUNT(*), 14814 FROM gdrive_silver.nh_provider_info
  UNION ALL SELECT 'silver.nh_covid_vax_provider', COUNT(*), 14814 FROM gdrive_silver.nh_covid_vax_provider
  UNION ALL SELECT 'silver.snf_vbp_facility_performance', COUNT(*), 10858 FROM gdrive_silver.snf_vbp_facility_performance
  UNION ALL SELECT 'silver.nh_citation_descriptions', COUNT(*), 641 FROM gdrive_silver.nh_citation_descriptions
  UNION ALL SELECT 'silver.nh_state_us_averages', COUNT(*), 54 FROM gdrive_silver.nh_state_us_averages
  UNION ALL SELECT 'silver.nh_covid_vax_averages', COUNT(*), 54 FROM gdrive_silver.nh_covid_vax_averages
  UNION ALL SELECT 'silver.nh_hlth_inspec_cutpoints_state', COUNT(*), 53 FROM gdrive_silver.nh_hlth_inspec_cutpoints_state
  UNION ALL SELECT 'silver.nh_data_collection_intervals', COUNT(*), 45 FROM gdrive_silver.nh_data_collection_intervals
  UNION ALL SELECT 'silver.snf_qrp_national_data', COUNT(*), 24 FROM gdrive_silver.snf_qrp_national_data
  UNION ALL SELECT 'silver.snf_vbp_aggregate_performance', COUNT(*), 1 FROM gdrive_silver.snf_vbp_aggregate_performance
  UNION ALL SELECT 'gold.gold_staffing_daily', COUNT(*), 1325324 FROM gdrive_gold.gold_staffing_daily
  UNION ALL SELECT 'gold.gold_staffing_facility_monthly', COUNT(*), 43692 FROM gdrive_gold.gold_staffing_facility_monthly
  UNION ALL SELECT 'gold.gold_staffing_state_monthly', COUNT(*), 156 FROM gdrive_gold.gold_staffing_state_monthly
  UNION ALL SELECT 'gold.gold_staffing_state_ownership', COUNT(*), 486 FROM gdrive_gold.gold_staffing_state_ownership
  UNION ALL SELECT 'gold.gold_facility_quality_staffing', COUNT(*), 14564 FROM gdrive_gold.gold_facility_quality_staffing
  UNION ALL SELECT 'gold.gold_relationship_stats', COUNT(*), 12 FROM gdrive_gold.gold_relationship_stats
) ORDER BY t