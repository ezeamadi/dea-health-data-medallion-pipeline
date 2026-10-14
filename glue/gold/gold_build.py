import sys
import boto3

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.context import SparkContext

# ---------------------------------------------------------------
# 1. Job setup
# ---------------------------------------------------------------

args = getResolvedOptions(sys.argv, ["JOB_NAME", "TABLES", "GOLD_BUCKET", "GOLD_DB"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Same behaviour as the Silver job: unusable values become NULL instead of errors
spark.conf.set("spark.sql.ansi.enabled", "false")

# ---------------------------------------------------------------
# 2. Gold table definitions
#    Each table: the SQL that builds it, its partition column(s),
#    and checks as (name, SQL returning one number, max allowed, severity).
#    In the SQL of a check, "candidate" is the table just built.
# ---------------------------------------------------------------

DAILY_SQL = """
    WITH info AS (
      SELECT ccn,
             ownership_type,
             provider_type,
             number_of_certified_beds AS certified_beds,
             provider_resides_in_hospital AS in_hospital_flag,
             overall_rating,
             staffing_rating
      FROM gdrive_silver.nh_provider_info
      WHERE processing_date = (SELECT MAX(processing_date) FROM gdrive_silver.nh_provider_info)
    ),
    p AS (
      SELECT
        ccn, work_date, cy_qtr, provider_name, city, state, county_name, county_fips,
        mds_census,
        COALESCE(hrs_rndon, 0) + COALESCE(hrs_rnadmin, 0) + COALESCE(hrs_rn, 0) AS rn_hrs,
        COALESCE(hrs_lpnadmin, 0) + COALESCE(hrs_lpn, 0) AS lpn_hrs,
        COALESCE(hrs_cna, 0) AS cna_hrs,
        COALESCE(hrs_natrn, 0) + COALESCE(hrs_medaide, 0) AS other_aide_hrs,
        COALESCE(hrs_rndon_ctr, 0) + COALESCE(hrs_rnadmin_ctr, 0) + COALESCE(hrs_rn_ctr, 0) AS rn_hrs_ctr,
        COALESCE(hrs_lpnadmin_ctr, 0) + COALESCE(hrs_lpn_ctr, 0) AS lpn_hrs_ctr,
        COALESCE(hrs_cna_ctr, 0) AS cna_hrs_ctr,
        COALESCE(hrs_natrn_ctr, 0) + COALESCE(hrs_medaide_ctr, 0) AS other_aide_hrs_ctr,
        dq_flags
      FROM gdrive_silver.pbj_daily_nurse_staffing
    ),
    t AS (
      SELECT p.*,
             p.rn_hrs + p.lpn_hrs + p.cna_hrs + p.other_aide_hrs AS total_nurse_hrs,
             p.rn_hrs_ctr + p.lpn_hrs_ctr + p.cna_hrs_ctr + p.other_aide_hrs_ctr AS contract_hrs
      FROM p
    )
    SELECT
      t.ccn,
      t.work_date,
      YEAR(t.work_date) AS year,
      QUARTER(t.work_date) AS quarter,
      t.cy_qtr,
      MONTH(t.work_date) AS month_num,
      DATE_FORMAT(t.work_date, 'MMMM') AS month_name,
      DATE_FORMAT(t.work_date, 'yyyy-MM') AS year_month,
      ((DAYOFWEEK(t.work_date) + 5) % 7) + 1 AS day_of_week,
      DATE_FORMAT(t.work_date, 'EEEE') AS day_name,
      DAYOFWEEK(t.work_date) IN (1, 7) AS is_weekend,
      t.provider_name,
      t.city,
      t.state,
      t.county_name,
      t.county_fips,
      COALESCE(i.ownership_type, 'Unknown') AS ownership_type,
      COALESCE(i.provider_type, 'Unknown') AS provider_type,
      i.certified_beds,
      i.in_hospital_flag,
      i.overall_rating,
      i.staffing_rating,
      (i.ccn IS NOT NULL) AS has_provider_info,
      t.mds_census,
      t.rn_hrs,
      t.lpn_hrs,
      t.cna_hrs,
      t.other_aide_hrs,
      t.total_nurse_hrs,
      t.rn_hrs_ctr,
      t.lpn_hrs_ctr,
      t.cna_hrs_ctr,
      t.other_aide_hrs_ctr,
      t.contract_hrs,
      t.total_nurse_hrs - t.contract_hrs AS employee_hrs,
      CAST(CASE WHEN t.mds_census > 0 THEN t.total_nurse_hrs / t.mds_census END AS DECIMAL(10,4)) AS hprd_total,
      CAST(CASE WHEN t.mds_census > 0 THEN t.rn_hrs / t.mds_census END AS DECIMAL(10,4)) AS hprd_rn,
      CAST(CASE WHEN t.mds_census > 0 THEN t.lpn_hrs / t.mds_census END AS DECIMAL(10,4)) AS hprd_lpn,
      CAST(CASE WHEN t.mds_census > 0 THEN t.cna_hrs / t.mds_census END AS DECIMAL(10,4)) AS hprd_cna,
      CAST(CASE WHEN t.mds_census > 0 THEN t.other_aide_hrs / t.mds_census END AS DECIMAL(10,4)) AS hprd_other_aide,
      CAST(CASE WHEN i.certified_beds > 0 AND t.mds_census > 0 THEN 100.0 * t.mds_census / i.certified_beds END AS DECIMAL(10,4)) AS occupancy_rate,
      CAST(CASE WHEN t.total_nurse_hrs > 0 THEN 100.0 * t.contract_hrs / t.total_nurse_hrs END AS DECIMAL(10,4)) AS contract_share_pct,
      (t.mds_census = 0) AS is_zero_census,
      (t.mds_census >= 10 AND t.total_nurse_hrs > 24 * t.mds_census) AS is_hprd_outlier,
      t.dq_flags,
      CURRENT_TIMESTAMP() AS built_at
    FROM t
    LEFT JOIN info i ON t.ccn = i.ccn
"""

MONTHLY_SQL = """
    WITH d AS (
      SELECT * FROM gdrive_gold.gold_staffing_daily
    ),
    loc AS (
      SELECT ccn, latitude, longitude
      FROM gdrive_silver.nh_provider_info
      WHERE processing_date = (SELECT MAX(processing_date) FROM gdrive_silver.nh_provider_info)
    ),
    m AS (
      SELECT
        d.ccn,
        d.year_month,
        MAX(d.year) AS year,
        MAX(d.quarter) AS quarter,
        MAX(d.month_num) AS month_num,
        MAX(d.month_name) AS month_name,
        MAX(d.provider_name) AS provider_name,
        MAX(d.city) AS city,
        MAX(d.state) AS state,
        MAX(d.county_name) AS county_name,
        MAX(d.ownership_type) AS ownership_type,
        MAX(d.provider_type) AS provider_type,
        MAX(d.certified_beds) AS certified_beds,
        MAX(d.overall_rating) AS overall_rating,
        MAX(d.staffing_rating) AS staffing_rating,
        MAX(l.latitude) AS latitude,
        MAX(l.longitude) AS longitude,
        (MAX(CASE WHEN d.has_provider_info THEN 1 ELSE 0 END) = 1) AS has_provider_info,
        COUNT(*) AS days_reported,
        SUM(CASE WHEN d.is_zero_census THEN 1 ELSE 0 END) AS zero_census_days,
        SUM(CASE WHEN d.is_hprd_outlier THEN 1 ELSE 0 END) AS hprd_outlier_days,
        SUM(d.mds_census) AS resident_days,
        SUM(CASE WHEN NOT d.is_zero_census THEN 1 ELSE 0 END) AS census_days,
        SUM(d.total_nurse_hrs) AS total_nurse_hrs,
        SUM(d.rn_hrs) AS rn_hrs,
        SUM(d.lpn_hrs) AS lpn_hrs,
        SUM(d.cna_hrs) AS cna_hrs,
        SUM(d.other_aide_hrs) AS other_aide_hrs,
        SUM(d.contract_hrs) AS contract_hrs,
        SUM(d.employee_hrs) AS employee_hrs,
        SUM(CASE WHEN NOT d.is_zero_census AND NOT d.is_hprd_outlier THEN d.mds_census END) AS hprd_resident_days,
        SUM(CASE WHEN NOT d.is_zero_census AND NOT d.is_hprd_outlier THEN d.total_nurse_hrs END) AS hprd_numerator_hrs,
        SUM(CASE WHEN NOT d.is_zero_census AND NOT d.is_hprd_outlier THEN d.rn_hrs END) AS rn_num,
        SUM(CASE WHEN NOT d.is_zero_census AND NOT d.is_hprd_outlier THEN d.lpn_hrs END) AS lpn_num,
        SUM(CASE WHEN NOT d.is_zero_census AND NOT d.is_hprd_outlier THEN d.cna_hrs END) AS cna_num,
        SUM(CASE WHEN NOT d.is_zero_census AND NOT d.is_hprd_outlier THEN d.other_aide_hrs END) AS aide_num
      FROM d
      LEFT JOIN loc l ON d.ccn = l.ccn
      GROUP BY d.ccn, d.year_month
    ),
    o AS (
      SELECT m.*,
             CASE WHEN m.certified_beds > 0 AND m.census_days > 0
                  THEN 100.0 * m.resident_days / (m.certified_beds * m.census_days) END AS occ
      FROM m
    )
    SELECT
      ccn,
      year_month,
      CAST(CONCAT(year_month, '-01') AS DATE) AS month_start_date,
      year, quarter, month_num, month_name,
      provider_name, city, state, county_name, ownership_type, provider_type,
      certified_beds, overall_rating, staffing_rating, latitude, longitude, has_provider_info,
      CAST(days_reported AS INT) AS days_reported,
      CAST(zero_census_days AS INT) AS zero_census_days,
      CAST(hprd_outlier_days AS INT) AS hprd_outlier_days,
      CAST(resident_days AS INT) AS resident_days,
      CAST(CASE WHEN census_days > 0 THEN resident_days / census_days END AS DECIMAL(10,2)) AS avg_daily_census,
      CAST(occ AS DECIMAL(10,4)) AS occupancy_rate,
      COALESCE(occ > 150, FALSE) AS occupancy_unreliable,
      total_nurse_hrs, rn_hrs, lpn_hrs, cna_hrs, other_aide_hrs, contract_hrs, employee_hrs,
      CAST(hprd_resident_days AS INT) AS hprd_resident_days,
      hprd_numerator_hrs,
      rn_num AS hprd_numerator_rn_hrs,
      lpn_num AS hprd_numerator_lpn_hrs,
      cna_num AS hprd_numerator_cna_hrs,
      aide_num AS hprd_numerator_other_aide_hrs,
      CAST(CASE WHEN hprd_resident_days > 0 THEN hprd_numerator_hrs / hprd_resident_days END AS DECIMAL(10,4)) AS hprd_total,
      CAST(CASE WHEN hprd_resident_days > 0 THEN rn_num / hprd_resident_days END AS DECIMAL(10,4)) AS hprd_rn,
      CAST(CASE WHEN hprd_resident_days > 0 THEN lpn_num / hprd_resident_days END AS DECIMAL(10,4)) AS hprd_lpn,
      CAST(CASE WHEN hprd_resident_days > 0 THEN cna_num / hprd_resident_days END AS DECIMAL(10,4)) AS hprd_cna,
      CAST(CASE WHEN hprd_resident_days > 0 THEN aide_num / hprd_resident_days END AS DECIMAL(10,4)) AS hprd_other_aide,
      CAST(CASE WHEN total_nurse_hrs > 0 THEN 100.0 * contract_hrs / total_nurse_hrs END AS DECIMAL(10,4)) AS contract_share_pct,
      CURRENT_TIMESTAMP() AS built_at
    FROM o
"""

STATE_MONTHLY_SQL = """
    WITH f AS (
      SELECT m.*,
             (m.days_reported - m.zero_census_days) AS census_days,
             (m.certified_beds > 0 AND (m.days_reported - m.zero_census_days) > 0
              AND NOT m.occupancy_unreliable) AS occ_ok
      FROM gdrive_gold.gold_staffing_facility_monthly m
    ),
    a AS (
      SELECT
        state, year_month,
        MAX(year) AS year,
        MAX(quarter) AS quarter,
        MAX(month_num) AS month_num,
        MAX(month_name) AS month_name,
        COUNT(*) AS facilities_reporting,
        SUM(resident_days) AS resident_days,
        SUM(total_nurse_hrs) AS total_nurse_hrs,
        SUM(rn_hrs) AS rn_hrs,
        SUM(lpn_hrs) AS lpn_hrs,
        SUM(cna_hrs) AS cna_hrs,
        SUM(other_aide_hrs) AS other_aide_hrs,
        SUM(contract_hrs) AS contract_hrs,
        SUM(employee_hrs) AS employee_hrs,
        SUM(hprd_resident_days) AS hprd_resident_days,
        SUM(hprd_numerator_hrs) AS hprd_numerator_hrs,
        SUM(hprd_numerator_rn_hrs) AS num_rn,
        SUM(hprd_numerator_lpn_hrs) AS num_lpn,
        SUM(hprd_numerator_cna_hrs) AS num_cna,
        SUM(hprd_numerator_other_aide_hrs) AS num_aide,
        AVG(hprd_total) AS hprd_facility_avg,
        SUM(CASE WHEN occ_ok THEN resident_days END) AS occ_rd,
        SUM(CASE WHEN occ_ok THEN certified_beds * census_days END) AS occ_bd,
        SUM(CASE WHEN occupancy_unreliable THEN 1 ELSE 0 END) AS facilities_occupancy_unreliable
      FROM f
      GROUP BY state, year_month
    ),
    b AS (
      SELECT state_or_nation AS state,
             reported_total_nurse_staffing_hours_per_resident_per_day AS cms_hprd
      FROM gdrive_silver.nh_state_us_averages
      WHERE processing_date = (SELECT MAX(processing_date) FROM gdrive_silver.nh_state_us_averages)
    )
    SELECT
      a.state,
      a.year_month,
      a.year, a.quarter, a.month_num, a.month_name,
      CAST(a.facilities_reporting AS INT) AS facilities_reporting,
      CAST(a.resident_days AS INT) AS resident_days,
      a.total_nurse_hrs, a.rn_hrs, a.lpn_hrs, a.cna_hrs, a.other_aide_hrs,
      a.contract_hrs, a.employee_hrs,
      CAST(a.hprd_resident_days AS INT) AS hprd_resident_days,
      a.hprd_numerator_hrs,
      CAST(CASE WHEN a.hprd_resident_days > 0 THEN a.hprd_numerator_hrs / a.hprd_resident_days END AS DECIMAL(10,4)) AS hprd_total,
      CAST(CASE WHEN a.hprd_resident_days > 0 THEN a.num_rn / a.hprd_resident_days END AS DECIMAL(10,4)) AS hprd_rn,
      CAST(CASE WHEN a.hprd_resident_days > 0 THEN a.num_lpn / a.hprd_resident_days END AS DECIMAL(10,4)) AS hprd_lpn,
      CAST(CASE WHEN a.hprd_resident_days > 0 THEN a.num_cna / a.hprd_resident_days END AS DECIMAL(10,4)) AS hprd_cna,
      CAST(CASE WHEN a.hprd_resident_days > 0 THEN a.num_aide / a.hprd_resident_days END AS DECIMAL(10,4)) AS hprd_other_aide,
      CAST(a.hprd_facility_avg AS DECIMAL(10,4)) AS hprd_facility_avg,
      CAST(CASE WHEN a.total_nurse_hrs > 0 THEN 100.0 * a.contract_hrs / a.total_nurse_hrs END AS DECIMAL(10,4)) AS contract_share_pct,
      CAST(CASE WHEN a.occ_bd > 0 THEN 100.0 * a.occ_rd / a.occ_bd END AS DECIMAL(10,4)) AS occupancy_rate,
      CAST(a.facilities_occupancy_unreliable AS INT) AS facilities_occupancy_unreliable,
      CAST(b.cms_hprd AS DECIMAL(10,4)) AS cms_reported_hprd_state,
      CAST(a.hprd_facility_avg - b.cms_hprd AS DECIMAL(10,4)) AS hprd_vs_cms_diff,
      CURRENT_TIMESTAMP() AS built_at
    FROM a
    LEFT JOIN b ON a.state = b.state
"""

STATE_OWNERSHIP_SQL = """
    WITH fq AS (
      SELECT
        ccn, state, ownership_type, year, quarter,
        MAX(certified_beds) AS certified_beds,
        SUM(resident_days) AS resident_days,
        SUM(days_reported - zero_census_days) AS census_days,
        SUM(total_nurse_hrs) AS total_nurse_hrs,
        SUM(rn_hrs) AS rn_hrs,
        SUM(lpn_hrs) AS lpn_hrs,
        SUM(cna_hrs) AS cna_hrs,
        SUM(other_aide_hrs) AS other_aide_hrs,
        SUM(contract_hrs) AS contract_hrs,
        SUM(employee_hrs) AS employee_hrs,
        SUM(hprd_resident_days) AS hprd_resident_days,
        SUM(hprd_numerator_hrs) AS hprd_numerator_hrs,
        SUM(hprd_numerator_rn_hrs) AS num_rn,
        SUM(hprd_numerator_lpn_hrs) AS num_lpn,
        SUM(hprd_numerator_cna_hrs) AS num_cna,
        SUM(hprd_numerator_other_aide_hrs) AS num_aide
      FROM gdrive_gold.gold_staffing_facility_monthly
      GROUP BY ccn, state, ownership_type, year, quarter
    ),
    fq2 AS (
      SELECT fq.*,
             CASE WHEN hprd_resident_days > 0 THEN hprd_numerator_hrs / hprd_resident_days END AS hprd_fq,
             CASE WHEN certified_beds > 0 AND census_days > 0
                  THEN 100.0 * resident_days / (certified_beds * census_days) END AS occ_fq
      FROM fq
    )
    SELECT
      state,
      ownership_type,
      CONCAT(CAST(year AS STRING), 'Q', CAST(quarter AS STRING)) AS cy_qtr,
      CAST(COUNT(*) AS INT) AS facilities_reporting,
      CAST(SUM(resident_days) AS INT) AS resident_days,
      SUM(total_nurse_hrs) AS total_nurse_hrs,
      SUM(rn_hrs) AS rn_hrs,
      SUM(lpn_hrs) AS lpn_hrs,
      SUM(cna_hrs) AS cna_hrs,
      SUM(other_aide_hrs) AS other_aide_hrs,
      SUM(contract_hrs) AS contract_hrs,
      SUM(employee_hrs) AS employee_hrs,
      CAST(SUM(hprd_resident_days) AS INT) AS hprd_resident_days,
      SUM(hprd_numerator_hrs) AS hprd_numerator_hrs,
      CAST(CASE WHEN SUM(hprd_resident_days) > 0 THEN SUM(hprd_numerator_hrs) / SUM(hprd_resident_days) END AS DECIMAL(10,4)) AS hprd_total,
      CAST(CASE WHEN SUM(hprd_resident_days) > 0 THEN SUM(num_rn) / SUM(hprd_resident_days) END AS DECIMAL(10,4)) AS hprd_rn,
      CAST(CASE WHEN SUM(hprd_resident_days) > 0 THEN SUM(num_lpn) / SUM(hprd_resident_days) END AS DECIMAL(10,4)) AS hprd_lpn,
      CAST(CASE WHEN SUM(hprd_resident_days) > 0 THEN SUM(num_cna) / SUM(hprd_resident_days) END AS DECIMAL(10,4)) AS hprd_cna,
      CAST(CASE WHEN SUM(hprd_resident_days) > 0 THEN SUM(num_aide) / SUM(hprd_resident_days) END AS DECIMAL(10,4)) AS hprd_other_aide,
      CAST(AVG(hprd_fq) AS DECIMAL(10,4)) AS hprd_facility_avg,
      CAST(CASE WHEN SUM(total_nurse_hrs) > 0 THEN 100.0 * SUM(contract_hrs) / SUM(total_nurse_hrs) END AS DECIMAL(10,4)) AS contract_share_pct,
      CAST(CASE WHEN SUM(CASE WHEN occ_fq <= 150 THEN certified_beds * census_days END) > 0
                THEN 100.0 * SUM(CASE WHEN occ_fq <= 150 THEN resident_days END)
                     / SUM(CASE WHEN occ_fq <= 150 THEN certified_beds * census_days END) END AS DECIMAL(10,4)) AS occupancy_rate,
      CAST(SUM(CASE WHEN occ_fq > 150 THEN 1 ELSE 0 END) AS INT) AS facilities_occupancy_unreliable,
      CURRENT_TIMESTAMP() AS built_at
    FROM fq2
    GROUP BY state, ownership_type, year, quarter
"""

QUALITY_SQL = """
    WITH q AS (
      SELECT
        ccn, year, quarter,
        MAX(provider_name) AS provider_name,
        MAX(city) AS city,
        MAX(state) AS state,
        MAX(county_name) AS county_name,
        MAX(ownership_type) AS ownership_type,
        MAX(provider_type) AS provider_type,
        MAX(certified_beds) AS certified_beds,
        MAX(latitude) AS latitude,
        MAX(longitude) AS longitude,
        MAX(overall_rating) AS overall_rating,
        MAX(staffing_rating) AS staffing_rating,
        (MAX(CASE WHEN has_provider_info THEN 1 ELSE 0 END) = 1) AS has_provider_info,
        SUM(days_reported) AS days_reported,
        SUM(days_reported - zero_census_days) AS census_days,
        SUM(resident_days) AS resident_days,
        SUM(total_nurse_hrs) AS total_nurse_hrs,
        SUM(contract_hrs) AS contract_hrs,
        SUM(hprd_resident_days) AS hprd_resident_days,
        SUM(hprd_numerator_hrs) AS num_total,
        SUM(hprd_numerator_rn_hrs) AS num_rn,
        SUM(hprd_numerator_lpn_hrs) AS num_lpn,
        SUM(hprd_numerator_cna_hrs) AS num_cna,
        SUM(hprd_numerator_other_aide_hrs) AS num_aide
      FROM gdrive_gold.gold_staffing_facility_monthly
      GROUP BY ccn, year, quarter
    ),
    info AS (
      SELECT ccn, health_inspection_rating, qm_rating,
             staffing_rating_footnote, reported_staffing_footnote,
             total_nursing_staff_turnover, registered_nurse_turnover,
             number_of_fines, total_amount_of_fines_in_dollars,
             reported_total_nurse_staffing_hours_per_resident_per_day
      FROM gdrive_silver.nh_provider_info
      WHERE processing_date = (SELECT MAX(processing_date) FROM gdrive_silver.nh_provider_info)
    ),
    claims AS (
      SELECT ccn,
        MAX(CASE WHEN measure_code = '521' THEN adjusted_score END) AS rehosp_adj,
        MAX(CASE WHEN measure_code = '521' THEN observed_score END) AS rehosp_obs,
        MAX(CASE WHEN measure_code = '521' THEN expected_score END) AS rehosp_exp,
        MAX(CASE WHEN measure_code = '522' THEN adjusted_score END) AS ed_adj,
        MAX(CASE WHEN measure_code = '551' THEN adjusted_score END) AS hosp_adj,
        MAX(CASE WHEN measure_code = '552' THEN adjusted_score END) AS ed_long_adj,
        MAX(CASE WHEN measure_code = '521' THEN measure_period END) AS readmission_period
      FROM gdrive_silver.nh_quality_msr_claims
      WHERE processing_date = (SELECT MAX(processing_date) FROM gdrive_silver.nh_quality_msr_claims)
      GROUP BY ccn
    ),
    vbp AS (
      SELECT ccn,
             MAX(baseline_period_fy_2019_risk_standardized_readmission_rate) AS vbp_baseline,
             MAX(performance_period_fy_2022_risk_standardized_readmission_rate) AS vbp_perf_rate,
             MAX(performance_score) AS vbp_score
      FROM gdrive_silver.snf_vbp_facility_performance
      GROUP BY ccn
    ),
    c AS (
      SELECT
        q.*,
        CASE WHEN q.census_days > 0 THEN q.resident_days / q.census_days END AS avg_census,
        CASE WHEN q.hprd_resident_days > 0 THEN q.num_total / q.hprd_resident_days END AS hprd_t,
        CASE WHEN q.hprd_resident_days > 0 THEN q.num_rn / q.hprd_resident_days END AS hprd_r,
        CASE WHEN q.hprd_resident_days > 0 THEN q.num_lpn / q.hprd_resident_days END AS hprd_l,
        CASE WHEN q.hprd_resident_days > 0 THEN q.num_cna / q.hprd_resident_days END AS hprd_c,
        CASE WHEN q.hprd_resident_days > 0 THEN q.num_aide / q.hprd_resident_days END AS hprd_a,
        CASE WHEN q.certified_beds > 0 AND q.census_days > 0
             THEN 100.0 * q.resident_days / (q.certified_beds * q.census_days) END AS occ,
            (q.census_days >= 45 AND q.hprd_resident_days > 0
                AND q.num_total < 1.0 * q.hprd_resident_days) AS low_hprd,
            (q.census_days >= 45 AND q.hprd_resident_days > 0
                AND q.num_total >= 1.0 * q.hprd_resident_days) AS eligible
      FROM q
    ),
    r AS (
      SELECT c.*,
        RANK() OVER (PARTITION BY year, quarter, eligible ORDER BY hprd_t) AS rk_nat,
        RANK() OVER (PARTITION BY year, quarter, state, eligible ORDER BY hprd_t) AS rk_state,
        PERCENT_RANK() OVER (PARTITION BY year, quarter, eligible ORDER BY hprd_t) AS pct_rank
      FROM c
    )
    SELECT
      r.ccn,
      CONCAT(CAST(r.year AS STRING), 'Q', CAST(r.quarter AS STRING)) AS cy_qtr,
      r.provider_name, r.city, r.state, r.county_name, r.ownership_type, r.provider_type,
      r.certified_beds, r.latitude, r.longitude,
      r.overall_rating,
      i.health_inspection_rating,
      i.qm_rating,
      r.staffing_rating,
      CAST(i.total_nursing_staff_turnover AS DECIMAL(10,4)) AS total_nursing_turnover_pct,
      CAST(i.registered_nurse_turnover AS DECIMAL(10,4)) AS rn_turnover_pct,
      CAST(i.number_of_fines AS INT) AS num_fines,
      CAST(i.total_amount_of_fines_in_dollars AS DECIMAL(14,2)) AS total_fines_usd,
      r.has_provider_info,
      r.low_hprd AS hprd_implausibly_low,
      i.staffing_rating_footnote,
      i.reported_staffing_footnote,
      CAST(r.days_reported AS INT) AS days_reported,
      CAST(r.census_days AS INT) AS census_days,
      CAST(r.avg_census AS DECIMAL(10,2)) AS avg_daily_census,
      CAST(r.occ AS DECIMAL(10,4)) AS occupancy_rate,
      CAST(r.hprd_t AS DECIMAL(10,4)) AS hprd_total,
      CAST(r.hprd_r AS DECIMAL(10,4)) AS hprd_rn,
      CAST(r.hprd_l AS DECIMAL(10,4)) AS hprd_lpn,
      CAST(r.hprd_c AS DECIMAL(10,4)) AS hprd_cna,
      CAST(r.hprd_a AS DECIMAL(10,4)) AS hprd_other_aide,
      CAST(CASE WHEN r.total_nurse_hrs > 0 THEN 100.0 * r.contract_hrs / r.total_nurse_hrs END AS DECIMAL(10,4)) AS contract_share_pct,
      CAST(i.reported_total_nurse_staffing_hours_per_resident_per_day AS DECIMAL(10,4)) AS cms_reported_total_hprd,
      CAST(cl.rehosp_adj AS DECIMAL(10,4)) AS rehosp_short_stay_adj,
      CAST(cl.rehosp_obs AS DECIMAL(10,4)) AS rehosp_short_stay_obs,
      CAST(cl.rehosp_exp AS DECIMAL(10,4)) AS rehosp_short_stay_exp,
      CAST(cl.ed_adj AS DECIMAL(10,4)) AS ed_short_stay_adj,
      CAST(cl.hosp_adj AS DECIMAL(10,4)) AS hosp_long_stay_per1000_adj,
      CAST(cl.ed_long_adj AS DECIMAL(10,4)) AS ed_long_stay_per1000_adj,
      CAST(v.vbp_perf_rate AS DECIMAL(10,6)) AS vbp_readmission_rate_fy2022,
      CAST(v.vbp_baseline AS DECIMAL(10,6)) AS vbp_baseline_rate_fy2019,
      CAST(v.vbp_score AS DECIMAL(10,4)) AS vbp_performance_score,
      cl.readmission_period AS readmission_period,
      CAST(CASE WHEN r.eligible THEN r.rk_nat END AS INT) AS hprd_total_rank_national,
      CAST(CASE WHEN r.eligible THEN r.rk_state END AS INT) AS hprd_total_rank_in_state,
      CAST(CASE WHEN r.eligible THEN 100.0 * r.pct_rank END AS DECIMAL(10,2)) AS hprd_total_percentile,
      CURRENT_TIMESTAMP() AS built_at
    FROM r
    LEFT JOIN info i ON r.ccn = i.ccn
    LEFT JOIN claims cl ON r.ccn = cl.ccn
    LEFT JOIN vbp v ON r.ccn = v.ccn
"""

RELATIONSHIPS = [
    ("hprd_total", "occupancy_rate", "occupancy_rate <= 150",
     "M3: staffing vs occupancy (occupancy over 150 percent excluded)"),
    ("hprd_total", "rehosp_short_stay_adj", "TRUE",
     "M5: staffing vs short-stay rehospitalization"),
    ("hprd_rn", "rehosp_short_stay_adj", "TRUE",
     "M5: RN staffing vs short-stay rehospitalization"),
    ("hprd_total", "ed_short_stay_adj", "TRUE",
     "Staffing vs short-stay ED visits"),
    ("hprd_total", "hosp_long_stay_per1000_adj", "TRUE",
     "Staffing vs long-stay hospitalizations per 1000 days"),
    ("contract_share_pct", "rehosp_short_stay_adj", "TRUE",
     "Contract labor share vs short-stay rehospitalization"),
]


def relationship_sql():
    selects = []
    for x, y, cond, note in RELATIONSHIPS:
        base = (f"SELECT cy_qtr, CAST({x} AS DOUBLE) AS x, CAST({y} AS DOUBLE) AS y "
                f"FROM gdrive_gold.gold_facility_quality_staffing "
                f"WHERE {x} IS NOT NULL AND {y} IS NOT NULL "
                f"AND census_days >= 45 AND NOT hprd_implausibly_low AND {cond}")
        selects.append(f"""
            SELECT CONCAT('{x}__vs__{y}__pearson__', cy_qtr) AS relationship_id,
                   '{x}' AS x_metric, '{y}' AS y_metric, 'pearson' AS method, cy_qtr,
                   CAST(COUNT(*) AS INT) AS n_facilities,
                   CAST(CORR(x, y) AS DECIMAL(10,4)) AS coefficient,
                   CAST(POW(CORR(x, y), 2) AS DECIMAL(10,4)) AS r_squared,
                   CAST(COVAR_SAMP(x, y) / VAR_SAMP(x) AS DECIMAL(18,6)) AS slope,
                   '{note}' AS note,
                   CURRENT_TIMESTAMP() AS built_at
            FROM ({base}) b
            GROUP BY cy_qtr
        """)
        
        selects.append(f"""
            SELECT CONCAT('{x}__vs__{y}__spearman__', cy_qtr) AS relationship_id,
                   '{x}' AS x_metric, '{y}' AS y_metric, 'spearman' AS method, cy_qtr,
                   CAST(COUNT(*) AS INT) AS n_facilities,
                   CAST(CORR(rx, ry) AS DECIMAL(10,4)) AS coefficient,
                   CAST(POW(CORR(rx, ry), 2) AS DECIMAL(10,4)) AS r_squared,
                   CAST(NULL AS DECIMAL(18,6)) AS slope,
                   '{note}' AS note,
                   CURRENT_TIMESTAMP() AS built_at
            FROM (SELECT cy_qtr,
                         CAST(RANK() OVER (PARTITION BY cy_qtr ORDER BY x) AS DOUBLE) AS rx,
                         CAST(RANK() OVER (PARTITION BY cy_qtr ORDER BY y) AS DOUBLE) AS ry
                  FROM ({base}) b) r
            GROUP BY cy_qtr
        """)
        
    return "\nUNION ALL\n".join(selects)


TABLES = {
    "gold_staffing_daily": {
        "sql": DAILY_SQL,
        "partition": ["year_month"],
        "checks": [
            ("row_count_matches_silver",
             "SELECT ABS((SELECT COUNT(*) FROM candidate) - "
             "(SELECT COUNT(*) FROM gdrive_silver.pbj_daily_nurse_staffing))", 0, "block"),
            ("duplicate_keys",
             "SELECT COUNT(*) - COUNT(DISTINCT ccn, work_date) FROM candidate", 0, "block"),
            ("negative_hours",
             "SELECT COUNT(*) FROM candidate WHERE total_nurse_hrs < 0 "
             "OR contract_hrs < 0 OR employee_hrs < 0", 0, "block"),
            ("hprd_null_only_when_zero_census",
             "SELECT COUNT(*) FROM candidate WHERE hprd_total IS NULL AND NOT is_zero_census", 0, "block"),
            ("facilities_missing_provider_info_share",
             "SELECT AVG(CASE WHEN has_provider_info THEN 0.0 ELSE 1.0 END) FROM candidate", 0.01, "block"),
            ("hprd_over_24_hours",
             "SELECT COUNT(*) FROM candidate WHERE hprd_total > 24", 0, "warn"),
            ("occupancy_over_150_pct",
             "SELECT COUNT(*) FROM candidate WHERE occupancy_rate > 150", 0, "warn"),
        ],
    },
    "gold_staffing_facility_monthly": {
        "sql": MONTHLY_SQL,
        "partition": [],
        "checks": [
            ("row_count_matches_daily",
             "SELECT ABS((SELECT COUNT(*) FROM candidate) - "
             "(SELECT COUNT(*) FROM (SELECT DISTINCT ccn, year_month "
             "FROM gdrive_gold.gold_staffing_daily) x))", 0, "block"),
            ("duplicate_keys",
             "SELECT COUNT(*) - COUNT(DISTINCT ccn, year_month) FROM candidate", 0, "block"),
            ("resident_days_reconcile",
             "SELECT ABS((SELECT SUM(resident_days) FROM candidate) - "
             "(SELECT SUM(mds_census) FROM gdrive_gold.gold_staffing_daily))", 0, "block"),
            ("hours_reconcile",
             "SELECT ABS((SELECT SUM(total_nurse_hrs) FROM candidate) - "
             "(SELECT SUM(total_nurse_hrs) FROM gdrive_gold.gold_staffing_daily))", 0.01, "block"),
            ("days_in_month_exceeded",
             "SELECT COUNT(*) FROM candidate WHERE days_reported > 31", 0, "block"),
            ("groups_sum_to_total",
             "SELECT COUNT(*) FROM candidate WHERE "
             "ABS(rn_hrs + lpn_hrs + cna_hrs + other_aide_hrs - total_nurse_hrs) > 0.01", 0, "block"),
            ("contract_share_out_of_range",
             "SELECT COUNT(*) FROM candidate WHERE contract_share_pct < 0 "
             "OR contract_share_pct > 100", 0, "block"),
            ("hprd_over_24_hours",
             "SELECT COUNT(*) FROM candidate WHERE hprd_total > 24", 0, "warn"),
            ("occupancy_over_150_pct",
             "SELECT COUNT(*) FROM candidate WHERE occupancy_rate > 150", 0, "warn"),
        ],
    },
    "gold_staffing_state_monthly": {
        "sql": STATE_MONTHLY_SQL,
        "partition": [],
        "checks": [
            ("row_count_matches_monthly_groups",
             "SELECT ABS((SELECT COUNT(*) FROM candidate) - (SELECT COUNT(*) FROM "
             "(SELECT DISTINCT state, year_month FROM gdrive_gold.gold_staffing_facility_monthly) x))",
             0, "block"),
            ("duplicate_keys",
             "SELECT COUNT(*) - COUNT(DISTINCT state, year_month) FROM candidate", 0, "block"),
            ("facility_months_reconcile",
             "SELECT ABS((SELECT SUM(facilities_reporting) FROM candidate) - "
             "(SELECT COUNT(*) FROM gdrive_gold.gold_staffing_facility_monthly))", 0, "block"),
            ("resident_days_reconcile",
             "SELECT ABS((SELECT SUM(resident_days) FROM candidate) - "
             "(SELECT SUM(resident_days) FROM gdrive_gold.gold_staffing_facility_monthly))", 0, "block"),
            ("hours_reconcile",
             "SELECT ABS((SELECT SUM(total_nurse_hrs) FROM candidate) - "
             "(SELECT SUM(total_nurse_hrs) FROM gdrive_gold.gold_staffing_facility_monthly))",
             0.01, "block"),
            ("groups_sum_to_total",
             "SELECT COUNT(*) FROM candidate WHERE "
             "ABS(rn_hrs + lpn_hrs + cna_hrs + other_aide_hrs - total_nurse_hrs) > 0.01", 0, "block"),
            ("contract_share_out_of_range",
             "SELECT COUNT(*) FROM candidate WHERE contract_share_pct < 0 "
             "OR contract_share_pct > 100", 0, "block"),
            ("states_without_cms_benchmark",
             "SELECT COUNT(*) FROM candidate WHERE cms_reported_hprd_state IS NULL", 0, "warn"),
            ("hprd_far_from_cms",
             "SELECT COUNT(*) FROM candidate WHERE ABS(hprd_vs_cms_diff) > 1", 0, "warn"),
            ("occupancy_over_100_pct",
             "SELECT COUNT(*) FROM candidate WHERE occupancy_rate > 100", 0, "warn"),
        ],
    },
    "gold_staffing_state_ownership": {
        "sql": STATE_OWNERSHIP_SQL,
        "partition": [],
        "checks": [
            ("duplicate_keys",
             "SELECT COUNT(*) - COUNT(DISTINCT state, ownership_type, cy_qtr) FROM candidate",
             0, "block"),
            ("facilities_reconcile",
             "SELECT ABS((SELECT SUM(facilities_reporting) FROM candidate) - "
             "(SELECT COUNT(*) FROM (SELECT DISTINCT ccn, year, quarter "
             "FROM gdrive_gold.gold_staffing_facility_monthly) x))", 0, "block"),
            ("resident_days_reconcile",
             "SELECT ABS((SELECT SUM(resident_days) FROM candidate) - "
             "(SELECT SUM(resident_days) FROM gdrive_gold.gold_staffing_state_monthly))", 0, "block"),
            ("hours_reconcile",
             "SELECT ABS((SELECT SUM(total_nurse_hrs) FROM candidate) - "
             "(SELECT SUM(total_nurse_hrs) FROM gdrive_gold.gold_staffing_state_monthly))",
             0.01, "block"),
            ("groups_sum_to_total",
             "SELECT COUNT(*) FROM candidate WHERE "
             "ABS(rn_hrs + lpn_hrs + cna_hrs + other_aide_hrs - total_nurse_hrs) > 0.01", 0, "block"),
            ("hprd_over_24_hours",
             "SELECT COUNT(*) FROM candidate WHERE hprd_total > 24", 0, "warn"),
        ],
    },
        "gold_facility_quality_staffing": {
        "sql": QUALITY_SQL,
        "partition": [],
        "checks": [
            ("row_count_matches_facility_quarters",
             "SELECT ABS((SELECT COUNT(*) FROM candidate) - (SELECT COUNT(*) FROM "
             "(SELECT DISTINCT ccn, year, quarter FROM gdrive_gold.gold_staffing_facility_monthly) x))",
             0, "block"),
            ("duplicate_keys",
             "SELECT COUNT(*) - COUNT(DISTINCT ccn, cy_qtr) FROM candidate", 0, "block"),
            ("rank_without_hprd",
             "SELECT COUNT(*) FROM candidate WHERE hprd_total_rank_national IS NOT NULL "
             "AND hprd_total IS NULL", 0, "block"),
            ("percentile_out_of_range",
             "SELECT COUNT(*) FROM candidate WHERE hprd_total_percentile < 0 "
             "OR hprd_total_percentile > 100", 0, "block"),
            ("missing_claims_share",
             "SELECT AVG(CASE WHEN has_provider_info AND rehosp_short_stay_adj IS NULL "
             "THEN 1.0 ELSE 0.0 END) FROM candidate", 0.5, "warn"),
            ("hprd_over_24_hours",
             "SELECT COUNT(*) FROM candidate WHERE hprd_total > 24", 0, "warn"),
            ("low_hprd_not_ranked",
             "SELECT COUNT(*) FROM candidate WHERE hprd_implausibly_low "
             "AND hprd_total_rank_national IS NOT NULL", 0, "block"),
            ("implausibly_low_share",
             "SELECT AVG(CASE WHEN hprd_implausibly_low THEN 1.0 ELSE 0.0 END) "
             "FROM candidate", 0.01, "warn"),
        ],
    },
    "gold_relationship_stats": {
        "sql": relationship_sql(),
        "partition": [],
        "checks": [
            ("no_rows",
             "SELECT CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END FROM candidate", 0, "block"),
            ("coefficient_out_of_range",
             "SELECT COUNT(*) FROM candidate WHERE ABS(coefficient) > 1", 0, "block"),
            ("small_sample",
             "SELECT COUNT(*) FROM candidate WHERE n_facilities < 100", 0, "warn"),
        ],
    },
}


# ---------------------------------------------------------------
# 3. Helper functions
# ---------------------------------------------------------------
def delete_prefix(bucket, prefix):
    boto3.resource("s3").Bucket(bucket).objects.filter(Prefix=prefix).delete()


def drop_catalog_table(database, table):
    """Gold tables are rebuilt in full, so drop the catalog entry as well.
    This guarantees the table's columns match the new build exactly."""
    glue = boto3.client("glue")
    try:
        glue.delete_table(DatabaseName=database, Name=table)
    except glue.exceptions.EntityNotFoundException:
        pass


def write_catalog(df, bucket, table, database, partition_keys):
    sink = glueContext.getSink(
        connection_type="s3", path=f"s3://{bucket}/{table}/",
        enableUpdateCatalog=True, updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=partition_keys)
    sink.setFormat("glueparquet")
    sink.setCatalogInfo(catalogDatabase=database, catalogTableName=table)
    sink.writeFrame(DynamicFrame.fromDF(df, glueContext, f"{table}_out"))


def run_checks(name, checks):
    problems = []
    for check_name, sql, max_allowed, severity in checks:
        value = spark.sql(sql).collect()[0][0]
        value = 0.0 if value is None else float(value)
        ok = value <= max_allowed
        print(f"CHECK table={name} check={check_name} value={value} "
              f"max={max_allowed} severity={severity} {'PASS' if ok else 'FAIL'}")
        if not ok and severity == "block":
            problems.append(f"{check_name} (value {value}, max {max_allowed})")
    if problems:
        raise Exception(f"Blocking checks failed for {name}: " + "; ".join(problems))


# ---------------------------------------------------------------
# 4. Build each requested table
# ---------------------------------------------------------------
requested = [t.strip() for t in args["TABLES"].split(",") if t.strip()]
if requested == ["ALL"]:
    requested = list(TABLES)

failures = []
for name in requested:
    try:
        spec = TABLES[name]
        df = spark.sql(spec["sql"]).cache()
        df.createOrReplaceTempView("candidate")
        n_rows = df.count()
        run_checks(name, spec["checks"])
        delete_prefix(args["GOLD_BUCKET"], f"{name}/")
        drop_catalog_table(args["GOLD_DB"], name)
        write_catalog(df, args["GOLD_BUCKET"], name, args["GOLD_DB"], spec["partition"])
        print(f"SUMMARY table={name} rows={n_rows} columns={len(df.columns)}")
    except Exception as e:
        failures.append(f"{name}: {e}")
        print(f"FAILED {name}: {e}")
    finally:
        spark.catalog.clearCache()

if failures:
    raise Exception("One or more tables failed:\n" + "\n".join(failures))

job.commit()