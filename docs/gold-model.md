# Gold Data Model

![Gold model](images/gold-model.png)

## Design

The Gold layer is built for fast read, so the tables are **denormalized**: wide and pre-joined, with facility, state, ownership and date attributes copied into every table. No analysis query needs a join, and there is no separate date table.

## The six tables

| Table                          | Grain (one row per)               | Key                           | Rows      | Columns |
| ------------------------------ | --------------------------------- | ----------------------------- | --------- | ------- |
| gold_staffing_daily            | Facility and day                  | ccn, work_date                | 1,325,324 | 46      |
| gold_staffing_facility_monthly | Facility and month                | ccn, year_month               | 43,692    | 46      |
| gold_staffing_state_monthly    | State and month                   | state, year_month             | 156       | 29      |
| gold_staffing_state_ownership  | State, ownership type and quarter | state, ownership_type, cy_qtr | 486       | 24      |
| gold_facility_quality_staffing | Facility and quarter              | ccn, cy_qtr                   | 14,564    | 48      |
| gold_relationship_stats        | Statistic                         | relationship_id               | 12        | 11      |

## How the tables build on each other

```
Silver: pbj_daily_nurse_staffing + nh_provider_info
        |
        v
gold_staffing_daily  (1,325,324 rows)
        |
        v
gold_staffing_facility_monthly  (14,564 facilities x 3 months)
        |
        +--> gold_staffing_state_monthly
        +--> gold_staffing_state_ownership
        +--> gold_facility_quality_staffing  (also joins nh_provider_info and nh_quality_msr_claims)
                    |
                    v
             gold_relationship_stats
```

The state tables are built from the monthly table's group numerators, so state figures are exact weighted ratios, not averages of facility ratios.

## What each table is for

| Table                          | Use                                                                                    |
| ------------------------------ | -------------------------------------------------------------------------------------- |
| gold_staffing_daily            | Day-level staffing and occupancy; outlier diagnostics; the source of every other table |
| gold_staffing_facility_monthly | Facility staffing trends, contract-labor hours and share                               |
| gold_staffing_state_monthly    | Staffing by state and month, compared with the CMS state benchmark                     |
| gold_staffing_state_ownership  | Staffing and contract share by state and ownership type                                |
| gold_facility_quality_staffing | Facility staffing beside quality outcomes, ratings and national ranks                  |
| gold_relationship_stats        | Pre-computed correlations between staffing and occupancy or quality outcomes           |

## Key columns used in this project

Column names as they appear in the queries in `sql/`. For the complete column lists, see the section at the end.

| Column                                                                                         | Meaning                                                                                | Tables                         |
| ---------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- | ------------------------------ |
| ccn                                                                                            | CMS Certification Number (6-character string)                                          | Facility tables                |
| hprd_total, hprd_rn                                                                            | Nursing hours per resident per day, all nurse groups and RN only                       | Daily, monthly, quality        |
| hprd*numerator*\* (four columns)                                                               | Exact hours for the RN, LPN, CNA and other-aide groups, kept so state tables are exact | Monthly                        |
| hprd_resident_days, census_days, avg_daily_census                                              | Denominators and census                                                                | Monthly, quality               |
| is_hprd_outlier, hprd_outlier_days                                                             | Implausible-day marker and count                                                       | Daily, monthly                 |
| occupancy_rate, occupancy_unreliable                                                           | Occupancy and its reliability flag                                                     | Daily, monthly, state, quality |
| contract_hrs, total_nurse_hrs, contract_share_pct                                              | Contract-labor hours and share                                                         | Monthly, quality               |
| ownership_type, has_provider_info                                                              | Ownership and whether provider info matched                                            | Facility tables                |
| hprd_total_rank_national, hprd_implausibly_low                                                 | National rank; low-staffing flag                                                       | Quality                        |
| cms_reported_total_hprd, staffing_rating, staffing_rating_footnote, reported_staffing_footnote | CMS's own published staffing figure, rating and footnote codes                         | Quality                        |
| rehosp_short_stay_adj, ed_short_stay_adj, hosp_long_stay_per1000_adj                           | Claims-based quality outcomes                                                          | Quality                        |

## Relationship statistics

Six pairs, each with Pearson and Spearman (12 rows), computed after excluding the 30 flagged facilities. Columns include `x_metric`, `y_metric`, `method`, `n_facilities`, `coefficient`, `r_squared` and `slope`. <FILL: the remaining columns, from SHOW COLUMNS>

| x                  | y                          | Pearson r | Spearman r | n      |
| ------------------ | -------------------------- | --------- | ---------- | ------ |
| hprd_total         | occupancy_rate             | -0.0542   | +0.0232    | 14,507 |
| hprd_total         | rehosp_short_stay_adj      | -0.0605   | -0.0472    | 11,799 |
| hprd_rn            | rehosp_short_stay_adj      | -0.0832   | -0.0999    | 11,799 |
| hprd_total         | ed_short_stay_adj          | -0.0696   | -0.0591    | 11,799 |
| hprd_total         | hosp_long_stay_per1000_adj | -0.0476   | -0.0325    | 11,477 |
| contract_share_pct | rehosp_short_stay_adj      | -0.0124   | -0.0104    | 11,799 |

All correlations are weak (none above 0.10 in size). The staffing-occupancy relationship is U-shaped, which is why a straight-line correlation is near zero.

## Full column lists

`SHOW COLUMNS IN gdrive_gold.gold_facility_quality_staffing`
ccn  
cy_qtr  
provider_name  
city  
state  
county_name  
ownership_type  
provider_type  
certified_beds  
latitude  
longitude  
overall_rating  
health_inspection_rating
qm_rating  
staffing_rating  
total_nursing_turnover_pct
rn_turnover_pct  
num_fines  
total_fines_usd  
has_provider_info  
hprd_implausibly_low
staffing_rating_footnote
reported_staffing_footnote
days_reported  
census_days  
avg_daily_census  
occupancy_rate  
hprd_total  
hprd_rn  
hprd_lpn  
hprd_cna  
hprd_other_aide  
contract_share_pct  
cms_reported_total_hprd
rehosp_short_stay_adj
rehosp_short_stay_obs
rehosp_short_stay_exp
ed_short_stay_adj  
hosp_long_stay_per1000_adj
ed_long_stay_per1000_adj
vbp_readmission_rate_fy2022
vbp_baseline_rate_fy2019
vbp_performance_score
readmission_period  
hprd_total_rank_national
hprd_total_rank_in_state
hprd_total_percentile
built_at

`SHOW COLUMNS IN gdrive_gold.gold_relationship_stats`
relationship_id  
x_metric  
y_metric  
method  
cy_qtr  
n_facilities  
coefficient  
r_squared  
slope  
note  
built_at

`SHOW COLUMNS IN gdrive_gold.gold_staffing_daily`
ccn  
work_date  
year  
quarter  
cy_qtr  
month_num  
month_name  
day_of_week  
day_name  
is_weekend  
provider_name  
city  
state  
county_name  
county_fips  
ownership_type  
provider_type  
certified_beds  
in_hospital_flag  
overall_rating  
staffing_rating  
has_provider_info  
mds_census  
rn_hrs  
lpn_hrs  
cna_hrs  
other_aide_hrs  
total_nurse_hrs  
rn_hrs_ctr  
lpn_hrs_ctr  
cna_hrs_ctr  
other_aide_hrs_ctr  
contract_hrs  
employee_hrs  
hprd_total  
hprd_rn  
hprd_lpn  
hprd_cna  
hprd_other_aide  
occupancy_rate  
contract_share_pct  
is_zero_census  
is_hprd_outlier  
dq_flags  
built_at  
year_month

`SHOW COLUMNS IN gdrive_gold.gold_staffing_facility_monthly`
ccn  
year_month  
month_start_date  
year  
quarter  
month_num  
month_name  
provider_name  
city  
state  
county_name  
ownership_type  
provider_type  
certified_beds  
overall_rating  
staffing_rating  
latitude  
longitude  
has_provider_info  
days_reported  
zero_census_days  
hprd_outlier_days  
resident_days  
avg_daily_census  
occupancy_rate  
occupancy_unreliable
total_nurse_hrs  
rn_hrs  
lpn_hrs  
cna_hrs  
other_aide_hrs  
contract_hrs  
employee_hrs  
hprd_resident_days  
hprd_numerator_hrs  
hprd_numerator_rn_hrs
hprd_numerator_lpn_hrs
hprd_numerator_cna_hrs
hprd_numerator_other_aide_hrs
hprd_total  
hprd_rn  
hprd_lpn  
hprd_cna  
hprd_other_aide  
contract_share_pct  
built_at

`SHOW COLUMNS IN gdrive_gold.gold_staffing_state_monthly`
state  
year_month  
year  
quarter  
month_num  
month_name  
facilities_reporting
resident_days  
total_nurse_hrs  
rn_hrs  
lpn_hrs  
cna_hrs  
other_aide_hrs  
contract_hrs  
employee_hrs  
hprd_resident_days  
hprd_numerator_hrs  
hprd_total  
hprd_rn  
hprd_lpn  
hprd_cna  
hprd_other_aide  
hprd_facility_avg  
contract_share_pct  
occupancy_rate  
facilities_occupancy_unreliable
cms_reported_hprd_state
hprd_vs_cms_diff  
built_at

`SHOW COLUMNS IN gdrive_gold.gold_staffing_state_ownership`
state  
ownership_type  
cy_qtr  
facilities_reporting
resident_days  
total_nurse_hrs  
rn_hrs  
lpn_hrs  
cna_hrs  
other_aide_hrs  
contract_hrs  
employee_hrs  
hprd_resident_days  
hprd_numerator_hrs  
hprd_total  
hprd_rn  
hprd_lpn  
hprd_cna  
hprd_other_aide  
hprd_facility_avg  
contract_share_pct  
occupancy_rate  
facilities_occupancy_unreliable
built_at
