# Data Quality Rules

Every rule in the pipeline, why it exists, and what it did to the data. Figures are from the October 2024 release and the Q2 2024 staffing file.

## Silver rules

| Rule                                                | Treatment                                                        | Why                                                            |
| --------------------------------------------------- | ---------------------------------------------------------------- | -------------------------------------------------------------- |
| Read every column as text, then cast explicitly     | Types are applied after reading                                  | Leading zeros in the CMS Certification Number (CCN) survive    |
| Placeholder values (blank, `-`, `--`, `---`)        | Become NULL                                                      | These mean "no value" in the CMS files                         |
| Value cannot be cast to its type                    | Becomes NULL plus a flag; the job does not crash (ANSI mode off) | A bad value should be counted, not stop a 1.3 million-row load |
| Hard failure (a required key is missing or invalid) | Row goes to the quarantine table with its reasons and raw record | Keeps the evidence for inspection                              |
| Soft problem (a suspicious but possible value)      | Row is kept and named in the `dq_flags` column                   | The analyst decides whether to exclude it                      |
| Reject rate above 5% for a dataset                  | Circuit breaker: Silver is not written and the job fails         | A file this broken needs a person                              |
| Duplicates                                          | Removed on all columns for event tables                          | No business key is guessed where none exists                   |
| Writing                                             | Delete-then-write per release partition                          | Re-runs never double rows                                      |
| Wide files (many columns)                           | Names converted to snake_case; types inferred by name pattern    | Avoids hand-writing hundreds of column definitions             |

Each Silver dataset is partitioned by release date (a processing date, or a fixed label such as `oct2024`).

### Quarantine table

`gdrive_quarantine.quarantine_rows`, always present and empty when nothing is rejected. Columns: `source_file`, `reasons`, `raw_record`, `quarantined_at`, `dataset`, `run_date`.

### Silver flags

| Flag                                   | Rows                                                        | Table                                |
| -------------------------------------- | ----------------------------------------------------------- | ------------------------------------ |
| correction_before_survey               | 2,008                                                       | Health citations                     |
| Non-numeric percentage fields          | 1,092 (911 both columns, 130 residents only, 51 staff only) | Covid vaccination by provider        |
| Invalid baseline or improvement values | 517                                                         | SNF value-based purchasing, facility |
| zero_census                            | 320                                                         | Daily nurse staffing                 |
| Flagged fire-safety rows               | 20                                                          | Fire safety citations                |
| **Total**                              | **3,957**                                                   |                                      |

Result: 21 tables, 3,421,978 rows, 0 quarantined.

### Silver tests

- **Deliberate quarantine test:** a test file whose IDs had lost their leading zeros had 7 of 7 rows quarantined, and the job failed on the circuit breaker, as designed.
- **Known gap:** a CSV row with too few fields is only caught if a required column ends up empty. A field-count check would close this.

### Silver reconciliations

| Check                                     | Expected                                                           | Result      |
| ----------------------------------------- | ------------------------------------------------------------------ | ----------- |
| Daily staffing rows                       | 14,564 facilities x 91 days = 1,325,324                            | Exact       |
| Claims measures                           | 14,814 facilities x 4 measures = 59,256                            | Exact       |
| VBP facility rows vs the aggregate file   | 10,858                                                             | Exact       |
| Facility 015009                           | 1 fine of $23,989; 4 health citations in cycle 1; 6 fire citations | All matched |
| Total hours for one facility-day, by hand | 262.45                                                             | Matched     |

Facility coverage: 14,547 facilities appear in both staffing and provider info; 17 have staffing but no provider-info match (probably de-certified by October 2024); 267 have provider info but no staffing (254 approved before April 2024, 13 after).

## Gold rules

### Measures

**Hours per resident per day (HPRD)** = total nurse hours divided by total resident census, each summed over the days counted. This is a ratio of sums, not an average of daily ratios, so busy days and large facilities count in proportion.

Nurse hours come in four groups: RN (director of nursing, RN administrative and RN), LPN (LPN administrative and LPN), CNA, and other aide (trainee and medication aide).

**Occupancy** = sum of census divided by (certified beds x census days). It is NULL on zero-census days.

### Exclusions and flags

| Rule                                                                   | Treatment                                                                                   | Effect                                                         |
| ---------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- | -------------------------------------------------------------- |
| Zero-census day                                                        | Excluded from HPRD and occupancy; hours still count in totals                               | 320 facility-days                                              |
| Implausible day (census of 10 or more and hours above 24 per resident) | Excluded from HPRD only (`is_hprd_outlier`)                                                 | 1 day (facility 145446, May); May HPRD fell from 8.19 to 2.75  |
| Occupancy above 150%                                                   | Flagged `occupancy_unreliable`; left out of state occupancy                                 | 32 facility-months at 19 facilities with mismatched bed counts |
| Quarter HPRD below 1.0 with 45 or more census days                     | Flagged `hprd_implausibly_low`; left out of rankings and correlations; kept in state totals | 30 facilities, 0.188% of resident-days                         |
| 17 facilities with staffing but no provider-info match                 | Kept in all staffing tables; ownership type `Unknown`; bed-based fields empty               | `has_provider_info` = false                                    |

### Gold checks

Checks run before any table is written. **Blocking** checks stop the build; **warn** checks report.

| Table         | Check                                      | Severity        | Observed     |
| ------------- | ------------------------------------------ | --------------- | ------------ |
| Quality       | Row count matches facility-quarters        | Block           | 0 difference |
| Quality       | Duplicate keys                             | Block           | 0            |
| Quality       | Rank without HPRD                          | Block           | 0            |
| Quality       | Percentile out of range                    | Block           | 0            |
| Quality       | Flagged facility appears in a ranking      | Block           | 0            |
| Quality       | Share of facilities missing a claims score | Warn (max 0.5)  | 0.187        |
| Quality       | HPRD over 24 hours                         | Warn            | 0            |
| Quality       | Share flagged implausibly low              | Warn (max 0.01) | 0.00206      |
| Relationships | No rows; coefficient out of range          | Block           | Pass         |
| Relationships | Small sample                               | Warn            | Pass         |

Warn-level findings on the daily table before the rules were applied: 75 days with HPRD above 24 (17 facilities, mostly census of 1 to 7) and 995 days with occupancy above 150% (19 facilities, 8 of them on all 91 days). Both were investigated and led to the rules above.

### Validation against CMS

For facility 015009 (Burns Nursing Home), the computed `hprd_total` is **4.2936** and the CMS-published value is **4.2936**.

## Idempotency and re-runs

- Silver: delete-then-write per partition.
- Gold: each table is dropped from the Glue Data Catalog before it is rewritten. This fixed a stale-schema problem where the catalog kept an old column count.
- After a forced end-to-end run, all 27 Silver and Gold row counts matched exactly. `sql/01_row_counts_all_tables.sql` repeats that check.
