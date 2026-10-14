import re
import sys
import csv
import boto3
from datetime import datetime, timezone
from functools import reduce

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# ---------------------------------------------------------------
# 1. Job setup
# ---------------------------------------------------------------
args = getResolvedOptions(sys.argv, [
    "JOB_NAME", "DATASET", "BRONZE_BUCKET", "SILVER_BUCKET",
    "QUARANTINE_BUCKET", "SILVER_DB", "QUARANTINE_DB",
])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Unparseable dates become NULL instead of raising errors
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
spark.conf.set("spark.sql.ansi.enabled", "false")

DECIMAL_TYPES = {"decimal2": T.DecimalType(12, 2), "decimal": T.DecimalType(20, 10)}


def any_of(conditions):
    return reduce(lambda a, b: a | b, conditions)


# ---------------------------------------------------------------
# 2. Dataset configuration
# ---------------------------------------------------------------
# --- PBJ (explicit columns, strict casting) ---
ROLES = [("RNDON", "rndon"), ("RNadmin", "rnadmin"), ("RN", "rn"),
         ("LPNadmin", "lpnadmin"), ("LPN", "lpn"), ("CNA", "cna"),
         ("NAtrn", "natrn"), ("MedAide", "medaide")]

PBJ_COLUMNS = [
    ("PROVNUM", "ccn", "string"),
    ("PROVNAME", "provider_name", "string"),
    ("CITY", "city", "string"),
    ("STATE", "state", "string"),
    ("COUNTY_NAME", "county_name", "string"),
    ("COUNTY_FIPS", "county_fips", "string"),
    ("CY_Qtr", "cy_qtr", "string"),
    ("WorkDate", "work_date", "date:yyyyMMdd"),
    ("MDScensus", "mds_census", "int"),
]
for src, name in ROLES:
    for suffix in ["", "_emp", "_ctr"]:
        PBJ_COLUMNS.append((f"Hrs_{src}{suffix}", f"hrs_{name}{suffix}", "decimal2"))


def pbj_rules(df):
    hours = [c for _, c, t in PBJ_COLUMNS if t == "decimal2"]
    totals = [f"hrs_{n}" for _, n in ROLES]
    hard = [
        ("bad_ccn_format",
         F.col("ccn").isNotNull() & ~F.col("ccn").rlike("^[0-9A-Z]{6}$")),
        ("negative_census", F.col("mds_census") < 0),
        ("negative_hours", any_of([F.col(c) < 0 for c in hours])),
    ]
    soft = [
        ("emp_ctr_mismatch", any_of([
            F.abs(F.coalesce(F.col(t), F.lit(0))
                  - F.coalesce(F.col(t + "_emp"), F.lit(0))
                  - F.coalesce(F.col(t + "_ctr"), F.lit(0))) > 0.01
            for t in totals])),
        ("zero_census", F.col("mds_census") == 0),
        ("date_outside_quarter",
         F.col("work_date").isNotNull() & (
             F.concat(F.year("work_date").cast("string"), F.lit("Q"),
                      F.quarter("work_date").cast("string")) != F.col("cy_qtr"))),
    ]
    return hard, soft


# --- ProviderInfo (auto columns, soft casting) ---
PROVIDER_INFO_AUTO = {
    "rename": {"cms_certification_number_ccn": "ccn"},
    "must_have": ["ccn", "provider_name", "state", "ownership_type",
                  "number_of_certified_beds", "processing_date"],
    "date_format": "yyyy-MM-dd",
    "int_patterns": [
        r"^(overall|health_inspection|qm|long_stay_qm|short_stay_qm|staffing)_rating$",
        r"^number_of_",
        r"^rating_cycle_\d_(total_number|number)_of_",
        r"^rating_cycle_\d_(health_deficiency|health_revisit|total_health)_score$",
        r"^total_number_of_penalties$",
    ],
    "date_patterns": [r"_date$", r"^date_"],
    "decimal_patterns": [
        r"^average_number_of_residents_per_day$",
        r"staffing_hours_per_resident_per_day",
        r"hours_per_resident_per_day_on_the_weekend",
        r"_turnover$",
        r"^nursing_case_mix_index",
        r"^total_weighted_health_survey_score$",
        r"^total_amount_of_fines_in_dollars$",
        r"^(latitude|longitude)$",
    ],
}


def provider_info_rules(df):
    ratings = ["overall_rating", "health_inspection_rating", "qm_rating",
               "long_stay_qm_rating", "short_stay_qm_rating", "staffing_rating"]
    lat, lon = F.col("latitude"), F.col("longitude")
    hard = [
        ("bad_ccn_format",
         F.col("ccn").isNotNull() & ~F.col("ccn").rlike("^[0-9A-Z]{6}$")),
        ("negative_beds", F.col("number_of_certified_beds") < 0),
    ]
    soft = [
        ("rating_out_of_range", any_of([
            F.col(c).isNotNull() & ((F.col(c) < 1) | (F.col(c) > 5)) for c in ratings])),
        ("bad_coordinates",
         (lat.isNotNull() & ((lat < -90) | (lat > 90)))
         | (lon.isNotNull() & ((lon < -180) | (lon > 180)))),
        ("negative_residents", F.col("average_number_of_residents_per_day") < 0),
        ("zero_beds", F.col("number_of_certified_beds") == 0),
    ]
    return hard, soft


# --- Quality measures from claims (auto columns, soft casting) ---
CLAIMS_AUTO = {
    "rename": {"cms_certification_number_ccn": "ccn"},
    "must_have": ["ccn", "measure_code", "adjusted_score", "observed_score",
                  "expected_score", "measure_period", "processing_date"],
    "date_format": "yyyy-MM-dd",
    "date_patterns": [r"_date$"],
    "decimal_patterns": [r"^(adjusted|observed|expected)_score$"],
}


def claims_derived():
    parts = F.split(F.col("measure_period"), "-")
    return [
        ("measure_period_start", F.to_date(parts.getItem(0), "yyyyMMdd")),
        ("measure_period_end", F.to_date(parts.getItem(1), "yyyyMMdd")),
    ]


def claims_rules(df):
    hard = [("bad_ccn_format",
             F.col("ccn").isNotNull() & ~F.col("ccn").rlike("^[0-9A-Z]{6}$"))]
    soft = [
        ("negative_adjusted_score", F.col("adjusted_score") < 0),
        ("bad_measure_period",
         F.col("measure_period").isNotNull() & F.col("measure_period_start").isNull()),
    ]
    return hard, soft


# --- VBP facility performance (auto columns, soft casting) ---
VBP_AUTO = {
    "rename": {"cms_certification_number_ccn": "ccn"},
    "must_have": ["ccn", "snf_vbp_program_ranking", "performance_score",
                  "incentive_payment_multiplier"],
    "date_format": "yyyy-MM-dd",
    "int_patterns": [r"^snf_vbp_program_ranking$"],
    "decimal_patterns": [r"_rate$", r"_score$", r"_multiplier$"],
}


def vbp_rules(df):
    hard = [("bad_ccn_format",
             F.col("ccn").isNotNull() & ~F.col("ccn").rlike("^[0-9A-Z]{6}$"))]
    soft = [
        ("bad_ranking", F.col("snf_vbp_program_ranking") < 1),
        ("performance_score_out_of_range",
         (F.col("performance_score") < 0) | (F.col("performance_score") > 100)),
    ]
    return hard, soft


CONFIG = {
    "pbj_daily_nurse_staffing": {
        "bronze_prefix": "pbj_daily_nurse_staffing",
        "columns": PBJ_COLUMNS,
        "key": ["ccn", "work_date"],
        "partition": "cy_qtr",
        "required": ["ccn", "work_date", "cy_qtr", "mds_census"],
        "null_values": ["", "-", "--", "---"],
        "cast_mode": "hard",
        "rules": pbj_rules,
        "max_reject_rate": 0.05,
    },
    "nh_provider_info": {
        "bronze_prefix": "nh_providerinfo",
        "auto": PROVIDER_INFO_AUTO,
        "key": ["ccn"],
        "partition": "processing_date",
        "required": ["ccn", "provider_name", "state", "processing_date"],
        "null_values": ["", "-"],
        "cast_mode": "soft",
        "rules": provider_info_rules,
        "max_reject_rate": 0.05,
    },
    "nh_quality_msr_claims": {
        "bronze_prefix": "nh_qualitymsr_claims",
        "auto": CLAIMS_AUTO,
        "key": ["ccn", "measure_code"],
        "partition": "processing_date",
        "required": ["ccn", "measure_code", "processing_date"],
        "null_values": ["", "-"],
        "cast_mode": "soft",
        "derived": claims_derived,
        "rules": claims_rules,
        "max_reject_rate": 0.05,
    },
    "snf_vbp_facility_performance": {
        "bronze_prefix": "fy_2024_snf_vbp_facility_performance",
        "auto": VBP_AUTO,
        "key": ["ccn"],
        "partition": "release",
        "literal_columns": {"release": "fy2024"},
        "required": ["ccn"],
        "null_values": ["", "-"],
        "cast_mode": "soft",
        "rules": vbp_rules,
        "max_reject_rate": 0.05,
    },
}

# ---------------------------------------------------------------
# Part F1: event / survey tables
# ---------------------------------------------------------------
NULLS = ["", "-", "--", "---"]

EVENT_AUTO_BASE = {
    "rename": {"cms_certification_number_ccn": "ccn"},
    "date_format": "yyyy-MM-dd",
    "date_patterns": [r"_date$"],
}


def ccn_rules(df):
    hard = [("bad_ccn_format",
             F.col("ccn").isNotNull() & ~F.col("ccn").rlike("^[0-9A-Z]{6}$"))]
    return hard, []


def penalties_rules(df):
    hard, _ = ccn_rules(df)
    soft = [
        ("negative_fine", F.col("fine_amount") < 0),
        ("negative_denial_days", F.col("payment_denial_length_in_days") < 0),
    ]
    return hard, soft


def citation_rules(df):
    hard, _ = ccn_rules(df)
    soft = [
        ("bad_scope_severity",
         F.col("scope_severity_code").isNotNull()
         & ~F.col("scope_severity_code").rlike("^[A-L]$")),
        ("correction_before_survey",
         F.col("correction_date").isNotNull() & F.col("survey_date").isNotNull()
         & (F.col("correction_date") < F.col("survey_date"))),
    ]
    return hard, soft


def survey_summary_rules(df):
    hard, _ = ccn_rules(df)
    soft = [("negative_deficiency_count",
             (F.col("total_number_of_health_deficiencies") < 0)
             | (F.col("total_number_of_fire_safety_deficiencies") < 0))]
    return hard, soft


def ownership_derived():
    pct = F.regexp_extract(F.col("ownership_percentage"), r"(\d+(\.\d+)?)", 1)
    since = F.regexp_extract(F.col("association_date"), r"(\d{2}/\d{2}/\d{4})", 1)
    return [
        ("ownership_percent", F.when(pct != "", pct.cast(T.DecimalType(5, 2)))),
        ("association_date_value", F.when(since != "", F.to_date(since, "MM/dd/yyyy"))),
    ]


def event_config(prefix, auto_extra, rules, derived=None):
    cfg = {
        "bronze_prefix": prefix,
        "auto": {**EVENT_AUTO_BASE, **auto_extra},
        "key": [],
        "dedupe": "all_columns",
        "partition": "processing_date",
        "required": ["ccn", "processing_date"],
        "null_values": NULLS,
        "cast_mode": "soft",
        "rules": rules,
        "max_reject_rate": 0.05,
    }
    if derived:
        cfg["derived"] = derived
    return cfg


CITATION_MUST_HAVE = ["ccn", "survey_date", "survey_type", "deficiency_tag_number",
                      "scope_severity_code", "correction_date", "inspection_cycle",
                      "processing_date"]

CONFIG.update({
    "nh_penalties": event_config(
        "nh_penalties",
        {"must_have": ["ccn", "penalty_date", "penalty_type", "fine_amount",
                       "payment_denial_start_date", "payment_denial_length_in_days",
                       "processing_date"],
         "int_patterns": [r"^payment_denial_length_in_days$"],
         "decimal_patterns": [r"^fine_amount$"]},
        penalties_rules),
    "nh_health_citations": event_config(
        "nh_healthcitations",
        {"must_have": CITATION_MUST_HAVE,
         "int_patterns": [r"^inspection_cycle$"]},
        citation_rules),
    "nh_fire_safety_citations": event_config(
        "nh_firesafetycitations",
        {"must_have": CITATION_MUST_HAVE + ["tag_version"],
         "int_patterns": [r"^inspection_cycle$"]},
        citation_rules),
    "nh_survey_summary": event_config(
        "nh_surveysummary",
        {"must_have": ["ccn", "inspection_cycle", "health_survey_date",
                       "fire_safety_survey_date", "total_number_of_health_deficiencies",
                       "total_number_of_fire_safety_deficiencies", "processing_date"],
         "int_patterns": [r"^inspection_cycle$", r"^total_number_of_", r"^count_of_"]},
        survey_summary_rules),
    "nh_survey_dates": event_config(
        "nh_surveydates",
        {"must_have": ["ccn", "survey_date", "type_of_survey", "survey_cycle",
                       "processing_date"],
         "int_patterns": [r"^survey_cycle$"]},
        ccn_rules),
    "nh_ownership": event_config(
        "nh_ownership",
        {"must_have": ["ccn", "owner_name", "owner_type", "ownership_percentage",
                       "association_date", "processing_date"],
         "string_cols": ["association_date"]},
        ccn_rules,
        derived=ownership_derived),
})

# ---------------------------------------------------------------
# Part F2: measure tables and reference tables
# ---------------------------------------------------------------
def no_rules(df):
    return [], []


def std_config(prefix, auto_extra, rules, partition="processing_date",
               literal=None, derived=None, required=None, date_format="yyyy-MM-dd"):
    cfg = {
        "bronze_prefix": prefix,
        "auto": {**EVENT_AUTO_BASE, "date_format": date_format, **auto_extra},
        "key": [],
        "dedupe": "all_columns",
        "partition": partition,
        "required": required if required is not None else [partition],
        "null_values": NULLS,
        "cast_mode": "soft",
        "rules": rules,
        "max_reject_rate": 0.05,
    }
    if literal:
        cfg["literal_columns"] = literal
    if derived:
        cfg["derived"] = derived
    return cfg


def mds_derived():
    parts = F.split(F.col("measure_period"), "-")
    return [("measure_period_start", parts.getItem(0)),
            ("measure_period_end", parts.getItem(1))]


def score_derived():
    # Score is text for some measures ("No Different than the National Rate");
    # score_numeric holds the number where the score is numeric, NULL otherwise
    return [("score_numeric", F.col("score").cast(T.DecimalType(20, 10)))]


def vbp_aggregate_derived():
    d = T.DecimalType(20, 10)
    perf = F.split(F.col("range_of_performance_scores"), "-")
    mult = F.split(F.col("range_of_incentive_payment_multipliers"), "-")
    return [("performance_score_min", perf.getItem(0).cast(d)),
            ("performance_score_max", perf.getItem(1).cast(d)),
            ("multiplier_min", mult.getItem(0).cast(d)),
            ("multiplier_max", mult.getItem(1).cast(d))]


def cutpoints_derived():
    out = []
    for c in ["c_5_stars", "c_4_stars", "c_3_stars", "c_2_stars", "c_1_star"]:
        lo = F.regexp_extract(F.col(c), r"^>?(\d+(\.\d+)?)", 1)
        hi = F.regexp_extract(F.col(c), r"-(\d+(\.\d+)?)$", 1)
        out.append((f"{c}_min", F.when(lo != "", lo.cast(T.DecimalType(10, 3)))))
        out.append((f"{c}_max", F.when(hi != "", hi.cast(T.DecimalType(10, 3)))))
    return out


QRP_MUST_HAVE = ["ccn", "measure_code", "score", "start_date", "end_date"]

CONFIG.update({
    "nh_quality_msr_mds": std_config(
        "nh_qualitymsr_mds",
        {"must_have": ["ccn", "measure_code", "measure_description", "resident_type",
                       "q1_measure_score", "four_quarter_average_score",
                       "measure_period", "processing_date"],
         "decimal_patterns": [r"^q\d_measure_score$", r"^four_quarter_average_score$"]},
        ccn_rules, required=["ccn", "measure_code", "processing_date"],
        derived=mds_derived),
    "snf_qrp_provider_data": std_config(
        "skilled_nursing_facility_quality_reporting_program_provider_data",
        {"must_have": QRP_MUST_HAVE + ["provider_name"]},
        ccn_rules, partition="release", literal={"release": "oct2024"},
        required=["ccn", "measure_code"], derived=score_derived,
        date_format="MM/dd/yyyy"),
    "snf_qrp_national_data": std_config(
        "skilled_nursing_facility_quality_reporting_program_national_data",
        {"must_have": QRP_MUST_HAVE},
        no_rules, partition="release", literal={"release": "oct2024"},
        required=["measure_code"], derived=score_derived,
        date_format="MM/dd/yyyy"),
    "swing_bed_snf_data": std_config(
        "swing_bed_snf_data",
        {"must_have": QRP_MUST_HAVE + ["provider_name"]},
        ccn_rules, partition="release", literal={"release": "oct2024"},
        required=["ccn", "measure_code"], derived=score_derived,
        date_format="MM/dd/yyyy"),
    "snf_vbp_aggregate_performance": std_config(
        "fy_2024_snf_vbp_aggregate_performance",
        {"must_have": [
            "baseline_period_fy_2019_national_average_readmission_rate",
            "performance_period_fy_2022_national_average_readmission_rate",
            "fy_2024_achievement_threshold", "fy_2024_benchmark",
            "range_of_performance_scores",
            "total_number_of_snfs_receiving_value_based_incentive_payments",
            "range_of_incentive_payment_multipliers",
            "range_of_value_based_incentive_payments",
            "total_amount_of_value_based_incentive_payments"],
         "int_patterns": [r"^total_number_of_snfs_"],
         "decimal_patterns": [r"_national_average_readmission_rate$",
                              r"^fy_2024_(achievement_threshold|benchmark)$"]},
        no_rules, partition="release", literal={"release": "fy2024"},
        required=[], derived=vbp_aggregate_derived),
    "nh_citation_descriptions": std_config(
        "nh_citationdescriptions",
        {"must_have": ["deficiency_prefix", "deficiency_tag_number",
                       "deficiency_prefix_and_number", "deficiency_description",
                       "deficiency_category"]},
        no_rules, partition="release", literal={"release": "oct2024"}, required=[]),
    "nh_data_collection_intervals": std_config(
        "nh_datacollectionintervals",
        {"must_have": ["measure_code", "measure_description",
                       "data_collection_period_from_date",
                       "data_collection_period_through_date", "processing_date"],
         "date_formats": {"processing_date": "yyyyMMdd"}},
        no_rules, required=["measure_code", "processing_date"],
        date_format="MM/dd/yyyy"),
    "nh_hlth_inspec_cutpoints_state": std_config(
        "nh_hlthinspeccutpointsstate",
        {"must_have": ["state", "c_5_stars", "c_4_stars", "c_3_stars",
                       "c_2_stars", "c_1_star"]},
        no_rules, partition="release", literal={"release": "oct2024"},
        required=["state"], derived=cutpoints_derived),
    "nh_state_us_averages": std_config(
        "nh_stateusaverages",
        {"must_have": ["state_or_nation", "average_number_of_residents_per_day",
                       "reported_total_nurse_staffing_hours_per_resident_per_day",
                       "processing_date"],
         "string_cols": ["state_or_nation"],
         "decimal_patterns": [r"."]},
        no_rules, required=["state_or_nation", "processing_date"]),
    "nh_covid_vax_averages": std_config(
        "nh_covidvaxaverages",
        {"must_have": ["state",
                       "percent_of_residents_who_are_up_to_date_on_their_vaccines",
                       "percent_of_staff_who_are_up_to_date_on_their_vaccines",
                       "date_vaccination_data_last_updated"],
         "date_patterns": [r"^date_"],
         "decimal_patterns": [r"^percent_"]},
        no_rules, partition="date_vaccination_data_last_updated",
        required=["state", "date_vaccination_data_last_updated"],
        date_format="MM/dd/yyyy"),
    "nh_covid_vax_provider": std_config(
        "nh_covidvaxprovider",
        {"must_have": ["ccn", "state",
                       "percent_of_residents_who_are_up_to_date_on_their_vaccines",
                       "percent_of_staff_who_are_up_to_date_on_their_vaccines",
                       "date_vaccination_data_last_updated"],
         "date_patterns": [r"^date_"],
         "decimal_patterns": [r"^percent_"]},
        ccn_rules, partition="date_vaccination_data_last_updated",
        required=["ccn", "date_vaccination_data_last_updated"],
        date_format="MM/dd/yyyy"),
})

def check_field_counts(paths, expected):
    """Stop if any line has a different number of fields than the header."""
    lines = spark.read.text(paths).filter(F.length(F.trim(F.col("value"))) > 0)
    unquoted = F.regexp_replace(F.col("value"), '"[^"]*"', "")
    n_fields = F.length(unquoted) - F.length(F.regexp_replace(unquoted, ",", "")) + 1
    bad = lines.withColumn("n_fields", n_fields).filter(F.col("n_fields") != expected)
    n_bad = bad.count()
    if n_bad > 0:
        examples = [r["value"][:200] for r in bad.limit(5).collect()]
        raise ValueError(
            f"{n_bad} line(s) do not have {expected} fields. Examples: {examples}")

# ---------------------------------------------------------------
# 3. Helper functions
# ---------------------------------------------------------------
def to_snake(name):
    s = re.sub(r"[^a-z0-9]+", "_", name.lower().replace("&", " and ")).strip("_")
    return f"c_{s}" if s[:1].isdigit() else s


def infer_type(name, auto):
    """Type from the column NAME using documented patterns. Unknown = string."""
    if name in auto.get("string_cols", []) or "footnote" in name:
        return "string"
    if name in auto.get("date_formats", {}):
        return "date:" + auto["date_formats"][name]
    for pat in auto.get("int_patterns", []):
        if re.search(pat, name):
            return "int"
    for pat in auto.get("date_patterns", []):
        if re.search(pat, name):
            return "date:" + auto["date_format"]
    for pat in auto.get("decimal_patterns", []):
        if re.search(pat, name):
            return "decimal"
    return "string"


def build_auto_columns(header, auto):
    columns, seen = [], {}
    for src in header:
        name = auto.get("rename", {}).get(to_snake(src), to_snake(src))
        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 1
        columns.append((src, name, infer_type(name, auto)))
    names = [c[1] for c in columns]
    missing = [m for m in auto["must_have"] if m not in names]
    if missing:
        raise ValueError(f"Expected columns missing from the file header: {missing}")
    return columns


def read_header(path):
    line = spark.read.text(path).limit(1).collect()[0][0]
    return [c.strip() for c in next(csv.reader([line.lstrip("\ufeff")]))]


def latest_bronze_keys(bucket, prefix):
    """Newest file in each Bronze folder whose name starts with prefix."""
    s3c = boto3.client("s3")
    latest = {}
    paginator = s3c.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=f"source=gdrive/file={prefix}"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            folder = key.split("/")[1]
            if key.endswith(".csv") and (folder not in latest or key > latest[folder]):
                latest[folder] = key
    return sorted(latest.values())


def clean_str(name, null_values):
    trimmed = F.trim(F.col(name))
    return F.when(trimmed.isin(*null_values), F.lit(None)).otherwise(trimmed)


def delete_prefix(bucket, prefix):
    boto3.resource("s3").Bucket(bucket).objects.filter(Prefix=prefix).delete()


def write_catalog(df, bucket, table, database, partition_keys):
    sink = glueContext.getSink(
        connection_type="s3", path=f"s3://{bucket}/{table}/",
        enableUpdateCatalog=True, updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=partition_keys)
    sink.setFormat("glueparquet")
    sink.setCatalogInfo(catalogDatabase=database, catalogTableName=table)
    sink.writeFrame(DynamicFrame.fromDF(df, glueContext, f"{table}_out"))


# ---------------------------------------------------------------
# 4. Process one dataset
# ---------------------------------------------------------------
def process_dataset(dataset):
    spec = CONFIG[dataset]
    now = datetime.now(timezone.utc)
    run_date = now.strftime("%Y-%m-%d")
    batch_id = now.strftime("%Y%m%dT%H%M%SZ")
    null_values = spec["null_values"]
    strict = spec.get("cast_mode", "hard") == "hard"

    # --- find files and settle the column list ---
    keys = latest_bronze_keys(args["BRONZE_BUCKET"], spec["bronze_prefix"])
    if not keys:
        raise ValueError(f"No Bronze files found for {dataset}")
    paths = [f"s3://{args['BRONZE_BUCKET']}/{k}" for k in keys]
    headers = [read_header(p) for p in paths]

    if "columns" in spec:
        columns = spec["columns"]
        expected = [s for s, _, _ in columns]
        for p, h in zip(paths, headers):
            if h != expected:
                raise ValueError(
                    f"Schema drift in {p}. Missing: {sorted(set(expected) - set(h))}. "
                    f"Unexpected: {sorted(set(h) - set(expected))}.")
    else:
        if any(h != headers[0] for h in headers):
            raise ValueError(f"Files for {dataset} have different headers")
        columns = build_auto_columns(headers[0], spec["auto"])

    col_names = [n for _, n, _ in columns]
    print(f"{dataset}: {len(paths)} file(s), {len(col_names)} columns")
    check_field_counts(paths, len(col_names))

    # --- read everything as text ---
    raw_schema = T.StructType(
        [T.StructField(n, T.StringType(), True) for n in col_names]
        + [T.StructField("_corrupt_record", T.StringType(), True)])
    raw = (spark.read.schema(raw_schema)
           .option("header", "true").option("quote", '"').option("escape", '"')
           .option("mode", "PERMISSIVE")
           .option("columnNameOfCorruptRecord", "_corrupt_record")
           .csv(paths)
           .withColumn("_source_file", F.input_file_name()))

    # --- clean and cast ---
    typed_cols, hard_cast, soft_cast = [], [], []
    for _, name, typ in columns:
        base = clean_str(name, null_values)
        bad = None
        if typ == "string":
            typed = base
        elif typ == "int":
            typed = base.cast("int")
            bad = base.isNotNull() & (typed.isNull()
                                      | (typed.cast("double") != base.cast("double")))
        elif typ in DECIMAL_TYPES:
            typed = base.cast(DECIMAL_TYPES[typ])
        elif typ.startswith("date:"):
            typed = F.to_date(base, typ.split(":", 1)[1])
        else:
            raise ValueError(f"Unknown type {typ} for {name}")
        if bad is None and typ != "string":
            bad = base.isNotNull() & typed.isNull()
        typed_cols.append(typed.alias(name))
        if bad is not None:
            err = F.when(bad, F.lit(f"invalid_{name}"))
            if strict or name in spec["required"] or name in spec["key"]:
                hard_cast.append(err)
            else:
                soft_cast.append(err)

    literal_cols = spec.get("literal_columns", {})
    soft_cast_expr = F.concat_ws(",", *soft_cast)
    typed_df = raw.select(
        *typed_cols,
        *[F.lit(v).alias(k) for k, v in literal_cols.items()],
        F.concat_ws(";", *hard_cast).alias("_cast_errors"),
        F.when(F.length(soft_cast_expr) > 0, soft_cast_expr).alias("_soft_cast"),
        F.to_json(F.struct(*[F.col(n) for n in col_names])).alias("_raw_json"),
        F.col("_corrupt_record"),
        F.col("_source_file"))

    derived = spec["derived"]() if "derived" in spec else []
    for name, expr in derived:
        typed_df = typed_df.withColumn(name, expr)
    out_names = col_names + list(literal_cols) + [n for n, _ in derived]

    # --- apply rules ---
    hard_rules, soft_rules = spec["rules"](typed_df)
    reasons = [
        F.when(F.col("_corrupt_record").isNotNull(), F.lit("malformed_row")),
        F.when(F.col("_cast_errors") != "", F.col("_cast_errors")),
    ]
    reasons += [F.when(F.col(c).isNull(), F.lit(f"missing_{c}")) for c in spec["required"]]
    reasons += [F.when(cond, F.lit(n)) for n, cond in hard_rules]
    flags = F.concat_ws(",", F.col("_soft_cast"),
                        *[F.when(cond, F.lit(n)) for n, cond in soft_rules])

    typed_df = (typed_df
                .withColumn("_hard_reasons", F.concat_ws(";", *reasons))
                .withColumn("dq_flags", F.when(F.length(flags) > 0, flags))
                .cache())

    good = typed_df.filter(F.col("_hard_reasons") == "")
    bad_rows = typed_df.filter(F.col("_hard_reasons") != "")

    # Duplicates: keep the first row per key (within a release), quarantine the rest
    dedupe_cols = out_names if spec.get("dedupe") == "all_columns" else spec["key"]
    w = (Window.partitionBy(*(dedupe_cols + [spec["partition"]]))
         .orderBy(F.monotonically_increasing_id()))
    good = good.withColumn("_rn", F.row_number().over(w))
    dups = good.filter("_rn > 1").withColumn("_hard_reasons", F.lit("duplicate_key"))
    good = good.filter("_rn = 1").drop("_rn")

    def to_quarantine(df):
        return df.select(
            F.col("_source_file").alias("source_file"),
            F.col("_hard_reasons").alias("reasons"),
            F.coalesce(F.col("_corrupt_record"), F.col("_raw_json")).alias("raw_record"),
            F.current_timestamp().alias("quarantined_at"),
            F.lit(dataset).alias("dataset"),
            F.lit(run_date).alias("run_date"))

    quarantine = to_quarantine(bad_rows).unionByName(to_quarantine(dups)).cache()
    good = good.cache()

    total_rows = typed_df.count()
    n_good = good.count()
    n_quarantine = quarantine.count()
    if total_rows == 0:
        raise ValueError("Bronze file has no data rows")
    reject_rate = n_quarantine / total_rows

    # --- quarantine first, then the circuit breaker ---
    delete_prefix(args["QUARANTINE_BUCKET"],
                  f"quarantine_rows/dataset={dataset}/run_date={run_date}/")
    if n_quarantine > 0:
        write_catalog(quarantine, args["QUARANTINE_BUCKET"], "quarantine_rows",
                      args["QUARANTINE_DB"], ["dataset", "run_date"])

    if reject_rate > spec["max_reject_rate"]:
        raise Exception(
            f"Reject rate {reject_rate:.2%} exceeds {spec['max_reject_rate']:.0%}. "
            f"Silver NOT written. Review gdrive_quarantine.quarantine_rows.")

    # --- Silver: replace only the partitions in this batch ---
    part_col = spec["partition"]
    partitions = [str(r[0]) for r in good.select(part_col).distinct().collect()]
    for p in partitions:
        delete_prefix(args["SILVER_BUCKET"], f"{dataset}/{part_col}={p}/")

    silver = good.select(
        *out_names, "dq_flags", F.col("_source_file"),
        F.lit(batch_id).alias("_batch_id"),
        F.current_timestamp().alias("_processed_at"))
    write_catalog(silver, args["SILVER_BUCKET"], dataset, args["SILVER_DB"], [part_col])

    n_flagged = good.filter(F.col("dq_flags").isNotNull()).count()
    print(f"SUMMARY dataset={dataset} rows_read={total_rows} silver_rows={n_good} "
          f"quarantined={n_quarantine} flagged={n_flagged} "
          f"reject_rate={reject_rate:.4%} partitions={partitions}")


# ---------------------------------------------------------------
# 5. Run every requested dataset; fail at the end if any failed
# ---------------------------------------------------------------
requested = [d.strip() for d in args["DATASET"].split(",") if d.strip()]
if requested == ["ALL"]:
    requested = list(CONFIG)

failures = []
for ds in requested:
    try:
        process_dataset(ds)
    except Exception as e:
        failures.append(f"{ds}: {e}")
        print(f"FAILED {ds}: {e}")
    finally:
        spark.catalog.clearCache()

if failures:
    raise Exception("One or more datasets failed:\n" + "\n".join(failures))

job.commit()