import duckdb
from pathlib import Path

# --- Config ---
S3_PREFIX = "s3://aodn-cloud-optimised/slocum_glider_delayed_qc.parquet"
REGION = "ap-southeast-2"

DEPLOYMENT_CODE = "BassStrait20170321"   # <-- change this
DEPTH_MIN, DEPTH_MAX = 19, 20

# --- Connect ---
con = duckdb.connect()

# S3 support
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")
con.execute(f"SET s3_region='{REGION}';")

# Optional: see a global progress bar (not per-thread)
con.execute("PRAGMA enable_progress_bar;")

# Use your CPU threads
con.execute("PRAGMA threads=14;")

# --- Option A (recommended): prune by PATH (guaranteed) ---
partition_path = f"{S3_PREFIX}/deployment_code={DEPLOYMENT_CODE}/**/*.parquet"

sql_stats = f"""
SELECT
  '{DEPLOYMENT_CODE}' AS deployment_code,
  COUNT(*) AS n_rows,
  COUNT(DISTINCT time) AS n_unique_times,
  MIN(time) AS min_time,
  MAX(time) AS max_time,
  MIN(TEMP) AS min_temp,
  MAX(TEMP) AS max_temp,
  AVG(TEMP) AS mean_temp,
  STDDEV_SAMP(TEMP) AS sd_temp
FROM read_parquet('{partition_path}')
WHERE depth BETWEEN {DEPTH_MIN} AND {DEPTH_MAX}
  AND TEMP_quality_control = 1
"""

# This returns ONE row -> safe to bring into pandas
stats_df = con.execute(sql_stats).df()
print(stats_df)


# --- Option B: read whole dataset but enable Hive partitioning + filter by partition column ---
# (Good when you want to query multiple partitions at once, but PATH pruning is often faster on S3)
sql_stats_hive = f"""
SELECT
  deployment_code,
  COUNT(*) AS n_rows,
  COUNT(DISTINCT time) AS n_unique_times,
  MIN(time) AS min_time,
  MAX(time) AS max_time,
  AVG(TEMP) AS mean_temp
FROM read_parquet('{S3_PREFIX}/**/*.parquet', hive_partitioning=1)
WHERE deployment_code = '{DEPLOYMENT_CODE}'
  AND depth BETWEEN {DEPTH_MIN} AND {DEPTH_MAX}
  AND TEMP_quality_control = 1
GROUP BY deployment_code
"""

stats_df2 = con.execute(sql_stats_hive).df()
print(stats_df2)


# --- If you actually want the filtered data (be careful: can be large) ---
# Better to write to Parquet than .df() if it might be big:
OUT_LOCAL = Path(f"filtered_{DEPLOYMENT_CODE}_{DEPTH_MIN}_{DEPTH_MAX}_qc1.parquet")

sql_export = f"""
COPY (
  SELECT time, latitude, longitude, depth, TEMP
  FROM read_parquet('{partition_path}')
  WHERE depth BETWEEN {DEPTH_MIN} AND {DEPTH_MAX}
    AND TEMP_quality_control = 1
)
TO '{OUT_LOCAL.as_posix()}'
(FORMAT PARQUET, COMPRESSION ZSTD);
"""

con.execute(sql_export)
print(f"Wrote: {OUT_LOCAL.resolve()}")