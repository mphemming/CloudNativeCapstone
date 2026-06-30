# %% Import Packages

import duckdb
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import time
from shapely import wkt
from fsspec.implementations.http import HTTPFileSystem
import pyarrow.parquet as pq


# %% Initialize DuckDB connection and setup

con = duckdb.connect()

con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("""
            SET s3_region='ap-southeast-2';
            """)
con.execute("PRAGMA enable_progress_bar")
con.execute("PRAGMA threads=14")

# %% define SQL query 

SQL_QUERY = ("""
            SELECT
                TEMP
            FROM read_parquet(
                's3://aodn-cloud-optimised/slocum_glider_delayed_qc.parquet/**/*.parquet'
            )
            WHERE
                depth BETWEEN 19 AND 20
                AND TEMP_quality_control = 1
            """)

# %% Read data set from S3 using DuckDB (readin into memory, slow)

# Dataset URL
url = "https://aodn-cloud-optimised.s3.ap-southeast-2.amazonaws.com/slocum_glider_delayed_qc.parquet/"

%time df = con.execute(SQL_QUERY).df()


# count = con.execute(SQL_QUERY).fetchone()[0]
# print("Rows that will be returned:", count)

# %%  read small dataset and confirm partition columns used

con.execute(SELECT *
FROM read_parquet(
  's3://aodn-cloud-optimised/slocum_glider_delayed_qc.parquet/**/*.parquet',
  hive_partitioning=1
)
LIMIT 5;

# %% Get statistics using DuckDB (without reading into memory)

# %% Min/max for specific variables (TIME, TEMP, DEPTH, PSAL, DOX1, CPHL, LONG, LAT)

# SQL_QUERY = """
# SELECT
#     MIN(TIME)  AS min_TIME,
#     MAX(TIME)  AS max_TIME,

#     MIN(TEMP)  AS min_TEMP,
#     MAX(TEMP)  AS max_TEMP,

#     MIN(DEPTH) AS min_DEPTH,
#     MAX(DEPTH) AS max_DEPTH,

#     MIN(PSAL)  AS min_PSAL,
#     MAX(PSAL)  AS max_PSAL,

#     MIN(DOX2)  AS min_DOX2,
#     MAX(DOX2)  AS max_DOX2,

#     MIN(CPHL)  AS min_CPHL,
#     MAX(CPHL)  AS max_CPHL,

#     MIN(LONGITUDE) AS min_LON,
#     MAX(LONGITUDE) AS max_LON,

#     MIN(LATITUDE) AS min_LAT,
#     MAX(LATITUDE) AS max_LAT
    
# FROM read_parquet(
#     's3://aodn-cloud-optimised/slocum_glider_delayed_qc.parquet/**/*.parquet')

# WHERE depth BETWEEN 19 AND 20
#   AND TEMP_quality_control = 1
# """

# %time stats_df = con.execute(SQL_QUERY).df()
# stats_df



# # %% Total data points

# SQL_query = ("""
# SELECT COUNT(DISTINCT time) AS n_unique_times
# FROM read_parquet(
#     's3://aodn-cloud-optimised/slocum_glider_delayed_qc.parquet/**/*.parquet'
# )
# WHERE depth BETWEEN 19 AND 20
#   AND TEMP_quality_control = 1
# """)

# %time df = con.execute(SQL_query).df()

# df

# %% get statistics for each deployment code (using Hive partitioning)

# first get all hive partitions

S3_PREFIX = "s3://aodn-cloud-optimised/slocum_glider_delayed_qc.parquet"
REGION = "ap-southeast-2"
DEPTH_MIN, DEPTH_MAX = 19, 20

# Get partitions for looping through deployments

partitions = con.execute(f"""
WITH files AS (
  SELECT file
  FROM glob('{S3_PREFIX}/**/*.parquet')
)
SELECT DISTINCT
  regexp_extract(file, 'deployment_code=([^/]+)', 1) AS deployment_code
FROM files
WHERE file LIKE '%deployment_code=%'
  AND regexp_extract(file, 'deployment_code=([^/]+)', 1) IS NOT NULL
ORDER BY deployment_code
""").df()

partitions.head(), len(partitions)

sql_all = f"""
SELECT
    deployment_code,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT TIME) AS n_unique_times,
    MIN(TIME) AS min_time,
    MAX(TIME) AS max_time,
    MIN(TEMP) AS min_temp,
    MAX(TEMP) AS max_temp,
FROM read_parquet(
    '{S3_PREFIX}/**/*.parquet',
    hive_partitioning=1
)
WHERE depth BETWEEN {DEPTH_MIN} AND {DEPTH_MAX}
  AND TEMP_quality_control = 1
GROUP BY deployment_code
ORDER BY deployment_code
"""

stats_df = con.execute(sql_all).df()

stats_df.head()




for DEPLOYMENT_CODE in partitions['deployment_code']:
    
    print(f"Processing deployment: {DEPLOYMENT_CODE}")

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

    stats_df = con.execute(sql_stats).df()



# %% Learnings

# 34GB is too large to read into memory, even with DuckDB. We need to filter the data set more aggressively, or read it in chunks.
# Need to think through the whole process prior to writing code
# filter most of the dataset to begin with and test changes

# initial tests
# 19-20m depth, select TEMP => ~ Wall time: 8min 28s no parallelisation, Wall time: 7min 9s 14 cores used
# but duckdb with 14 cores is double the speed
# most of the time came from reading the data into a .df, so instead get the statistics back only
# Wall time: 4min 7s four count
# using hive partitioning and filtering by deployment code is much faster for getting statistics







