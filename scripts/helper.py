import os
import logging
import sys
from datetime import datetime

from pathlib import Path

import pandas as pd

LOGGER_LEVEL = "INFO"

if os.environ.get('LOGGER_LEVEL') is not None:
    LOGGER_LEVEL = os.environ['LOGGER_LEVEL']

logFormatter = logging.Formatter('[%(asctime)s] - '
                                 '[%(filename)s:%(lineno)d] - '
                                 '%(levelname)s - %(message)s',
                                 '%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)
logger.setLevel(LOGGER_LEVEL)
logger.propagate = False

console = logging.StreamHandler(sys.stdout)
console.setFormatter(logFormatter)
logger.addHandler(console)

load_date = str(datetime.today().strftime('%Y-%m-%d'))


def write_pd_to_file_in_chunks(data, file_path):
    """
 Writes a pandas DataFrame to a file in JSON Lines format, processing in chunks.

 Args:
     data (pd.DataFrame): The DataFrame to write.
     file_path (str): The path to the output file.

 Returns:
     None: Writes data to the specified file path.
 """
    chunk_size = 2000  # Define your chunk size

    # Assuming 'data' is your DataFrame and 'file_path' is your file path
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        for i in range(0, len(data), chunk_size):
            chunk = data.iloc[i:i + chunk_size]
            if i == 0:
                # Writes header or overwrites file for the first chunk
                chunk.to_json(file_path, orient='records', lines=True, mode='w')
            else:
                # Appends subsequent chunks without header
                chunk.to_json(file_path, orient='records', lines=True, mode='a')


def write_data_to_local(data, file_name):
    """
 Writes a DataFrame locally as JSON Lines

 Args:
     data (pd.DataFrame): The DataFrame to save and upload.
     file_name (str): The desired filename for the local file.

 Returns:
     None: Performs file writing operations, logging results.
 """
    try:
        file_path = "data/{}".format(file_name)
        output_file_path = Path(file_path)

        logger.info("Writing {} records to {}"
                    .format(file_name.split("/")[-1].split(".")[0], file_path))
        output_file_path.parent.mkdir(parents=True, exist_ok=True)
        # Original used direct .to_json, switched to chunk writing function
        write_pd_to_file_in_chunks(data, file_path)
    except Exception as e:
        logger.error("Uploading {} to storage: {}".format(file_name, e))


# Output file paths
jobs_list_fp = "{}/{}/jobs/jobs.json"
jobs_tasks_fp = "{}/{}/job_tasks/jobs_tasks.json"
job_runs_list_fp = "{}/{}/job_runs/job_runs.json"
clusters_list_fp = "{}/{}/clusters/clusters.json"
cluster_libraries_fp = "{}/{}/cluster_libraries/cluster_libraries.json"
clusters_activity_events_fp = "{}/{}/clusters_activity_events/clusters_activity_events.json"
db_sql_warehouse_fp = "{}/{}/db_sql_warehouses/db_sql_warehouse.json"
workspace_list_fp = "{}/{}/workspaces/workspace.json"
sql_warehouses_list_fp = "{}/{}/sql_warehouses/sql_warehouses.json"
query_history_list_fp = "{}/{}/query_history/query_history.json"
sql_queries_list_fp = "{}/{}/sql_queries/sql_queries.json"
sql_alerts_list_fp = "{}/{}/sql_alerts/sql_alerts.json"
delta_live_pipelines_list_fp = "{}/{}/delta_live_pipelines/delta_live_pipelines.json"
registered_models_list_fp = "{}/{}/registered_models/registered_models.json"
cluster_versions_list_fp = "{}/{}/cluster_versions/cluster_versions.json"
ml_experiments_list_fp = "{}/{}/ml_experiments/ml_experiments.json"
serving_endpoints_list_fp = "{}/{}/serving_endpoints/serving_endpoints.json"
catalog_fp = "{}/{}/catalogs/catalogs.json"
external_locations_fp = "{}/{}/external_locations/external_locations.json"
metastore_fp = "{}/{}/metastores/metastores.json"
tables_fp = "{}/{}/tables/tables.json"
views_fp = "{}/{}/views/views.json"
schemas_fp = "{}/{}/schemas/schemas.json"
routines_fp = "{}/{}/routines/routines.json"
volumes_fp = "{}/{}/volumes/volumes.json"
warehouses_fp = "{}/{}/warehouses/warehouses.json"
cpu_usage_fp = "{}/{}/cpu_usage/cpu_usage.json"
memory_usage_fp = "{}/{}/memory_usage/memory_usage.json"
dbu_usage_annual_fp = "{}/{}/dbu_usage_annual/dbu_usage_annual.json"
dbu_usage_category_fp = "{}/{}/dbu_usage_category/dbu_usage_category.json"
usage_summary_fp = "{}/{}/usage_summary/usage_summary.json"
storage_stats_fp = "{}/{}/storage_stats/storage_stats.json"
nodes_fp = "{}/{}/nodes/nodes.json"
least_used_tables_fp = "{}/{}/least_used_tables/least_used_tables.json"

# SQL Queries
QUERY_CATALOGS_SELECT_ALL = '''
SELECT
    catalog_name,
    catalog_owner,
    comment,
    created,
    created_by,
    last_altered,
    last_altered_by
FROM
    system.information_schema.catalogs;
'''

QUERY_METASTORES_SELECT_ALL = '''
SELECT
    metastore_id,
    metastore_name,
    metastore_owner,
    storage_root,
    storage_root_credential_id,
    storage_root_credential_name,
    delta_sharing_scope,
    delta_sharing_recipient_token_lifetime,
    delta_sharing_organization_name,
    privilege_model_version,
    cloud,
    region,
    global_metastore_id,
    created,
    created_by,
    last_altered,
    last_altered_by
FROM
    system.information_schema.metastores;'''

QUERY_TABLES_SELECT_ALL = '''
SELECT
    table_catalog,
    table_schema,
    table_name,
    table_type,
    is_insertable_into,
    commit_action,
    table_owner,
    comment,
    created,
    created_by,
    last_altered,
    last_altered_by,
    data_source_format,
    storage_sub_directory,
    storage_path
FROM
    system.information_schema.tables
WHERE table_catalog <> 'system'
AND table_schema <> 'information_schema';'''

QUERY_VOLUMES_SELECT_ALL = '''SELECT
    volume_catalog,
    volume_schema,
    volume_name,
    volume_type,
    volume_owner,
    comment,
    storage_location,
    created,
    created_by,
    last_altered,
    last_altered_by
FROM
    system.information_schema.volumes;'''

QUERY_VIEWS_SELECT_ALL = '''SELECT
    table_catalog,
    table_schema,
    table_name,
    view_definition,
    check_option,
    is_updatable,
    is_insertable_into,
    sql_path,
    is_materialized
FROM
    system.information_schema.views;'''

QUERY_SCHEMAS_SELECT_ALL = '''SELECT
    catalog_name,
    schema_name,
    schema_owner,
    comment,
    created,
    created_by,
    last_altered,
    last_altered_by
FROM
    system.information_schema.schemata;'''

QUERY_ROUTINES_SELECT_ALL = '''
SELECT
    specific_catalog,
    specific_schema,
    specific_name,
    routine_catalog,
    routine_schema,
    routine_name,
    routine_owner,
    routine_type,
    data_type,
    full_data_type,
    character_maximum_length,
    character_octet_length,
    numeric_precision,
    numeric_precision_radix,
    numeric_scale,
    datetime_precision,
    interval_type,
    interval_precision,
    maximum_cardinality,
    routine_body
FROM
    system.information_schema.routines;'''

QUERY_EXTERNAL_LOCATIONS_SELECT_ALL = '''
SELECT
    external_location_name,
    external_location_owner,
    url,
    storage_credential_id,
    storage_credential_name,
    read_only,
    comment
FROM
    system.information_schema.external_locations;'''

# CPU Stats
# How many CPU core-minutes were consumed in total during each hour.
# What was the highest number of CPU cores concurrently used at any point during each hour.
QUERY_CPU_USAGE_STATS = '''
WITH NodeTimelineWithCores AS (
    SELECT
        DATE(nt.start_time) AS usage_date,
        EXTRACT(hour from nt.start_time) AS usage_date_hour,
        nt.cluster_id,
        nt.instance_id,
        nt.start_time,
        nt.end_time,
        (nt.cpu_user_percent + nt.cpu_system_percent) AS total_cpu_percent,
        ntype.core_count
    FROM system.compute.node_timeline nt
    JOIN system.compute.node_types ntype ON nt.node_type = ntype.node_type
    WHERE DATE(nt.start_time) >= date_sub(current_date(), 90)
),
UsedCoresPerMinute AS (
    SELECT
        usage_date,
        usage_date_hour,
        start_time,
        SUM(core_count * total_cpu_percent / 100.0) AS used_cores
    FROM NodeTimelineWithCores
    GROUP BY usage_date, usage_date_hour, start_time
)
SELECT
    usage_date,
    usage_date_hour,
    SUM(used_cores) AS total_cpu_core_minutes_used,
    MAX(used_cores) AS peak_cpu_cores_used
FROM UsedCoresPerMinute
GROUP BY usage_date, usage_date_hour;
'''

# Memory Stats
# How many MB-minutes of memory were consumed in total during each hour.
# What was the peak memory usage (in MB) across all nodes at any single point in time during each hour.
QUERY_MEMORY_USAGE_STATS = '''
WITH NodeTimelineWithMemory AS (
    SELECT
        DATE(nt.start_time) AS usage_date,
        EXTRACT(hour from nt.start_time) AS usage_date_hour,
        nt.cluster_id,
        nt.instance_id,
        nt.start_time,
        nt.end_time,
        nt.mem_used_percent,
        ntype.memory_mb
    FROM system.compute.node_timeline nt
    JOIN system.compute.node_types ntype ON nt.node_type = ntype.node_type
    WHERE DATE(nt.start_time) >= date_sub(current_date(), 90)
),
UsedMemoryPerMinute AS (
    SELECT
        usage_date,
        usage_date_hour,
        start_time,
        SUM(memory_mb * mem_used_percent / 100.0) AS used_memory_mb
    FROM NodeTimelineWithMemory
    GROUP BY usage_date, usage_date_hour, start_time
)
SELECT
    usage_date,
    usage_date_hour,
    SUM(used_memory_mb) AS total_memory_mb_minutes_used,
    MAX(used_memory_mb) AS peak_memory_mb_used
FROM UsedMemoryPerMinute
GROUP BY usage_date, usage_date_hour;
'''

QUERY_WAREHOUSES_SELECT_ALL = '''
select
  warehouse_id,
  workspace_id,
  account_id,
  warehouse_name,
  warehouse_type,
  warehouse_channel,
  warehouse_size,
  min_clusters,
  max_clusters,
  auto_stop_minutes,
  tags,
  change_time,
  delete_time
from
  system.compute.warehouses;
'''

QUERY_DBU_USAGE_STATS_ANNUAL = '''
SELECT
    EXTRACT(YEAR FROM usage_date) AS usage_year,
    SUM(usage_quantity) as total_dbus
FROM
    system.billing.usage
GROUP BY
    usage_year
ORDER BY
    usage_year;
'''

QUERY_USAGE_SUMMARY = '''
SELECT
      count(distinct u.workspace_id) as workspace_count,
      case when u.sku_name like '%SQL%' then 'SQL' else 'Standard' end as sku_category,
      case when sku_category = 'SQL' then round(sum(usage_quantity), 2) end as SQL_DBUs,
      case when sku_category = 'Standard' then round(sum(usage_quantity), 2) end as Standard_DBUs,
      sum(usage_quantity * lp.pricing.default) as dollar_dbus 
from system.billing.usage as u
inner join system.billing.list_prices lp 
ON u.account_id = lp.account_id
and u.cloud = lp.cloud
and u.sku_name = lp.sku_name
and u.usage_start_time between lp.price_start_time
and coalesce(lp.price_end_time, u.usage_end_time)
group by all
'''

QUERY_DBU_USAGE_STATS_CATEGORY = '''
-- Calculates DBU usage percentage over the last 90 days, categorized by workload type.
-- Categories:
--   - SQL/Analytics: All SQL-related SKUs.
--   - ETL: Job computes and Delta Live Tables.
--   - Spark/Python: All-purpose interactive compute.
--   - Other: Any other usage.
WITH categorized_usage AS (
    SELECT
        u.usage_quantity,
        CASE
            WHEN u.sku_name LIKE '%SQL%' THEN 'SQL/Analytics'
            WHEN u.sku_name LIKE '%JOBS_COMPUTE%' OR u.sku_name LIKE '%DLT%' THEN 'ETL'
            WHEN u.sku_name LIKE '%ALL_PURPOSE_COMPUTE%' THEN 'Spark/Python'
            ELSE 'Other'
        END as usage_category
    FROM system.billing.usage AS u
    WHERE u.usage_start_time >= date_sub(current_date(), 90)
)
SELECT
    usage_category,
    SUM(usage_quantity) AS total_dbus,
    (SUM(usage_quantity) / (SELECT SUM(usage_quantity) FROM categorized_usage)) * 100 AS percentage_of_total_dbus
FROM categorized_usage
GROUP BY usage_category;'''

QUERY_LEAST_USED_TABLES = '''WITH all_tables AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    table_type
  FROM
    system.information_schema.tables
  WHERE
    table_schema NOT IN ('information_schema', 'system')
),
recent_activity AS (
  SELECT
    source_table_catalog,
    source_table_schema,
    source_table_name,
    source_type,
    max(event_date) as last_event_date
  FROM
    system.access.table_lineage
  WHERE
    source_type is not null
    -- Filter for access events in the last 360 days
    AND event_time >= now() - INTERVAL '360' DAY
  GROUP BY
    source_table_catalog,
    source_table_schema,
    source_table_name,
    source_type
)
SELECT
  at.table_catalog,
  at.table_schema,
  at.table_name,
  at.table_type,
  ra.source_type,
  ra.last_event_date
FROM
  all_tables at
    LEFT JOIN recent_activity ra
      ON at.table_catalog = ra.source_table_catalog
      AND at.table_schema = ra.source_table_schema
      AND at.table_name = ra.source_table_name;'''