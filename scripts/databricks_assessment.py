from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Union, List, Dict, Any, Callable
import os
import pandas as pd
from base64 import b64decode
from databricks.sdk.service import workspace

# Custom module imports
from client_utils import get_databricks_client
from warehouse_manager import DatabricksSQLClient
from helper import *


class DatabricksAssessment:
    def __init__(self, customer_name):
        self.customer_name = customer_name
        self.client = get_databricks_client()
        self.host = None
        self.workspace_id = None
        self.cloud = None
        self.load_date = load_date  # Assuming load_date is a global constant from helper.py

        if self.client and self.client.config.host:
            self.host = self.client.config.host
            try:
                self.workspace_id = self.client.get_workspace_id()
            except Exception as e:
                logger.error(f"Assessor init: Failed to get workspace_id for customer {self.customer_name}: {e}")

            if '.com' in self.host:
                self.cloud = 'AWS'
            elif '.net' in self.host:
                self.cloud = 'AZURE'
            else:
                logger.warning(f"Assessor init: Cloud unknown for host {self.host}, defaulting to GCP.")
                self.cloud = 'GCP'
        else:
            logger.error(
                f"Assessor init: Failed to initialize Databricks client or client host is not configured for customer {self.customer_name}.")
            self.client = None

    def _get_workspace_id_for_logging(self) -> str:
        """Helper to safely get workspace_id for logging, defaulting to 'unknown'."""
        return self.workspace_id if self.workspace_id else "unknown"

    def append_common_fields(self, df: pd.DataFrame) -> Union[pd.DataFrame, None]:
        """
        Appends common metadata columns ('cloud', 'load_date', 'instance', 'workspace_id') to a DataFrame.
        """
        if df is None:
            logger.warning(f"Input DataFrame is None in append_common_fields for customer {self.customer_name}.")
            return None

        try:
            df['customer'] = self.customer_name

            if self.cloud: df['cloud'] = self.cloud
            if self.load_date: df['load_date'] = self.load_date
            if self.host: df['instance'] = self.host
            if self.workspace_id: df['workspace_id'] = self.workspace_id

            if not all([self.cloud, self.host, self.workspace_id]):
                logger.warning(
                    f"One or more common fields (cloud, host, workspace_id) were not initialized for customer {self.customer_name}. DataFrame might be incomplete.")
            return df
        except Exception as e:
            logger.error(f"Appending common fields failed for customer {self.customer_name}: {e}", exc_info=True)
            if 'customer' not in df.columns and self.customer_name:
                try:
                    df['customer'] = self.customer_name
                except Exception as cust_add_err:
                    logger.error(
                        f"Failed to add customer column during error handling for {self.customer_name}: {cust_add_err}")
            return df

    def replace_duplicate_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Identifies duplicate columns in a pandas DataFrame and replaces the underscore in subsequent occurrences with a hash symbol (#) to make them unique. Logs a report of the columns renamed.
        """
        cols = df.columns
        new_cols = []
        seen_cols = {}
        renamed_cols = {}

        for col in cols:
            if col not in seen_cols:
                new_cols.append(col)
                seen_cols[col] = 1
            else:
                new_col = col.replace('_', '__', seen_cols[col])
                new_cols.append(new_col)
                renamed_cols[col] = new_col
                seen_cols[col] += 1

        if renamed_cols:
            logger.info(f"Renamed duplicate columns: {renamed_cols}")

        df.columns = new_cols
        return df.copy()

    def create_pd_with_cols(self, data) -> Union[pd.DataFrame, None]:
        """
        Creates a pandas DataFrame from a list of dictionaries, cleans column names, and appends common fields.
        """
        try:
            if not data:
                logger.info(f"No data received to create pd for customer {self.customer_name}, skipping")
                return None

            df = pd.json_normalize(data)
            new_columns = {col: str(col).replace('.', '_') for col in df.columns}
            df = df.rename(columns=new_columns)
            df = self.replace_duplicate_columns(df)
            df = self.append_common_fields(df)
            return df
        except Exception as e:
            logger.error(f"Creating DF failed for customer {self.customer_name}: {e}", exc_info=True)
            return None

    # --- Data Fetching Methods ---
    def fetch_and_process_jobs(self):
        workspace_id_log = self._get_workspace_id_for_logging()
        logger.info(f"Fetching jobs data in workspace: {workspace_id_log} for customer: {self.customer_name}")
        if not self.client:
            logger.error(f"Client not available for fetch_and_process_jobs, customer {self.customer_name}.")
            return

        try:
            jobs_list_result = []
            jobs_tasks_result = []
            for job in self.client.jobs.list(expand_tasks=True):
                job_data = job.as_dict()
                settings = job_data.get('settings', {})
                tasks = settings.pop('tasks', [])
                job_id = job_data.get('job_id', '')

                if isinstance(tasks, list):
                    for task in tasks:
                        if isinstance(task, dict):
                            task['job_id'] = job_id
                            jobs_tasks_result.append(task)
                        else:
                            logger.warning(
                                f"Skipping non-dictionary task item in job {job_id} for customer {self.customer_name}: {task}")
                else:
                    logger.warning(
                        f"Expected 'tasks' to be a list in job {job_id} for customer {self.customer_name}, but got {type(tasks)}. Skipping tasks.")
                jobs_list_result.append(job_data)

            jobs_df = self.create_pd_with_cols(jobs_list_result)
            if jobs_df is not None:
                write_data_to_local(jobs_df, jobs_list_fp.format(self.load_date, workspace_id_log))
                logger.info(f"Fetched {len(jobs_df)} jobs for customer {self.customer_name}")

            jobs_tasks_df = self.create_pd_with_cols(jobs_tasks_result)
            if jobs_tasks_df is not None:
                write_data_to_local(jobs_tasks_df, jobs_tasks_fp.format(self.load_date, workspace_id_log))
                logger.info(f"Fetched {len(jobs_tasks_df)} job tasks for customer {self.customer_name}")
        except Exception as e:
            logger.error(f"Fetching jobs failed for workspace {workspace_id_log}, customer {self.customer_name}: {e}",
                         exc_info=True)

    def fetch_job_runs(self):
        workspace_id_log = self._get_workspace_id_for_logging()
        logger.info(f"Fetching job runs data in workspace: {workspace_id_log} for customer: {self.customer_name}")
        if not self.client:
            logger.error(f"Client not available for fetch_job_runs, customer {self.customer_name}.")
            return
        try:
            job_runs_list = [run.as_dict() for run in self.client.jobs.list_runs()]
            job_runs_df = self.create_pd_with_cols(job_runs_list)
            if job_runs_df is not None:
                write_data_to_local(job_runs_df, job_runs_list_fp.format(self.load_date, workspace_id_log))
                logger.info(f"Fetched {len(job_runs_df)} job runs for customer {self.customer_name}")
        except Exception as e:
            logger.error(
                f"Fetching job runs failed for workspace {workspace_id_log}, customer {self.customer_name}: {e}",
                exc_info=True)

    def fetch_clusters_and_nodes_stats(self):
        workspace_id_log = self._get_workspace_id_for_logging()
        logger.info(f"Fetching clusters data in workspace: {workspace_id_log} for customer: {self.customer_name}")
        if not self.client:
            logger.error(f"Client not available for fetch_clusters, customer {self.customer_name}.")
            return
        try:
            clusters = [cluster.as_dict() for cluster in self.client.clusters.list()]
            clusters_df = self.create_pd_with_cols(clusters)
            if clusters_df is not None:
                write_data_to_local(clusters_df, clusters_list_fp.format(self.load_date, workspace_id_log))
                logger.info(f"Fetched {len(clusters_df)} clusters for customer {self.customer_name}")

                num_workers = 0
                if 'num_workers' in clusters_df.columns:
                    # Sum of workers for fixed-size clusters
                    num_workers += clusters_df['num_workers'].fillna(0).sum()

                if 'autoscale_max_workers' in clusters_df.columns:
                    # Sum of max workers for autoscaling clusters, for capacity planning
                    num_workers += clusters_df['autoscale_max_workers'].fillna(0).sum()

                # Total nodes = driver nodes (1 per cluster) + worker nodes
                total_nodes = num_workers + len(clusters_df)
                nodes_df = pd.DataFrame([{'total_nodes': int(total_nodes), 'total_clusters': len(clusters_df)}])
                nodes_df = self.append_common_fields(nodes_df)
                if nodes_df is not None:
                    write_data_to_local(nodes_df, nodes_fp.format(self.load_date, workspace_id_log))
                    logger.info(f"Saved nodes count for customer {self.customer_name}")

        except Exception as e:
            logger.error(
                f"Fetching clusters failed for workspace {workspace_id_log}, customer {self.customer_name}: {e}",
                exc_info=True)

    def fetch_clusters_events(self):
        workspace_id_log = self._get_workspace_id_for_logging()
        logger.info(
            f"Fetching cluster activity events data in workspace: {workspace_id_log} for customer: {self.customer_name}")
        if not self.client:
            logger.error(f"Client not available for fetch_clusters_events, customer {self.customer_name}.")
            return
        try:
            clusters = [cluster.as_dict() for cluster in self.client.clusters.list()]
            cluster_ids = [cluster.get("cluster_id") for cluster in clusters if cluster.get("cluster_id") is not None]
            all_cluster_events = [event.as_dict() for cluster_id in cluster_ids for event in
                                  self.client.clusters.events(cluster_id=cluster_id)]
            all_cluster_events_df = self.create_pd_with_cols(all_cluster_events)
            if all_cluster_events_df is not None:
                write_data_to_local(all_cluster_events_df,
                                    clusters_activity_events_fp.format(self.load_date, workspace_id_log))
                logger.info(
                    f"Fetched {len(all_cluster_events_df)} cluster activity events for customer {self.customer_name}")
        except Exception as e:
            logger.error(
                f"Fetching cluster activity events failed for workspace {workspace_id_log}, customer {self.customer_name}: {e}",
                exc_info=True)

    def fetch_cluster_libraries(self):
        workspace_id_log = self._get_workspace_id_for_logging()
        logger.info(
            f"Fetching cluster libraries for each cluster in workspace: {workspace_id_log} for customer: {self.customer_name}")
        if not self.client:
            logger.error(f"Client not available for fetch_clusters_events, customer {self.customer_name}.")
            return
        try:
            parsed_libraries = []
            data = [cluster.as_dict() for cluster in self.client.libraries.all_cluster_statuses()]

            # Check if the 'statuses' key exists and is a list
            if not isinstance(data, list):
                print("Error: No Input received")
                return parsed_libraries

            # Iterate over each cluster's status information
            for cluster_status in data:
                cluster_id = cluster_status.get('cluster_id')
                library_statuses = cluster_status.get('library_statuses', [])

                if not cluster_id:
                    continue

                # Iterate over each library installed on the cluster
                for lib_status in library_statuses:
                    status = lib_status.get('status', 'UNKNOWN')
                    library_info = lib_status.get('library', {})
                    is_library_for_all_clusters = lib_status.get('is_library_for_all_clusters', False)

                    # The library_info dictionary contains the type as a key (e.g., 'pypi', 'jar')
                    # This loop will run only once for each library
                    for library_type, library_details in library_info.items():
                        parsed_libraries.append({
                            "cluster_id": cluster_id,
                            "library_type": library_type,
                            "library": library_details,
                            "is_library_for_all_clusters": is_library_for_all_clusters,
                            "status": status
                        })

            all_cluster_libraries_df = self.create_pd_with_cols(parsed_libraries)
            if all_cluster_libraries_df is not None:
                write_data_to_local(all_cluster_libraries_df,
                                    cluster_libraries_fp.format(self.load_date, workspace_id_log))
                logger.info(
                    f"Fetched {len(all_cluster_libraries_df)} cluster libraries for customer {self.customer_name}")
        except Exception as e:
            logger.error(
                f"Fetching cluster libraries failed for workspace {workspace_id_log}, customer {self.customer_name}: {e}",
                exc_info=True)

    def fetch_sql_warehouses(self):
        workspace_id_log = self._get_workspace_id_for_logging()
        logger.info(f"Fetching SQL warehouses data in workspace: {workspace_id_log} for customer: {self.customer_name}")
        if not self.client:
            logger.error(f"Client not available for fetch_sql_warehouses, customer {self.customer_name}.")
            return
        try:
            sql_warehouses = [warehouse.as_dict() for warehouse in self.client.warehouses.list()]
            sql_warehouses_df = self.create_pd_with_cols(sql_warehouses)
            if sql_warehouses_df is not None:
                write_data_to_local(sql_warehouses_df, sql_warehouses_list_fp.format(self.load_date, workspace_id_log))
                logger.info(f"Fetched {len(sql_warehouses_df)} SQL warehouses for customer {self.customer_name}")
        except Exception as e:
            logger.error(
                f"Fetching SQL warehouses failed for workspace {workspace_id_log}, customer {self.customer_name}: {e}",
                exc_info=True)

    def fetch_sql_query_history(self):
        workspace_id_log = self._get_workspace_id_for_logging()
        logger.info(f"Fetching SQL query history in workspace: {workspace_id_log} for customer: {self.customer_name}")
        if not self.client:
            logger.error(f"Client not available for fetch_sql_query_history, customer {self.customer_name}.")
            return
        try:
            # Fetch sql query history with pagination
            next_page_token = None
            query_history = []
            page = 0
            max_pages = 1000

            while page < max_pages:
                page += 1
                try:
                    records = self.client.query_history.list(page_token=next_page_token, max_results=100)
                    records_data = records.as_dict()
                except Exception as page_err:
                    logger.error(
                        f"Error fetching query history page {page} for customer {self.customer_name}, workspace {workspace_id_log}: {page_err}")
                    break

                res_list = records_data.get('res', [])
                if not res_list:
                    logger.info(
                        f"No more query history results found on page {page} for customer {self.customer_name}, workspace {workspace_id_log}.")
                    break
                query_history.extend(res_list)
                if not records_data.get('has_next_page', False):
                    break
                next_page_token = records_data.get('next_page_token')
                if not next_page_token:
                    logger.warning(
                        f"API indicated 'has_next_page' but no 'next_page_token' provided on page {page} for customer {self.customer_name}. Stopping.")
                    break

            if page >= max_pages:
                logger.warning(
                    f"Reached max page limit ({max_pages}) fetching query history for customer {self.customer_name}, workspace {workspace_id_log}. Data might be incomplete.")

            query_history_df = self.create_pd_with_cols(query_history)
            if query_history_df is not None:
                write_data_to_local(query_history_df, query_history_list_fp.format(self.load_date, workspace_id_log))
                logger.info(
                    f"Fetched {len(query_history_df)} SQL query history entries for customer {self.customer_name}")
        except Exception as e:
            logger.error(
                f"Fetching SQL query history failed for workspace {workspace_id_log}, customer {self.customer_name}: {e}",
                exc_info=True)

    def fetch_sql_queries(self):
        workspace_id_log = self._get_workspace_id_for_logging()
        logger.info(
            f"Fetching saved SQL queries data in workspace: {workspace_id_log} for customer: {self.customer_name}")
        if not self.client:
            logger.error(f"Client not available for fetch_sql_queries, customer {self.customer_name}.")
            return
        try:
            sql_queries = [query.as_dict() for query in self.client.queries.list()]
            sql_queries_df = self.create_pd_with_cols(sql_queries)
            if sql_queries_df is not None:
                write_data_to_local(sql_queries_df, sql_queries_list_fp.format(self.load_date, workspace_id_log))
                logger.info(f"Fetched {len(sql_queries_df)} saved SQL queries for customer {self.customer_name}")
        except Exception as e:
            logger.error(
                f"Fetching saved SQL queries failed for workspace {workspace_id_log}, customer {self.customer_name}: {e}",
                exc_info=True)

    def fetch_sql_alerts(self):
        workspace_id_log = self._get_workspace_id_for_logging()
        logger.info(f"Fetching SQL alerts data in workspace: {workspace_id_log} for customer: {self.customer_name}")
        if not self.client:
            logger.error(f"Client not available for fetch_sql_alerts, customer {self.customer_name}.")
            return
        try:
            sql_alerts = [alert.as_dict() for alert in self.client.alerts.list()]
            sql_alerts_df = self.create_pd_with_cols(sql_alerts)
            if sql_alerts_df is not None:
                write_data_to_local(sql_alerts_df, sql_alerts_list_fp.format(self.load_date, workspace_id_log))
                logger.info(f"Fetched {len(sql_alerts_df)} SQL alerts for customer {self.customer_name}")
        except Exception as e:
            logger.error(
                f"Fetching SQL alerts failed for workspace {workspace_id_log}, customer {self.customer_name}: {e}",
                exc_info=True)

    def fetch_delta_live_pipelines(self):
        workspace_id_log = self._get_workspace_id_for_logging()
        logger.info(
            f"Fetching Delta Live Pipelines data in workspace: {workspace_id_log} for customer: {self.customer_name}")
        if not self.client:
            logger.error(f"Client not available for fetch_delta_live_pipelines, customer {self.customer_name}.")
            return
        try:
            delta_live_pipelines = [pipeline.as_dict() for pipeline in self.client.pipelines.list_pipelines()]
            delta_live_pipelines_df = self.create_pd_with_cols(delta_live_pipelines)
            if delta_live_pipelines_df is not None:
                write_data_to_local(delta_live_pipelines_df,
                                    delta_live_pipelines_list_fp.format(self.load_date, workspace_id_log))
                logger.info(
                    f"Fetched {len(delta_live_pipelines_df)} Delta Live Pipelines for customer {self.customer_name}")
        except Exception as e:
            logger.error(
                f"Fetching Delta Live Pipelines failed for workspace {workspace_id_log}, customer {self.customer_name}: {e}",
                exc_info=True)

    def fetch_registered_models(self):
        workspace_id_log = self._get_workspace_id_for_logging()
        logger.info(
            f"Fetching registered models data in workspace: {workspace_id_log} for customer: {self.customer_name}")
        if not self.client:
            logger.error(f"Client not available for fetch_registered_models, customer {self.customer_name}.")
            return
        try:
            registered_models = [registered_model.as_dict() for registered_model in
                                 self.client.registered_models.list()]
            registered_models_df = self.create_pd_with_cols(registered_models)
            if registered_models_df is not None:
                write_data_to_local(registered_models_df,
                                    registered_models_list_fp.format(self.load_date, workspace_id_log))
                logger.info(f"Fetched {len(registered_models_df)} registered models for customer {self.customer_name}")
        except Exception as e:
            logger.error(
                f"Fetching registered models failed for workspace {workspace_id_log}, customer {self.customer_name}: {e}",
                exc_info=True)

    def fetch_ml_experiments(self):
        workspace_id_log = self._get_workspace_id_for_logging()
        logger.info(f"Fetching ML experiments data in workspace: {workspace_id_log} for customer: {self.customer_name}")
        if not self.client:
            logger.error(f"Client not available for fetch_ml_experiments, customer {self.customer_name}.")
            return
        try:
            ml_experiments = [experiment.as_dict() for experiment in self.client.experiments.list_experiments()]
            ml_experiments_df = self.create_pd_with_cols(ml_experiments)
            if ml_experiments_df is not None:
                write_data_to_local(ml_experiments_df, ml_experiments_list_fp.format(self.load_date, workspace_id_log))
                logger.info(f"Fetched {len(ml_experiments_df)} ML experiments for customer {self.customer_name}")
        except Exception as e:
            logger.error(
                f"Fetching ML experiments failed for workspace {workspace_id_log}, customer {self.customer_name}: {e}",
                exc_info=True)

    def fetch_serving_endpoints(self):
        workspace_id_log = self._get_workspace_id_for_logging()
        logger.info(
            f"Fetching serving endpoints data in workspace: {workspace_id_log} for customer: {self.customer_name}")
        if not self.client:
            logger.error(f"Client not available for fetch_serving_endpoints, customer {self.customer_name}.")
            return
        try:
            serving_endpoints = []
            endpoints_response = self.client.serving_endpoints.list()
            if hasattr(endpoints_response, 'endpoints') and endpoints_response.endpoints:
                for endpoint in endpoints_response.endpoints:
                    serving_endpoints.append(endpoint.as_dict())
            elif isinstance(endpoints_response, list):  # Handle cases where the SDK might return a list directly
                for endpoint in endpoints_response:
                    serving_endpoints.append(endpoint.as_dict())
            else:
                logger.info(
                    f"No serving endpoints found or unexpected response format in workspace {workspace_id_log} for customer {self.customer_name}.")

            serving_endpoints_df = self.create_pd_with_cols(serving_endpoints)
            if serving_endpoints_df is not None:
                write_data_to_local(serving_endpoints_df,
                                    serving_endpoints_list_fp.format(self.load_date, workspace_id_log))
                logger.info(f"Fetched {len(serving_endpoints_df)} serving endpoints for customer {self.customer_name}")
        except Exception as e:
            logger.error(
                f"Fetching serving endpoints failed for workspace {workspace_id_log}, customer {self.customer_name}: {e}",
                exc_info=True)

    def _fetch_sql_based_data(self, warehouse_id: str, query: str, output_fp_format: str, entity_name: str):
        workspace_id_log = self._get_workspace_id_for_logging()
        logger.info(
            f"Fetching {entity_name} data in workspace: {workspace_id_log} for customer: {self.customer_name} using WH: {warehouse_id}")
        if not self.client:
            logger.error(f"Client not available for fetching {entity_name}, customer {self.customer_name}.")
            return
        try:
            sql_client = DatabricksSQLClient(self.client)
            df = sql_client.execute_query(warehouse_id, query)

            if df is not None and not df.empty:
                df = self.append_common_fields(df)
                if df is not None:
                    write_data_to_local(df, output_fp_format.format(self.load_date, workspace_id_log))
                    logger.info(f"Fetched {len(df)} {entity_name} for customer {self.customer_name}")
            elif df is not None and df.empty:
                logger.info(f"No {entity_name} found for customer {self.customer_name}, workspace {workspace_id_log}.")
            else:
                logger.warning(
                    f"{entity_name} DataFrame is None for customer {self.customer_name}, workspace {workspace_id_log}.")
        except Exception as e:
            logger.error(
                f"Fetching {entity_name} failed for workspace {workspace_id_log}, customer {self.customer_name}: {e}",
                exc_info=True)

    def fetch_catalogs(self, warehouse_id: str):
        self._fetch_sql_based_data(warehouse_id, QUERY_CATALOGS_SELECT_ALL, catalog_fp, "catalogs")

    def fetch_metastores(self, warehouse_id: str):
        self._fetch_sql_based_data(warehouse_id, QUERY_METASTORES_SELECT_ALL, metastore_fp, "metastores")

    def fetch_tables(self, warehouse_id: str):
        self._fetch_sql_based_data(warehouse_id, QUERY_TABLES_SELECT_ALL, tables_fp, "tables")

    def fetch_views(self, warehouse_id: str):
        self._fetch_sql_based_data(warehouse_id, QUERY_VIEWS_SELECT_ALL, views_fp, "views")

    def fetch_schemas(self, warehouse_id: str):
        self._fetch_sql_based_data(warehouse_id, QUERY_SCHEMAS_SELECT_ALL, schemas_fp, "schemas")

    def fetch_routines(self, warehouse_id: str):
        self._fetch_sql_based_data(warehouse_id, QUERY_ROUTINES_SELECT_ALL, routines_fp, "routines")

    def fetch_volumes(self, warehouse_id: str):
        self._fetch_sql_based_data(warehouse_id, QUERY_VOLUMES_SELECT_ALL, volumes_fp, "volumes")

    def fetch_warehouses(self, warehouse_id: str):
        self._fetch_sql_based_data(warehouse_id, QUERY_WAREHOUSES_SELECT_ALL, warehouses_fp, "warehouses")

    def fetch_external_locations(self, warehouse_id: str):
        self._fetch_sql_based_data(warehouse_id, QUERY_EXTERNAL_LOCATIONS_SELECT_ALL, external_locations_fp,
                                   "external locations")

    def fetch_cpu_usage_stats(self, warehouse_id: str):
        self._fetch_sql_based_data(warehouse_id, QUERY_CPU_USAGE_STATS, cpu_usage_fp, "CPU usage stats")

    def fetch_memory_usage_stats(self, warehouse_id: str):
        self._fetch_sql_based_data(warehouse_id, QUERY_MEMORY_USAGE_STATS, memory_usage_fp, "memory usage stats")

    def fetch_dbu_usage_stats_annual(self, warehouse_id: str):
        self._fetch_sql_based_data(warehouse_id, QUERY_DBU_USAGE_STATS_ANNUAL, dbu_usage_annual_fp,
                                   "DBU usage stats annual")

    def fetch_dbu_usage_stats_category(self, warehouse_id: str):
        self._fetch_sql_based_data(warehouse_id, QUERY_DBU_USAGE_STATS_CATEGORY, dbu_usage_category_fp,
                                   "DBU usage stats by category")

    def fetch_usage_summary(self, warehouse_id: str):
        self._fetch_sql_based_data(warehouse_id, QUERY_USAGE_SUMMARY, usage_summary_fp, "usage summary")

    def fetch_least_used_tables(self, warehouse_id: str):
        self._fetch_sql_based_data(warehouse_id, QUERY_LEAST_USED_TABLES, least_used_tables_fp, "least used tables")

    def collect_workspace_file_stat_metadata_from_sdk(self, path):
        file_metadata_list = []
        for i in self.client.workspace.list(path, recursive=True):
            j = i.as_dict()
            file_metadata_list.append(j)
        return file_metadata_list

    def collect_nonrepo_notebook_source_from_sdk(self, path_list: list, output_dir: str) -> int:
        num_sources_written = 0
        for obj_id, path in path_list:
            try:
                export_result = self.client.workspace.export(path, format=workspace.ExportFormat.SOURCE)
                decoded_source = b64decode(export_result.content).decode('utf-8')
                filepath = f"{output_dir}/{str(obj_id)}_code.py"
                with open(filepath, 'w') as f_out:
                    f_out.write(decoded_source)
                    num_sources_written += 1
            except Exception as export_err:
                logger.error(
                    f'Error processing notebook {path} (ID: {obj_id}) for customer {self.customer_name}. Error: {export_err}',
                    exc_info=True)
        return num_sources_written

    def extract_notebooks(self):
        workspace_id_log = self._get_workspace_id_for_logging()
        logger.info(f"Extracting notebooks in workspace: {workspace_id_log} for customer: {self.customer_name}")
        if not self.client:
            logger.error(f"Client not available for extract_notebooks, customer {self.customer_name}.")
            return
        try:
            workspace_files_obj_list = self.collect_workspace_file_stat_metadata_from_sdk('/')
            workspace_files_obj_pd = pd.DataFrame.from_dict(workspace_files_obj_list)

            code_output_dir = f"./data/{self.load_date}/{workspace_id_log}/notebooks_extract"
            os.makedirs(code_output_dir, exist_ok=True)

            notebook_list_pd = pd.DataFrame.from_dict(workspace_files_obj_list)
            notebook_list_pd['dbricks_workspace_id'] = workspace_id_log
            notebook_list_pd.to_json(f'{code_output_dir}/notebook_inventory.json', orient='records', lines=True)

            notebook_path_list = list(
                zip(
                    workspace_files_obj_pd[workspace_files_obj_pd.object_type == 'NOTEBOOK'].object_id,
                    workspace_files_obj_pd[workspace_files_obj_pd.object_type == 'NOTEBOOK'].path
                )
            )
            notebooks_processed = self.collect_nonrepo_notebook_source_from_sdk(notebook_path_list, code_output_dir)
            logger.info(f"Extracted {notebooks_processed} source codes out of {len(notebook_path_list)} notebooks")
        except Exception as e:
            logger.error(
                f"Extract notebooks failed for workspace {workspace_id_log}, customer {self.customer_name}: {e}",
                exc_info=True)

    def process_workspace_data(self):
        if not self.client:
            logger.error(
                f"Databricks client not initialized for customer {self.customer_name}. Aborting data processing.")
            return

        warehouse_sql_client = None
        warehouse_id = None
        try:
            warehouse_sql_client = DatabricksSQLClient(self.client)
            warehouse_id = warehouse_sql_client.create_warehouse()
            if not warehouse_id:
                logger.error(
                    f"Failed to create temporary SQL warehouse for customer {self.customer_name}. SQL-based tasks will be skipped.")
            else:
                logger.info(f"Warehouse '{warehouse_id}' created successfully for customer {self.customer_name}.")

            tasks_to_run_defs: Dict[str, Callable] = {
                "fetch_and_process_jobs": self.fetch_and_process_jobs,
                "fetch_job_runs": self.fetch_job_runs,
                "fetch_clusters_and_nodes_stats": self.fetch_clusters_and_nodes_stats,
                "fetch_cluster_libraries": self.fetch_cluster_libraries,
                "fetch_clusters_events": self.fetch_clusters_events,
                "fetch_sql_warehouses": self.fetch_sql_warehouses,
                "fetch_sql_queries": self.fetch_sql_queries,
                "fetch_sql_alerts": self.fetch_sql_alerts,
                "fetch_sql_query_history": self.fetch_sql_query_history,
                "fetch_delta_live_pipelines": self.fetch_delta_live_pipelines,
                "fetch_registered_models": self.fetch_registered_models,
                "fetch_ml_experiments": self.fetch_ml_experiments,
                "fetch_serving_endpoints": self.fetch_serving_endpoints,
                "extract_notebooks": self.extract_notebooks,
            }

            if warehouse_id:
                sql_tasks = {
                    "fetch_catalogs": lambda: self.fetch_catalogs(warehouse_id),
                    "fetch_metastores": lambda: self.fetch_metastores(warehouse_id),
                    "fetch_tables": lambda: self.fetch_tables(warehouse_id),
                    "fetch_views": lambda: self.fetch_views(warehouse_id),
                    "fetch_schemas": lambda: self.fetch_schemas(warehouse_id),
                    "fetch_routines": lambda: self.fetch_routines(warehouse_id),
                    "fetch_volumes": lambda: self.fetch_volumes(warehouse_id),
                    "fetch_warehouses": lambda: self.fetch_warehouses(warehouse_id),
                    "fetch_external_locations": lambda: self.fetch_external_locations(warehouse_id),
                    "fetch_cpu_usage_stats": lambda: self.fetch_cpu_usage_stats(warehouse_id),
                    "fetch_memory_usage_stats": lambda: self.fetch_memory_usage_stats(warehouse_id),
                    "fetch_dbu_usage_stats_annual": lambda: self.fetch_dbu_usage_stats_annual(warehouse_id),
                    "fetch_dbu_usage_stats_category": lambda: self.fetch_dbu_usage_stats_category(warehouse_id),
                    "fetch_usage_summary": lambda: self.fetch_usage_summary(warehouse_id),
                    "fetch_least_used_tables": lambda: self.fetch_least_used_tables(warehouse_id), }
                tasks_to_run_defs.update(sql_tasks)
            else:
                logger.warning(
                    f"SQL warehouse not available for customer {self.customer_name}. Skipping SQL-dependent data fetching tasks.")

            with ThreadPoolExecutor() as executor:
                futures_map = {executor.submit(func): name for name, func in tasks_to_run_defs.items()}
                for future in as_completed(futures_map):
                    task_name = futures_map[future]
                    try:
                        future.result()
                        logger.info(f"Task '{task_name}' completed successfully for customer '{self.customer_name}'.")
                    except Exception as e:
                        logger.error(f"Error in task '{task_name}' for customer '{self.customer_name}': {e}",
                                     exc_info=True)

        except Exception as e:
            logger.error(f"Orchestration process failed for customer {self.customer_name}: {e}", exc_info=True)
        finally:
            if warehouse_id and warehouse_sql_client:
                try:
                    warehouse_sql_client.delete_warehouse(warehouse_id)
                    logger.info(f"Warehouse '{warehouse_id}' deleted successfully for customer {self.customer_name}.")
                except Exception as del_e:
                    logger.error(
                        f"Failed to delete warehouse '{warehouse_id}' for customer {self.customer_name}: {del_e}",
                        exc_info=True)
            elif warehouse_id:
                logger.warning(
                    f"Warehouse '{warehouse_id}' for customer {self.customer_name} was created but could not be deleted")
