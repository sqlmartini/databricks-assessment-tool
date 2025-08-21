import random
import string
import time
from typing import List, Optional, Dict, Any
import pandas as pd
import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql
from io import StringIO
import logging


class DatabricksSQLClient:
    """
    Client for executing SQL statements on a Databricks SQL warehouse and retrieving results.

    Handles fetching data in chunks using external links.
    """

    def __init__(self, workspace_client: WorkspaceClient):
        """
        Initializes the DatabricksSQLClient.

        Args:
            workspace_client (Optional[WorkspaceClient]):  An existing WorkspaceClient instance.
                If not provided, a new one will be created.
        """
        self.workspace_client = workspace_client
        self.statement_exec = self.workspace_client.statement_execution

    def execute_query(self, warehouse_id: str, statement: str, 
                      disposition: sql.Disposition = sql.Disposition.EXTERNAL_LINKS,
                      wait_timeout: float = 300.0,  # seconds
                      wait_interval: float = 2.0,  # seconds
                      ) -> pd.DataFrame:
        """
        Executes a SQL statement and retrieves the results as a Pandas DataFrame.

        Args:
            statement (str): The SQL query to execute.
            disposition (sql.Disposition):  The result disposition (EXTERNAL_LINKS is default).

        Returns:
            pd.DataFrame: A Pandas DataFrame containing the query results.
                         Returns an empty DataFrame if no data is returned.

        Raises:
            RuntimeError: If there's an error executing the query or fetching data.
        """
        try:
            statement_response = self.statement_exec.execute_statement(
                warehouse_id=warehouse_id,
                statement=statement,
                disposition=disposition
            )

            statement_id = statement_response.statement_id

            status = self._wait_for_statement_completion(statement_id, wait_timeout, wait_interval)
            if self.is_query_failed(statement_id):
                raise RuntimeError(f"Query failed: {self.get_query_error_message(statement_id)}")

            # Get the final statement details after completion to ensure manifest is populated.
            final_statement_details = self.statement_exec.get_statement(statement_id)
            manifest = final_statement_details.manifest

            columns = [col.name for col in manifest.schema.columns]
            num_of_chunks = len(manifest.chunks)
            all_data = []
            for chunk in range(num_of_chunks):
                chunk_n = self.statement_exec.get_statement_result_chunk_n(statement_id, chunk)
                if chunk_n.external_links:  # Added check for external_links existence
                    external_link = chunk_n.external_links[0].external_link
                else:
                    # Handle the case where there are no external links for a chunk (might be empty)
                    continue  # Skip to the next chunk

                response = requests.get(
                    external_link,
                    stream=True,
                )
                response.raise_for_status()

                response.raw.decode_content = True
                chunk_df = pd.read_json(StringIO(response.text)).reset_index(drop=True)
                all_data.append(chunk_df)


            if all_data:
                df = pd.concat(all_data, ignore_index=True)
                df.columns = columns  # Set columns *after* concatenation
                return df
            else:
                return pd.DataFrame()  # Return an empty DataFrame if no data


        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"HTTP Request failed: {e}") from e
        except Exception as e:
            raise RuntimeError(f"An error occurred: {e}") from e

    def _wait_for_statement_completion(self, statement_id: str, timeout: float, interval: float) -> sql.Status:
        """Waits for a statement to complete (SUCCEEDED, FAILED, CLOSED, CANCELLED).

        Args:
            statement_id: The ID of the statement.
            timeout: Maximum wait time in seconds.
            interval: Time between status checks in seconds.

        Returns:
            sql.Status: The final status of the statement.

        Raises:
            RuntimeError: If the wait times out.
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            status = self.statement_exec.get_statement(statement_id).status
            if status.state in (sql.StatementState.SUCCEEDED, sql.StatementState.CANCELED, sql.StatementState.FAILED, sql.StatementState.CLOSED):
                return status
            logging.debug(f"Query status: {status.state}, waiting...")
            time.sleep(interval)
        raise RuntimeError(f"Query timed out after {timeout} seconds.")
    
    def is_query_failed(self, statement_id: str) -> bool:
        """
        Checks if a query has failed.

        Args:
            statement_id (str): The ID of the statement.

        Returns:
            bool: True if the query failed, False otherwise.
        """
        try:
            status = self.statement_exec.get_statement(statement_id).status
            return status.state in (sql.StatementState.CANCELED, sql.StatementState.FAILED, sql.StatementState.CLOSED)
        except Exception as e:
            raise RuntimeError(f"Error checking query status: {e}") from e

    def get_query_error_message(self, statement_id: str) -> Optional[str]:
        """Retrieves the error message from a failed query.

        Args:
            statement_id (str): The statement ID.

        Returns:
            Optional[str]: The error message, or None if the query didn't fail
                           or if no error message is available.
        """
        try:
            status = self.statement_exec.get_statement(statement_id).status
            return status.error.message if status.error else "Unknown error"
        except Exception as e:
            raise RuntimeError(f"Error getting error message: {e}")

    def create_warehouse(self, cluster_size: str = "Medium", auto_stop_mins: int = 60, max_num_clusters: int = 1,
                         enable_serverless_compute: bool = True) -> str:
        """
        Creates a SQL warehouse.

        Args:
            cluster_size: The size of the cluster.
            auto_stop_mins:  Minutes of inactivity before auto-stopping.
            enable_serverless_compute:  Whether to enable serverless compute.

        Returns:
            The ID of the created warehouse.

        Raises:
            RuntimeError: If warehouse creation fails.
        """
        try:
            w = self.workspace_client

            warehouse = w.warehouses.create(
                name=f"assessment-tool-{''.join(random.choice(string.ascii_letters) for i in range(6))}",
                cluster_size=cluster_size,
                auto_stop_mins=auto_stop_mins,
                enable_serverless_compute=enable_serverless_compute,
                max_num_clusters=max_num_clusters
            )

            return warehouse.id

        except Exception as e:
            raise RuntimeError(f"Error creating SQL warehouse: {e}") from e
        
    def delete_warehouse(self, warehouse_id: str) -> None:
        """
        Deletes a SQL warehouse.

        Args:
            warehouse_id (str): The ID of the warehouse to delete.

        Raises:
            RuntimeError: If warehouse deletion fails, or if no warehouse_id is provided
                          (and none is set on the instance).
        """

        try:
            self.workspace_client.warehouses.delete(id=warehouse_id)
        except Exception as e:
            raise RuntimeError(f"Error deleting warehouse '{warehouse_id}': {e}") from e