from google.cloud import bigquery
from google.cloud import storage
import argparse


def get_workspace_ids(bucket_name, prefix=""):
    """
    Automatically fetch workspace IDs from the top level of the bucket

    Args:
        bucket_name (str): GCS bucket name
        prefix (str): Optional prefix/folder path in the bucket (Usually the date the extract script was executed in YYYY-MM-DD)

    Returns:
        list: List of workspace IDs
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    folders = set()  # Use a set to avoid duplicates

    blobs = bucket.list_blobs(prefix=prefix)  # important delimiter!

    for blob in blobs:
        folders.add(blob.name.split("/")[1])  # remove the trailing slash

    return list(folders)


def create_bigquery_tables(project_id, dataset_id, bucket_name, metrics, bucket_prefix=""):
    """
    Creates BigQuery tables from Databricks metadata stored in GCS without downloading locally

    Args:
        project_id (str): Google Cloud project ID
        dataset_id (str): BigQuery dataset ID
        bucket_name (str): GCS bucket name
        metrics (list): List of metric names
        bucket_prefix (str): Optional prefix/folder path in the bucket
    """

    # Initialize clients
    bq_client = bigquery.Client(project=project_id)
    storage_client = storage.Client(project=project_id)

    # Get workspace IDs automatically
    workspace_ids = get_workspace_ids(bucket_name, bucket_prefix)
    if not workspace_ids:
        print(f"No workspace IDs found in gs://{bucket_name}/{bucket_prefix}")
        return

    print(f"Found workspace IDs: {workspace_ids}")

    # Create dataset if it doesn't exist
    dataset_ref = bq_client.dataset(dataset_id)
    dataset = bigquery.Dataset(dataset_ref)
    try:
        bq_client.get_dataset(dataset_ref)
    except Exception as e:
        bq_client.create_dataset(dataset)
        print(f"Created dataset {project_id}.{dataset_id}, {e}")

    # Process each metric
    for metric in metrics:
        table_id = f"{project_id}.{dataset_id}.{metric}"
        uris = []

        # Collect all GCS URIs for this metric
        for workspace_id in workspace_ids:
            prefix = f"{bucket_prefix}/{workspace_id}/{metric}/" if bucket_prefix else f"{workspace_id}/{metric}/"
            bucket = storage_client.get_bucket(bucket_name)
            blobs = bucket.list_blobs(prefix=prefix)

            for blob in blobs:
                if blob.name.endswith('.json'):
                    uri = f"gs://{bucket_name}/{blob.name}"
                    uris.append(uri)

        if not uris:
            print(f"No files found for {metric}")
            continue

        # Create table configuration
        table = bigquery.Table(table_id)

        # Create or update table
        try:
            bq_client.get_table(table_id)
            print(f"Table {table_id} already exists")
        except Exception as e:
            bq_client.create_table(table)
            print(f"Created table {table_id}, {e}")

        # Load data directly from GCS
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        load_job = bq_client.load_table_from_uri(
            uris,
            table_id,
            job_config=job_config
        )

        load_job.result()  # Wait for the job to complete
        destination_table = bq_client.get_table(table_id)
        print(f"Loaded {destination_table.num_rows} rows into {table_id}")


def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Load Databricks metadata from GCS to BigQuery")
    parser.add_argument("--project", required=True, help="Google Cloud project ID")
    parser.add_argument("--dataset", required=True, help="BigQuery dataset ID")
    parser.add_argument("--bucket", required=True, help="GCS bucket name")
    parser.add_argument("--prefix", default="", help="Bucket prefix (Usually the date the extract script was executed in YYYY-MM-DD)")

    # Parse arguments
    args = parser.parse_args()

    # List of metrics (still hardcoded, but could be made an argument too)
    METRICS = [
        "catalogs",
        "clusters",
        "cluster_libraries",
        "clusters_activity_events",
        "cpu_usage",
        "dbu_usage_annual",
        "dbu_usage_category",
        "delta_live_pipelines",
        "external_locations",
        "job_runs",
        "job_tasks",
        "jobs",
        "least_used_tables",
        "memory_usage",
        "metastores",
        "ml_experiments",
        "nodes",
        "notebooks_extract",
        "query_history",
        "registered_models",
        "routines",
        "schemas",
        "serving_endpoints",
        "sql_alerts",
        "sql_queries",
        "sql_warehouses",
        "tables",
        "usage_summary",
        "views",
        "volumes",
        "warehouses"
    ]

    # Execute the table creation and data loading
    create_bigquery_tables(
        args.project,
        args.dataset,
        args.bucket,
        METRICS,
        args.prefix
    )


if __name__ == "__main__":
    # Set up Google Cloud credentials using env variable
    main()
