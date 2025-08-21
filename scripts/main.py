import argparse

# Custom module imports
from databricks_assessment import DatabricksAssessment
from helper import logger

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Databricks Assessment Tool - Data Fetcher")
    parser.add_argument("--customer", type=str, required=True, help="Specify the customer to process data for.")
    args = parser.parse_args()

    logger.info(f"Starting Databricks data fetch process for customer: {args.customer}")
    assessor = DatabricksAssessment(customer_name=args.customer)
    assessor.process_workspace_data()
    logger.info(f"Databricks data fetch process finished for customer: {args.customer}")

