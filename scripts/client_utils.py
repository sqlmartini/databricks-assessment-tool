from typing import Union
from databricks.sdk import WorkspaceClient
# Assuming 'logger' is initialized in your 'helper.py' module
from helper import logger

def get_databricks_client(host: str = None, token: str = None) -> Union[WorkspaceClient, None]:
    """
     Creates and returns a Databricks WorkspaceClient, using explicit or default config.
    """
    try:
        if host and token:
            logger.debug(f"Initializing Databricks client with provided host: {host}")
            return WorkspaceClient(host=host, token=token)
        else:
            logger.debug("Initializing Databricks client using default configuration.")
            client = WorkspaceClient()
            if not client.config.host:
                logger.error("Databricks host/token not found in environment variables or config file.")
                return None
            logger.debug(f"Databricks client initialized for host: {client.config.host}")
            return client
    except Exception as e:
        logger.error(f"Error creating Databricks client: {e}", exc_info=True)
        return None

