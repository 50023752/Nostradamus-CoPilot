# server/bigquery_client.py
from google.cloud import bigquery
import os
from server.logger import get_logger
from server.config import PROJECT_ID, LOCATION

logger = get_logger()

# Initialize the BigQuery client
try:
    client = bigquery.Client(project=PROJECT_ID, location=LOCATION)
    logger.info(f"✅ Connected to BigQuery: {PROJECT_ID} ({LOCATION})")
except Exception as e:
    logger.error(f"❌ Failed to initialize BigQuery client: {e}")
    client = None


def get_schema_and_sample(TABLE_NAME: str | None = None, limit: int = 5):
    try:
        if not client:
            raise RuntimeError("BigQuery client not initialized")

        table = client.get_table(TABLE_NAME)

        schema_info = "Column Name | Type | Description\n"
        schema_info += "-" * 60 + "\n"
        for field in table.schema:
            desc = field.description if field.description else "N/A"
            schema_info += f"{field.name} | {field.field_type} | {desc}\n"

        query = f"SELECT * FROM `{TABLE_NAME}` LIMIT {limit}"
        df = client.query(query).result().to_dataframe()

        result = {
            "schema": schema_info,
            "sample_rows": df.to_dict(orient="records")
        }
        logger.debug(f"Schema and Sample: {result}")
        return result
    except Exception as e:
        logger.error(f"Error fetching schema/sample: {e}", exc_info=True)
        raise


def run_query(query: str):
    try:
        if not client:
            raise RuntimeError("BigQuery client not initialized")

        job = client.query(query)
        arrow_table = job.result().to_arrow()
        return arrow_table.to_pylist()
    except Exception as e:
        logger.error(f"❌ BigQuery error: {e}", exc_info=True)
        raise
