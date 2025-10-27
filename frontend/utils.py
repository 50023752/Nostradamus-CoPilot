import os
import json
import re
import io
import pandas as pd
import pandas_gbq
import chainlit as cl
from datetime import datetime # type: ignore
from google.cloud import bigquery # NEW IMPORT
from logger import logger
import config


def load_system_prompt(file_path: str) -> str:
    """Loads the system prompt from a text file."""
    try:
        with open(file_path, "r") as f:
            return f.read()
    except FileNotFoundError:
        logger.error(f"System prompt file not found at: {file_path}")
        return "" # Return empty string if file not found


def parse_tool_response(response_text: str) -> dict:
    """Extracts SQL and Answer blocks from a multi-JSON string."""
    if not response_text:
        return {"SQL Generated": "", "Answer": ""}

    try:
        actual_content_string = json.loads(response_text)
    except (json.JSONDecodeError, TypeError):
        actual_content_string = response_text

    json_blocks = re.findall(r'\{.*?\}', str(actual_content_string), flags=re.S)
    parsed_objects = []
    for block in json_blocks:
        try:
            parsed_objects.append(json.loads(block))
        except json.JSONDecodeError:
            pass

    result = {"SQL Generated": "", "Answer": ""}
    for item in parsed_objects:
        if "SQL Generated" in item:
            result["SQL Generated"] = item["SQL Generated"]
        elif "Answer" in item:
            result["Answer"] = item["Answer"]

    return result


def to_bq(df: pd.DataFrame, project_id: str, dataset_id: str, table_id: str, if_exists: str = 'append'):
    """Writes a DataFrame to a BigQuery table."""
    table_full_id = f"{project_id}.{dataset_id}.{table_id}"
    try:
        pandas_gbq.to_gbq(df, destination_table=table_full_id, project_id=project_id, if_exists=if_exists)
        logger.info(f"Data written to {table_full_id} successfully with shape {df.shape}.")
    except Exception as e:
        logger.error(f"Failed to write to BigQuery table {table_full_id}: {e}")
        raise



def log_to_bq(user_query: str, answer: str, status: str = "success", user_feedback: str = None, error_message: str = None, interaction_id: str = None):
    """
    Constructs a log entry and writes it to BigQuery.
    If user_feedback is provided and interaction_id is present, it attempts to update an existing row.
    Otherwise, it inserts a new row.
    """
    try:
        user = cl.user_session.get("user")
        user_id = user.identifier if user else "anonymous"
        table_full_id = f"{config.PROJECT_ID}.{config.DATASET_ID_DUMP}.{config.TABLE_ID_DUMP}"
        bq_client = bigquery.Client(project=config.PROJECT_ID)

        df = pd.DataFrame([{
            "user": user_id,
            "time": datetime.utcnow(),
            "user_query": user_query,
            "model_answer": answer,
            "status": status,
            "error_message": error_message,
            "user_feedback": user_feedback,
        }])

        if user_feedback is not None and interaction_id is not None:
            # Attempt to update an existing row for feedback
            update_query = f"""
            UPDATE `{table_full_id}`
            SET user_feedback = @user_feedback
            WHERE interaction_id = @interaction_id
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("user_feedback", "STRING", user_feedback),
                    bigquery.ScalarQueryParameter("interaction_id", "STRING", interaction_id),
                ]
            )
            query_job = bq_client.query(update_query, job_config=job_config)
            query_job.result() # Wait for the job to complete
            logger.info(f"Feedback updated for interaction_id {interaction_id} in {table_full_id}.")
        else:
            # Insert a new row for initial log or error
            user = cl.user_session.get("user")
            user_id = user.identifier if user else "anonymous"
            
            data = {
                "user": user_id,
                "time": datetime.utcnow(),
                "user_query": user_query,
                "model_answer": answer,
                "status": status,
                "error_message": error_message,
                "user_feedback": user_feedback, # This will be None for initial logs
            }
            if interaction_id: # Add interaction_id to the data if provided
                data["interaction_id"] = interaction_id

            df = pd.DataFrame([data])

            to_bq(df, project_id=config.PROJECT_ID, dataset_id=config.DATASET_ID_DUMP, table_id=config.TABLE_ID_DUMP)
    except Exception as e:
        logger.error(f"Failed to log to BigQuery: {e}")


def markdown_table_to_df(markdown_text: str) -> pd.DataFrame:
    """Converts a Markdown table string to a pandas DataFrame."""
    lines = [line.strip() for line in markdown_text.splitlines() if line.strip() and line.startswith("|")]
    if len(lines) < 2: # Header and separator are minimum
        raise ValueError("Markdown text does not contain a valid table.")

    headers = [h.strip() for h in lines[0].strip("|").split("|")]
    
    data_rows = []
    for row_str in lines[2:]: # Skip header and separator
        cells = [c.strip() for c in row_str.strip("|").split("|")]
        if len(cells) == len(headers):
            data_rows.append(cells)

    df = pd.DataFrame(data_rows, columns=headers)
    return df