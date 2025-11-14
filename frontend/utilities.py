import streamlit as st
import requests
import os
import ast
from datetime import datetime, timezone
import json
from dotenv import load_dotenv
from google.cloud import bigquery
import pandas as pd
import io
import plotly.express as px
from logger import get_logger
logger = get_logger()
logger.info("Frontend started successfully.")

# For saving the user questions and answers
DATASET_ID_DUMP = 'aiml_cj'
TABLE_ID_DUMP = 'aiml_cj_nost_copilot_dump'

# --- BigQuery Logging ---
def log_to_bq(user_id: str, user_query: str, answer: str, project_id: str, status: str = "success", user_feedback: str = None, error_message: str = None, interaction_id: str = None):
    """Logs user interactions to BigQuery."""
    try:
        table_full_id = f"{project_id}.{DATASET_ID_DUMP}.{TABLE_ID_DUMP}"  # Replace with your actual table ID
        bq_client = bigquery.Client(project=project_id)

        # Prepare the row data
        row = {
            "user": user_id,
            "time": datetime.now(timezone.utc).isoformat(),
            "user_query": user_query,
            "model_answer": answer,
            "status": status,
            "error_message": error_message,
            "user_feedback": user_feedback,
            "interaction_id": interaction_id
        }

        # Convert row to a format BigQuery can ingest
        rows_to_insert = [row]

        # Make API request to insert rows
        errors = bq_client.insert_rows_json(table_full_id, rows_to_insert)
        if errors:
            logger.error(f"Encountered errors while inserting rows: {errors}")
        else:
            logger.info(f"Logged interaction to BigQuery for interaction_id {interaction_id}")

    except Exception as e:
        logger.error(f"Failed to log to BigQuery: {e}")

# --- Helper functions from Nostradamus-CoPilot/frontend/utils.py ---
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

def extract_markdown_table(text: str):
    """
    Extracts the first markdown table (including alignment rows) from text.
    Returns tuple: (before_text, table_text, after_text)
    """
    import re
    pattern = r"(\|[^\n]+\|\s*\n\|[:\-| ]+\|\s*\n(?:\|[^\n]+\|\s*\n*)+)"
    match = re.search(pattern, text)
    if match:
        before = text[:match.start()].strip()
        table = match.group(1).strip()
        after = text[match.end():].strip()
        return before, table, after
    return text, "", ""

def format_axis_title(title: str) -> str:
    """Formats a string by replacing underscores with spaces and capitalizing the first letter."""
    if not isinstance(title, str):
        return ""
    return title.replace('_', ' ').capitalize()
