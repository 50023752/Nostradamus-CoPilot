import os
import json
import re
import io
import pandas as pd
import pandas_gbq
from datetime import datetime, timezone # type: ignore
from google.cloud import bigquery # NEW IMPORT
from logger import logger
import config
from dotenv import load_dotenv
# Load .env
load_dotenv()

# ---------------------- BigQuery helper functions ----------------------
def to_bq(df: pd.DataFrame, project_id: str, dataset_id: str, table_id: str, if_exists: str = 'append'):
    """Writes a DataFrame to a BigQuery table."""
    table_full_id = f"{project_id}.{dataset_id}.{table_id}"
    try:
        pandas_gbq.to_gbq(df, destination_table=table_full_id, project_id=project_id, if_exists=if_exists)
        logger.info(f"Data written to {table_full_id} successfully with shape {df.shape}.")
    except Exception as e:
        logger.error(f"Failed to write to BigQuery table {table_full_id}: {e}")
        raise

def log_to_bq(user_id:str, user_query: str, answer: str, status: str = "success", user_feedback: str = None, error_message: str = None, interaction_id: str = None):
    """
    Constructs a log entry and writes it to BigQuery.
    If user_feedback is provided and interaction_id is present, it attempts to update an existing row.
    Otherwise, it inserts a new row.
    """
    try:
        table_full_id = f"{config.PROJECT_ID}.{config.DATASET_ID_DUMP}.{config.TABLE_ID_DUMP}"
        bq_client = bigquery.Client(project=config.PROJECT_ID)

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
            query_job.result()  # Wait for the job to complete
            logger.info(f"Feedback updated for interaction_id {interaction_id} in {table_full_id}.")
        else:
            # Insert a new row for initial log or error
            data = {
                "user": user_id,
                "time": datetime.now(timezone.utc),
                "user_query": user_query,
                "model_answer": answer,
                "status": status,
                "error_message": error_message,
                "user_feedback": user_feedback,  # This will be None for initial logs
            }
            if interaction_id:  # Add interaction_id to the data if provided
                data["interaction_id"] = interaction_id

            df = pd.DataFrame([data])
            to_bq(df, project_id=config.PROJECT_ID, dataset_id=config.DATASET_ID_DUMP, table_id=config.TABLE_ID_DUMP)
    except Exception as e:
        logger.error(f"Failed to log to BigQuery: {e}")


def load_system_prompt(file_path: str) -> str:
    """Loads the system prompt from a text file."""
    try:
        with open(file_path, "r") as f:
            return f.read()
    except FileNotFoundError:
        logger.error(f"System prompt file not found at: {file_path}")
        return "" # Return empty string if file not found


def parse_tool_response(response_text: str) -> dict:
    """
    Parses the multi-JSON string returned from the Toolbox/LLM.

    Returns a dictionary:
    {
        "SQL Generated": str,
        "Markdown Table": str,
        "x_axis": str,
        "y_axes": list
    }
    """
    
    result =  {
        "SQL Generated": "",
        "Answer": "",
        "Chart name" : "",
        "x_axis": None,
        "y_axes": [],
        "Markdown Table": "" # Retaining for compatibility if used elsewhere
    }

    if not response_text:
        return result

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

    # result = {"SQL Generated": "", "Answer": ""}
    for item in parsed_objects:
        if "SQL Generated" in item:
            logger.debug(f'SQL = {item["SQL Generated"]}')
            result["SQL Generated"] = item["SQL Generated"]
        elif "Answer" in item:
            logger.debug(f'Answer = {item["Answer"]}')
            answer = item["Answer"]
            result["Answer"] = answer # Store the full answer

            # # Attempt to parse chart info if present
            # if "\n\n" in answer:
            #     tables = answer.split("\n\n")
            #     table_1 = tables[0].strip() # Query results table
            #     result["Answer"] = table_1 # Overwrite with just the main table if split is successful

            #     if len(tables) > 1:
            #         table_2 = tables[1].strip() # Chart info table
            #         try:
            #             # Extract chart values from the second markdown table
            #             chart_lines = table_2.splitlines()
            #             if len(chart_lines) > 2:
            #                 # The third line contains the values
            #                 values_line = chart_lines[2]
            #                 values = [v.strip() for v in values_line.split('|')[1:-1]]
            #                 # Clean the values to remove any markdown artifacts
            #                 result['Chart name'] = values[0].strip(':').strip('-').strip()
            #                 result['x_axis'] = values[1].strip(':').strip('-').strip()
            #                 result['y_axes'] = values[2].strip(':').strip('-').strip()
            #         except (IndexError, ValueError) as e:
            #             logger.warning(f"Could not parse chart info from response: {e}")

            # Split into parts if '\n\n' present
            if "\n\n" in answer:
                tables = [t.strip() for t in answer.split("\n\n") if t.strip()]
                num_tables = len(tables)
                logger.debug(f"Detected {num_tables} markdown sections in Answer.")

                # Case 1️⃣: Only one section
                if num_tables == 1:
                    table_1 = tables[0]
                    result["Answer"] = table_1
                    logger.debug("Single table detected → stored as Answer only.")

                # Case 2️⃣: Two sections → first = Answer, second = chart info
                elif num_tables == 2:
                    table_1, table_2 = tables
                    result["Answer"] = table_1
                    logger.debug("Two tables detected → first = Answer, second = chart info.")
                    
                    try:
                        # Only parse if it looks like a markdown table
                        if table_2.strip().startswith("|"):
                            chart_lines = table_2.splitlines()
                            if len(chart_lines) > 2:
                                values_line = chart_lines[2]
                                values = [v.strip() for v in values_line.split('|')[1:-1]]
                                result['Chart name'] = values[0].strip(':').strip('-').strip()
                                result['x_axis'] = values[1].strip(':').strip('-').strip()
                                result['y_axes'] = values[2].strip(':').strip('-').strip()
                                logger.debug(f"Chart parsed → Name: {result['Chart name']}, X: {result['x_axis']}, Y: {result['y_axes']}")
                        else:
                            logger.warning("Chart info table found but not enough lines to parse.")
                    except Exception as e:
                        logger.warning(f"Could not parse chart info from response: {e}")

                # Case 3️⃣: More than 2 sections → all except last = answer, last = chart info
                elif num_tables > 2:
                    table_1 = "\n\n".join(tables[:-1])
                    table_2 = tables[-1]
                    result["Answer"] = table_1
                    logger.debug(f"Multiple ({num_tables}) tables detected → merged all except last as Answer, parsing last for chart info.")
                    
                    try:
                        # Only parse if it looks like a markdown table
                        if table_2.strip().startswith("|"):
                            chart_lines = table_2.splitlines()
                            if len(chart_lines) > 2:
                                values_line = chart_lines[2]
                                values = [v.strip() for v in values_line.split('|')[1:-1]]
                                result['Chart name'] = values[0].strip(':').strip('-').strip()
                                result['x_axis'] = values[1].strip(':').strip('-').strip()
                                result['y_axes'] = values[2].strip(':').strip('-').strip()
                                logger.debug(f"Chart parsed → Name: {result['Chart name']}, X: {result['x_axis']}, Y: {result['y_axes']}")
                        else:
                            logger.warning("Chart info table found but not enough lines to parse.")
                    except Exception as e:
                        logger.warning(f"Could not parse chart info from response: {e}")

            else:
                logger.debug("No '\\n\\n' found in Answer — stored as plain text.")


    return result

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

import re

def clean_table_alignment_row(table_text: str) -> str:
    """Removes alignment row (|:---|---:|:-:|) from markdown table."""
    lines = table_text.strip().splitlines()
    if len(lines) > 1 and re.match(r"^\|[:\-| ]+\|$", lines[1].strip()):
        del lines[1]
    return "\n".join(lines)

    
def fix_incomplete_markdown_table(table_text: str) -> str:
    """
    Detects and fixes tables missing headers or alignment rows.
    Ensures the table starts with a header and has consistent columns.
    """
    lines = [l.strip() for l in table_text.strip().splitlines() if l.strip()]
    
    # Ignore if no '|' present
    if not any('|' in l for l in lines):
        return table_text

    # If first line doesn’t look like a header (e.g. starts with '| | 2025...')
    # we’ll generate placeholder headers
    first_line = lines[0]
    cols = [c.strip() for c in first_line.split('|') if c.strip()]
    
    if not re.match(r'^[A-Za-z]', cols[0]):  # header missing
        num_cols = len(cols)
        headers = [f"Col_{i+1}" for i in range(num_cols)]
        header_row = "| " + " | ".join(headers) + " |"
        align_row = "| " + " | ".join(["---"] * num_cols) + " |"
        lines.insert(0, align_row)
        lines.insert(0, header_row)
    
    # Ensure at least header + alignment + data rows
    return "\n".join(lines)

def format_axis_title(title: str) -> str:
    """Formats a string by replacing underscores with spaces and capitalizing the first letter."""
    if not isinstance(title, str):
        return ""
    return title.replace('_', ' ').capitalize()


def format_single_line_table(text: str) -> str:
    """
    Finds a single-line markdown table and formats it into a multi-line table.
    Example: | a | b | | c | d | -> | a | b |\n| c | d |
    """
    # This regex finds multiple pipe-enclosed groups on the same line
    return re.sub(r'(\s*\|.*?\|\s*)\|', r'\1\n|', text)

def extract_markdown_table(text: str):
    """
    Extracts the first markdown table (including alignment rows) from text.
    Returns tuple: (before_text, table_text, after_text)
    """
    # Pattern handles tables like:
    # | A | B | C |
    # |:--|--:|:-:|
    # | 1 | 2 | 3 |
    pattern = r"(\|[^\n]+\|\s*\n\|[:\-| ]+\|\s*\n(?:\|[^\n]+\|\s*\n*)+)"
    match = re.search(pattern, text)
    if match:
        before = text[:match.start()].strip()
        table = match.group(1).strip()
        after = text[match.end():].strip()
        return before, table, after
    return text, "", ""