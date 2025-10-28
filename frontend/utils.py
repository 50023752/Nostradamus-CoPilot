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
            print(f'SQL = {item["SQL Generated"]}')
            result["SQL Generated"] = item["SQL Generated"]
        elif "Answer" in item:
            print(f'Answer = {item["Answer"]}')
            result["Answer"] = item["Answer"]

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
