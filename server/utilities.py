import json
import numpy as np
import pandas as pd
from decimal import Decimal

from server.logger import get_logger
logger = get_logger()
import re

def clean_sql_fences(sql: str) -> str:
    # Remove markdown fences and strip whitespace
    cleaned_sql = sql.replace("```sql", "").replace("```", "").strip()
    # Remove "googleSQL" prefix if it exists (case-insensitive)
    if cleaned_sql.lower().startswith("googlesql"):
        cleaned_sql = cleaned_sql[len("googlesql"):].strip()
    if cleaned_sql.lower().startswith("google-sql"):
        cleaned_sql = cleaned_sql[len("google-sql"):].strip()
    return cleaned_sql

def check_casual(question):
    casual_keywords = ["hi", "hello", "hey", "how are you", "good morning", "good afternoon"]
    question_lower = question.lower()
    
    # Match full words only
    for word in casual_keywords:
        # Add word boundaries for single-word keywords
        if re.search(rf"\b{re.escape(word)}\b", question_lower):
            return True
    return False

def load_system_prompt(file_path: str) -> str:
    """Loads the system prompt from a text file."""
    try:
        with open(file_path, "r") as f:
            return f.read()
    except FileNotFoundError:
        logger.error(f"System prompt file not found at: {file_path}")
        return "" # Return empty string if file not found

def sanitize_value(v):
    """Convert one value to a JSON-serializable native Python value."""
    # pandas NA
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass

    # pandas / numpy scalars
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        # convert 5.0 -> 5 (int) if exact integer, else keep float
        fv = float(v)
        return int(fv) if fv.is_integer() else fv
    if isinstance(v, (np.bool_, bool)):
        return bool(v)
    if isinstance(v, (np.ndarray,)):
        return [sanitize_value(x) for x in v.tolist()]

    # Decimal
    if isinstance(v, Decimal):
        fv = float(v)
        return int(fv) if fv.is_integer() else fv

    # Datetime-like -> ISO
    try:
        import datetime
        if isinstance(v, (datetime.datetime, datetime.date, datetime.time)):
            return v.isoformat()
    except Exception:
        pass

    # pandas Timestamp
    if isinstance(v, pd.Timestamp):
        return v.isoformat()

    # DataFrame / Series
    if isinstance(v, pd.DataFrame):
        # replace nan with None then convert to list-of-dicts
        df = v.where(pd.notnull(v), None).copy()
        return [ {k: sanitize_value(val) for k,val in row.items()} for row in df.to_dict(orient="records") ]
    if isinstance(v, pd.Series):
        return sanitize_value(v.to_dict())

    # dict / list recursion
    if isinstance(v, dict):
        return {str(k): sanitize_value(val) for k, val in v.items()}
    if isinstance(v, list) or isinstance(v, tuple):
        return [sanitize_value(x) for x in v]

    # Basic python types: int, float, str already OK
    if isinstance(v, (int, float, str)):
        # convert float that is mathematically integer to int
        if isinstance(v, float) and float(v).is_integer():
            return int(v)
        return v

    # fallback: convert to string (safe)
    try:
        return json.loads(json.dumps(v))
    except Exception:
        return str(v)


def sanitize_payload(obj):
    """Recursively sanitize a payload to plain JSON-safe Python objects."""
    return sanitize_value(obj)
