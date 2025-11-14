"""
Configuration file for the Nostradamus CoPilot frontend.

This file contains all the necessary configuration variables, such as file paths,
API endpoints, and Google Cloud project details.

Please replace the placeholder values with your actual configuration.
"""
import os

# --- GEMINI MODEL USED ---
LLM_MODEL = "gemini-2.5-flash"

# --- Google Cloud BigQuery Configuration ---
PROJECT_ID = 'analytics-datapipeline-prod'
LOCATION = "asia-south1"

# Dataset used for the realtime model
TABLE_ID_RT = "TW_NOSTD_MART_REALTIME_UPDATED"

# Dataset used for historic data
TABLE_ID_HIST = "TW_NOSTD_MART_HIST"

DATASET_ID = "aiml_cj_nostd_mart"
TABLE_IDS = [
    "TW_NOSTD_MART_REALTIME_UPDATED", 
    "TW_NOSTD_MART_HIST"
    ]

DISCRIPTIONS = [
    "2-wheeler most recent, up-to-date snapshot of portfolio performance. Provides the latest available data (yesterday's performance)",
    "2-wheeler portfolio loan historic data. Covers all completed months, from April 2024 to the end of last month."
    ]