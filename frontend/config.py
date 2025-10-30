"""
Configuration file for the Nostradamus CoPilot frontend.

This file contains all the necessary configuration variables, such as file paths,
API endpoints, and Google Cloud project details.

Please replace the placeholder values with your actual configuration.
"""
import os 

# --- System Prompt Configuration ---
# Path to the file containing the system prompt instructions for the model.
SYSTEM_PROMPT_FILE = "system_prompt.txt"

# --- Toolbox Configuration ---
TOOLBOX_URL = os.getenv("TOOLBOX_URL", "http://127.0.0.1:5000")
TOOLSET_NAME = "my-toolset"

# --- Google Cloud BigQuery Configuration ---
PROJECT_ID = 'analytics-datapipeline-prod'

# For saving the user questions and answers
DATASET_ID_DUMP = 'aiml_cj'
TABLE_ID_DUMP = 'aiml_cj_nost_copilot_dump'

# Dataset used for the realtime model
DATASET_ID_RT = "aiml_cj_nostd_mart"
TABLE_ID_RT = "TW_NOSTD_MART_REALTIME_UPDATED"

# Dataset used for historic data
DATASET_ID_HIST = "aiml_cj_nostd_mart"
TABLE_ID_HIST = "TW_NOSTD_MART_HIST"