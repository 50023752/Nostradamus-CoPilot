from logger import logger
import os
import json
import io
import pandas as pd
import pandas_gbq
import streamlit as st
from toolbox_core import ToolboxClient  # type: ignore
import config
from utils import * #parse_tool_response, markdown_table_to_df, load_system_prompt
import asyncio
import nest_asyncio
import threading
from google.cloud import bigquery
from datetime import datetime, timezone
from dotenv import load_dotenv
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)

# Load .env
load_dotenv()

# Allow nested event loops for Streamlit environment
nest_asyncio.apply()

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

def log_to_bq(user_query: str, answer: str, status: str = "success", user_feedback: str = None, error_message: str = None, interaction_id: str = None):
    """
    Constructs a log entry and writes it to BigQuery.
    If user_feedback is provided and interaction_id is present, it attempts to update an existing row.
    Otherwise, it inserts a new row.
    """
    try:
        # Get user id from Streamlit session (fallback to anonymous)
        user_id = st.session_state.get("username", "anonymous")

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

# ---------------------- Async helpers and background loop caching ----------------------
# Background event loop + thread to run toolbox coroutines reliably across interactions
_bg_loop = None
_bg_loop_thread = None

# Cached objects that live in the background loop context
_bg_client = None
_bg_toolset = None

def ensure_background_loop():
    """Ensure a background asyncio event loop is running in a dedicated thread."""
    global _bg_loop, _bg_loop_thread
    if _bg_loop and _bg_loop.is_running():
        return _bg_loop
    _bg_loop = asyncio.new_event_loop()
    def _run_loop(loop):
        asyncio.set_event_loop(loop)
        loop.run_forever()
    _bg_loop_thread = threading.Thread(target=_run_loop, args=(_bg_loop,), daemon=True)
    _bg_loop_thread.start()
    return _bg_loop

def run_async_sync(coro):
    """
    Run coroutine in the dedicated background loop thread and return the result.
    Uses asyncio.run_coroutine_threadsafe so tasks run in a proper task context for aiohttp.
    """
    loop = ensure_background_loop()
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    return future.result()

# Async initializer that will create and cache the client & toolset inside the background loop.
async def _ensure_client_and_toolset(toolset_name: str):
    global _bg_client, _bg_toolset
    if _bg_client is None or _bg_toolset is None:
        # create the client inside this async context so aiohttp uses the same loop/task
        _bg_client = ToolboxClient(config.TOOLBOX_URL)
        _bg_toolset = await _bg_client.load_toolset(toolset_name)
    return _bg_client, _bg_toolset

def get_toolbox_and_toolset_for_query(toolset_name: str):
    """
    Return a cached ToolboxClient and toolset. If they don't exist yet, create them inside the
    background loop. This avoids recreating client/toolset on every query while ensuring correct loop context.
    """
    return run_async_sync(_ensure_client_and_toolset(toolset_name))

# ---------------------- System prompt (same as Chainlit startup) ----------------------
SYSTEM_INSTRUCTION = load_system_prompt(config.SYSTEM_PROMPT_FILE)

WELCOME_MESSAGE = """***Welcome to Orion - The Nostradamus Copilot***

I'm ready to answer your questions about the Two Wheeler Data.
Type your question below to get started! \n
Example questions:
- What is the growth rate in TW disbursement of Pan India in the last 6 months?
- What is the GNS for 1st month region wise percentage and count?
- Which region has the highest slippage of customers from 0 dpd last year to 30+ dpd ?
- Could you tell me the split of high, medium and risky customers count and percentage according to the early warning score model?
"""

# ---------------------- AUTHENTICATION ----------------------
def authenticate():
    st.sidebar.title("🔐 Login")

    # If already logged in
    if st.session_state.get("authenticated"):
        st.sidebar.success(f"Welcome, {st.session_state.get('username')}!")
        return True

    username = st.sidebar.text_input("Username", key="auth_user")
    password = st.sidebar.text_input("Password", type="password", key="auth_pw")

    creds_str = os.getenv("USER_CREDENTIALS")

    if st.sidebar.button("Login", key="auth_login"):
        if creds_str:
            try:
                user_credentials = json.loads(creds_str)
                print(f'USER_CREDENTIALS: {user_credentials}')
                if not isinstance(user_credentials, list):
                    st.sidebar.error("USER_CREDENTIALS must be a JSON list of {username,password} objects.")
                    return False
                for cred in user_credentials:
                    if cred.get("username") == username and cred.get("password") == password:
                        st.session_state["authenticated"] = True
                        st.session_state["username"] = username
                        st.sidebar.success(f"Welcome, {username}!")
                        return True
            except json.JSONDecodeError:
                st.sidebar.error("Invalid USER_CREDENTIALS JSON format.")
                return False
        st.sidebar.error(f"Invalid credentials. - User: {username}  Password: {password}")
        return False

    return False

# def extract_final_answer(answer_text: str) -> str:
#     """
#     Extracts the final 'Answer' block containing markdown table and reasoning
#     from a messy multi-JSON string returned by the model.
#     """
#     # Regex to find the last "Answer": " ... " pattern
#     match = re.findall(r'"Answer"\s*:\s*"(.+?)"', answer_text, flags=re.DOTALL)
#     if match:
#         clean_text = match[-1]  # take the last 'Answer' field
#         # Unescape any escaped newlines or quotes
#         clean_text = clean_text.replace('\\"', '"').replace('\\n', '\n')
#         return clean_text.strip()
#     return answer_text.strip()

# ---------------------- MAIN APP ----------------------
def format_single_line_table(text: str) -> str:
    """
    Finds a single-line markdown table and formats it into a multi-line table.
    Example: | a | b | | c | d | -> | a | b |\n| c | d |
    """
    # This regex finds multiple pipe-enclosed groups on the same line
    return re.sub(r'(\s*\|.*?\|\s*)\|', r'\1\n|', text)


def main():
    st.set_page_config(page_title="Orion Copilot", page_icon="🪐", layout="wide")

    # --- Initialize session state containers ---
    # This needs to be at the top before any other st call that might use it
    if "history" not in st.session_state:
        st.session_state["history"] = []

    # --- Sidebar: History ---
    with st.sidebar:
        st.title("Chat History")
        if st.session_state.get("history"):
            # Show history in reverse order (most recent first)
            for i, (q, a) in enumerate(reversed(st.session_state.history)):
                with st.expander(f"**{len(st.session_state.history) - i}**: {q[:40]}"):
                    st.markdown(f"**You:** {q}")
                    st.markdown(f"**Orion:**\n{a}")
        else:
            st.info("Your chat history will appear here.")

    st.title("The Orion - The Nostradamus Copilot")

    # Show the welcome message exactly as in Chainlit startup
    st.markdown(WELCOME_MESSAGE)

    if "last_answer" not in st.session_state:
        st.session_state["last_answer"] = ""
    if "csv_bytes" not in st.session_state:
        st.session_state["csv_bytes"] = None
    # Keep client/toolset None in session (we create/cache inside background loop)
    st.session_state.setdefault("client", None)
    st.session_state.setdefault("toolset", None)

    # Input area
    col1, col2 = st.columns([5, 1])
    with col1:
        user_query = st.text_area("**Ask your question:**", value="", height=68, key="user_query")
    with col2:
        # Add empty lines to push the button down, aligning it with the text area input field
        st.text("")
        st.text("")
        submit_clicked = st.button("Submit Query", key="submit_query")
    if submit_clicked:
        st.session_state["trigger_run"] = True
    
    if st.session_state.get("trigger_run"):
        if not user_query.strip():
            st.warning("Please enter a question.")
            st.session_state["trigger_run"] = False
        else:
            with st.spinner("Processing your query..."):
                history = st.session_state["history"]

                # Build contextual query
                context_parts = []
                for q, a in history:
                    context_parts.append(f"Previous Question: {q}")
                    context_parts.append(f"Previous Answer: {a}")
                context_parts.append(f"Current Question: {user_query}")
                full_query_with_context = f"{SYSTEM_INSTRUCTION}\n\n" + "\n\n".join(context_parts)

                try:
                    # Create/get Toolbox client & toolset inside the background loop (cached there)
                    client, toolset_list = get_toolbox_and_toolset_for_query(config.TOOLSET_NAME)
                    st.session_state["client"] = client
                    st.session_state["toolset"] = toolset_list

                    ask_data_insights_tool = toolset_list[0]

                    tables_to_use = [{
                        "projectId": config.PROJECT_ID,
                        "datasetId": config.DATASET_ID_1,
                        "tableId": config.TABLE_ID_1
                    }]

                    temperature = 0.0
                    final_query = f"Temperature setting: {temperature}\n\n{full_query_with_context}"

                    # Execute the tool asynchronously but block until result (safe via background loop)
                    coro = ask_data_insights_tool(
                        user_query_with_context=final_query,
                        table_references=json.dumps(tables_to_use)
                    )
                    response_string = run_async_sync(coro)

                    if not response_string:
                        st.warning("No response received from the backend.")
                        return

                    # Parse toolbox response
                    parsed_data = parse_tool_response(response_string)
                    print("--- 2. PARSED DATA DICTIONARY ---")
                    print(f"Type: {type(parsed_data)}")
                    print(f"{parsed_data.keys()}")
                    print("---------------------------------")
                    print(f"Content:\n{parsed_data}")
                    # print("---------------------------------")

                    # Extract the data from the parsed dictionary
                    # answer_string = parsed_data['Answer']
                    # answer_string = extract_final_answer(parsed_data['Answer'])
                    answer_string = parsed_data['Answer']

                    # Fix for single-line markdown tables
                    # answer_string = format_single_line_table(answer_string)

                    print("###################################################")
                    print("---  Answer ---")
                    print(f"Answer:\n{answer_string}")
                    print("---------------------------------")

                    sql_query = parsed_data['SQL Generated']
                    print("###################################################")
                    print("---  sql_query ---")
                    print(f"sql_query :\n{sql_query}")
                    print("---------------------------------")

                    if not answer_string:
                        st.warning("Received response but could not extract a valid answer.")
                        with st.expander("Raw Backend Response"):
                            st.code(response_string)
                        return

                    # Split answer into table/introduction, reasoning and follow_ups
                    table_and_intro = ""
                    reasoning = ""
                    follow_ups = ""

                    if "Reasoning:" in answer_string:
                        parts = answer_string.split("Reasoning:", 1)
                        table_and_intro = parts[0]
                        remaining_text = "Reasoning:\n" + parts[1]
                        if "Follow-up Questions:" in remaining_text:
                            reasoning_parts = remaining_text.split("Follow-up Questions:", 1)
                            reasoning = reasoning_parts[0]
                            follow_ups = reasoning_parts[1]
                        else:
                            reasoning = remaining_text
                    else:
                        table_and_intro = answer_string

                    # main_content = (table_and_intro.strip() + "\n\n" + reasoning.strip()).strip()
                    main_content = table_and_intro
                    # print("---  main_content ---")
                    # print(f"main_content :\n{main_content}")
                    # print("---------------------------------")


                    # Log the interaction (no interaction_id)
                    interaction_id = f"{st.session_state.get('username', 'anonymous')}-{datetime.now(timezone.utc).timestamp()}"
                    st.session_state["interaction_id"] = interaction_id
                    try:
                        log_to_bq(user_query=user_query, answer=main_content, interaction_id=interaction_id)
                    except Exception as e:
                        logger.warning(f"Failed to log to BQ: {e}")

                    # --- After getting response_string ---
                    if main_content:
                        # Save to session so UI doesn't reset on button clicks
                        st.session_state["last_answer"] = main_content
                        st.session_state["last_sql"] = sql_query or ""
                        st.session_state["last_user_query"] = user_query

                        try:
                            df = markdown_table_to_df(answer_string)
                            for col in df.columns[1:]:
                                try:
                                    df[col] = pd.to_numeric(df[col])
                                except Exception:
                                    pass
                            csv_buffer = io.StringIO()
                            df.to_csv(csv_buffer, index=False)
                            st.session_state["csv_bytes"] = csv_buffer.getvalue().encode("utf-8")
                        except Exception as e:
                            logger.warning(f"CSV generation failed: {e}")
                            st.session_state["csv_bytes"] = None

                    # Update history exactly as Chainlit did
                    history.append((user_query, main_content))
                    st.session_state["history"] = history
                    st.session_state["last_answer"] = main_content

                except Exception as e:
                    logger.error(f"An unexpected error occurred: {e}", exc_info=True)
                    st.error(f"An unexpected error occurred: {str(e)}")

            st.session_state["trigger_run"] = False

    # --- Show the answer (persistent) ---
    if "last_answer" in st.session_state and st.session_state["last_answer"]:
        st.markdown("### Orion Answer")

        answer_text = st.session_state["last_answer"]

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

        table_text = clean_table_alignment_row(answer_text)
        table_text = fix_incomplete_markdown_table(table_text)
        before, table_text, after = extract_markdown_table(answer_text)

        output_display, buttons = st.columns([5, 1])

        if before:
            output_display.markdown(before)
        if table_text:
            try:
                df = markdown_table_to_df(table_text)
                output_display.dataframe(df, use_container_width=True, hide_index=True)
            except Exception:
                output_display.markdown(table_text)  # fallback
        # else:
        #     st.info("No table detected.")

        if after:
            st.markdown(after)

        # ✅ View SQL collapsible section
        with st.expander("🧾 View SQL Query", expanded=False):
            if st.session_state.get("last_sql"):
                st.code(st.session_state["last_sql"], language="sql")
            else:
                st.info("No SQL query available.")

        # ✅ Download CSV — no rerun wipe
        csv_bytes = st.session_state.get("csv_bytes")

        if csv_bytes:
            with buttons:
                st.write("")
                st.write("")
                st.write("")
                st.write("")
                st.download_button(
                label="⬇️ Download CSV",
                data=csv_bytes,
                file_name="copilot_output.csv",
                mime="text/csv",
                key="download_csv_button",
                help="Download the output table as CSV"
            )

        # ✅ Feedback buttons — state-safe and logs to BQ
        buttons.write(" ")
        buttons.write(" ")
        buttons.write("Please submit feedback:")
        button1, button2 = buttons.columns([1, 1])
        if button1.button("👍Helpful", key="helpful_button"):
            try:
                log_to_bq(
                    user_query=st.session_state.get("last_user_query"),
                    answer=st.session_state.get("last_answer"),
                    user_feedback='positive',
                    interaction_id=st.session_state.get("interaction_id")
                )
                st.success("Thanks for your feedback!")
            except Exception as e:
                st.error(f"Failed to submit feedback: {e}")

        if button2.button("👎Not Helpful", key="not_helpful_button"):
            log_to_bq(user_query=st.session_state.get("last_user_query"), answer=st.session_state.get("last_answer"), user_feedback='negative', interaction_id=st.session_state.get("interaction_id"))
            st.warning("Feedback noted. Thanks for helping us improve!")


# ---------------------- ENTRY POINT ----------------------
if __name__ == "__main__":
    # Authentication: keep same behavior as your Chainlit password callback
    # main()
    if authenticate():
        main()
    else:
        st.stop()
