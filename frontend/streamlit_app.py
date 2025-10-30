from logger import logger
import os
import ast
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
import plotly.express as px
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)

# Load .env
load_dotenv()

# Allow nested event loops for Streamlit environment
nest_asyncio.apply()

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
                logger.debug(f'USER_CREDENTIALS: {user_credentials}')
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


# ---------------------- MAIN APP ----------------------
def main():
    st.set_page_config(page_title="Orion Copilot", page_icon="🪐", layout="wide", initial_sidebar_state="auto")

    # Inject custom CSS to reduce top padding
    st.markdown(
        """
        <style>
            .block-container {
                padding-top: 1rem;
            }
        </style>
        """, unsafe_allow_html=True)
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

    st.title("Orion - The Nostradamus Copilot")

    if "last_answer" not in st.session_state:
        st.session_state["last_answer"] = ""
    if "csv_bytes" not in st.session_state:
        st.session_state["csv_bytes"] = None
    # Keep client/toolset None in session (we create/cache inside background loop)
    st.session_state.setdefault("client", None)
    st.session_state.setdefault("toolset", None)
    
    st.markdown("""I'm ready to answer your questions about the Two Wheeler Data. First, select a data source, then type your question below to get started!""")

    # --- Data source selection ---
    # st.markdown("---")
    st.markdown("##### Select a Data Source : ")

    # Define the new, more descriptive options
    option_historic = "Historical Performance Trends"
    option_realtime = "Current Performance & Disbursals"

    data_choice = st.radio(
        "Select which data to query:",
        options=[option_historic, option_realtime],
        key="data_choice",
        label_visibility="collapsed"
    )

    # Display detailed explanations for each choice
    if data_choice == option_historic:
        st.markdown(
            """
                <div style="margin-left:25px; background-color:#f0f2f6; padding:15px; border-left:5px solid #1f77b4; border-radius:8px;">
                <b>What it's for:</b> Analyzing portfolio performance and health (GNS, NNS, DPD, etc.) over time.<br>
                <b>Timeframe:</b> Covers all completed months, from April 2024 to the end of last month.<br>
                <b>Example Questions:</b><br>
                • Show me the GNS trend for the last 6 months.<br>
                • Compare Q1 vs. Q2 portfolio health.<br>
                • What was the total collection in May 2025?
            </div>
            """,
            unsafe_allow_html=True
        )

    elif data_choice == option_realtime:
        st.markdown(
            """
                <div style="margin-left:25px; background-color:#f0f2f6; padding:15px; border-left:5px solid #1f77b4; border-radius:8px;">
                <b>What it's for:</b> Getting the most recent, up-to-date snapshot of portfolio performance.<br>
                <b>Timeframe:</b> Provides the latest available data (yesterday's performance).<br>
                <b>Example Questions:</b><br>
                • What is the month-to-date disbursal amount?<br>
                • Show me yesterday's paid vs. unpaid accounts.<br>
                • What is the current MTD disbursal for the North region?
            </div>
            """,
            unsafe_allow_html=True
        )


    # Input area
    col1, col2 = st.columns([5, 1])
    with col1:
        st.markdown("##### Ask your question : ")
        user_query = st.text_area("**Ask your question:**", value="", height=68, key="user_query",label_visibility  = "collapsed")
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

                    if data_choice == option_realtime:
                        tables_to_use = [{
                            "projectId": config.PROJECT_ID,
                            "datasetId": config.DATASET_ID_RT,
                            "tableId": config.TABLE_ID_RT
                        }]
                    elif data_choice == option_historic:
                        tables_to_use = [{
                            "projectId": config.PROJECT_ID,
                            "datasetId": config.DATASET_ID_HIST,
                            "tableId": config.TABLE_ID_HIST
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

                    # logger.debug("###################################################")
                    # logger.debug("--- 1. RESPONSE STRING ---")
                    # logger.debug(f"Type: {type(response_string)}")
                    # logger.debug(f"response_string:\n{response_string}")
                    # logger.debug("---------------------------------")



                    # Parse toolbox response
                    parsed_data = parse_tool_response(response_string)
                    logger.debug("--- 2. PARSED DATA DICTIONARY ---")
                    logger.debug(f"Type: {type(parsed_data)}")
                    logger.debug(f"{parsed_data.keys()}")
                    logger.debug("---------------------------------")
                    logger.debug(f"Content:\n{parsed_data}")
                    # logger.debug("---------------------------------")

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
                    logger.debug("###################################################")
                    logger.debug("---  sql_query ---")
                    logger.debug(f"sql_query :\n{sql_query}")
                    logger.debug("---------------------------------")

                    chart_name = parsed_data.get("Chart name")
                    x_axis = parsed_data.get("x_axis")
                    y_axes = parsed_data.get("y_axes", [])
                    logger.debug('#################')
                    logger.debug(f'Chart Name - {chart_name}, X_Axis - {x_axis}, Y_Axes - {y_axes}')
                    logger.debug('#################')


                    if not answer_string:
                        st.warning("Received response but could not extract a valid answer.")
                        with st.expander("Raw Backend Response"):
                            st.code(response_string)
                        return

                    main_content = answer_string.strip()
                    # logger.debug("---  main_content ---")
                    # logger.debug(f"main_content :\n{main_content}")
                    # logger.debug("---------------------------------")


                    # Log the interaction (no interaction_id)
                    interaction_id = f"{st.session_state.get('username', 'anonymous')}-{datetime.now(timezone.utc).timestamp()}"
                    st.session_state["interaction_id"] = interaction_id
                    
                    # Get user id from Streamlit session (fallback to anonymous)
                    user_id = st.session_state.get("username", "anonymous")
                    try:
                        log_to_bq(user_id, user_query=user_query, answer=main_content, interaction_id=interaction_id)
                        logger.info(f'Interaction logged to BQ for {interaction_id}')
                    except Exception as e:
                        logger.warning(f"Failed to log to BQ: {e}")

                    # --- After getting response_string ---
                    if main_content:
                        # Save to session so UI doesn't reset on button clicks
                        st.session_state["last_answer"] = main_content
                        st.session_state["last_sql"] = sql_query or ""
                        st.session_state["last_user_query"] = user_query
                        st.session_state["chart_name"] = chart_name
                        st.session_state["x_axis"] = x_axis
                        st.session_state["y_axes"] = y_axes
                        st.session_state["df_for_chart"] = None # Reset

                        try:
                            df = markdown_table_to_df(answer_string)
                            for col in df.columns[1:]:
                                try:
                                    df[col] = pd.to_numeric(df[col])
                                except Exception:
                                    pass
                            csv_buffer = io.StringIO()
                            st.session_state["df_for_chart"] = df # Save df for chart
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

        table_text = clean_table_alignment_row(answer_text)
        table_text = fix_incomplete_markdown_table(table_text)
        before, table_text, after = extract_markdown_table(answer_text)

        output_display, buttons = st.columns([5, 1])

        logger.debug(f"before (repr): {repr(before)}")
        logger.debug(f"type(before): {type(before)}")

        if before and isinstance(before, str) and before.strip().lower() not in ["", "undefined", "none", "null", "```"]:
            logger.debug("In before")
            output_display.markdown(before)
        else:
            logger.debug(f"Skipping before — invalid value: {repr(before)}")

        if table_text:
            try:
                df = markdown_table_to_df(table_text)
                # Format column names: capitalize, replace underscores
                df.columns = [format_axis_title(col) for col in df.columns]

                # styled_df = (
                #     df.style.set_table_styles([
                #         {"selector": "th", "props": [("font-weight", "bold"), ("text-align", "center")]}
                #     ])
                # )

                # # ✅ Convert to HTML and render via markdown (allow HTML)
                # html_table = styled_df.to_html()
                # output_display.markdown(html_table, unsafe_allow_html=True)
                output_display.dataframe(df, use_container_width=True, hide_index=True)

            except Exception as e:
                logger.warning(f"Failed to render styled dataframe, falling back to markdown. Error: {e}")
                output_display.markdown(table_text)  # fallback

        if after and isinstance(after, str) and after.strip().lower() not in ["", "undefined", "none", "null", "```"]:
            st.markdown(after)

        # ✅ View SQL collapsible section
        with st.expander("View SQL Query", expanded=False):
            if st.session_state.get("last_sql"):
                st.code(st.session_state["last_sql"], language="sql")
            else:
                st.info("No SQL query available.")

        # ✅ View Chart collapsible section
        with st.expander("View Chart", expanded=False):
            chart_name = st.session_state.get("chart_name")
            df_for_chart = st.session_state.get("df_for_chart")

            if df_for_chart is not None and not df_for_chart.empty:
                columns = df_for_chart.columns.tolist()
                
                # --- Get default axes from model or fallback ---
                model_x_axis = st.session_state.get("x_axis")
                model_y_axes = st.session_state.get("y_axes", [])

                # Determine default X-axis
                if model_x_axis and model_x_axis in columns:
                    default_x_index = columns.index(model_x_axis)
                else:
                    default_x_index = 0 # Fallback to first column

                # Determine default Y-axes
                default_y_axes = []
                if model_y_axes:
                    # Handle if model_y_axes is a string like '["col1", "col2"]' or "col1, col2"
                    if isinstance(model_y_axes, str):
                        try:
                            # Try parsing as a list literal
                            parsed_axes = ast.literal_eval(model_y_axes)
                            if isinstance(parsed_axes, list):
                                model_y_axes = parsed_axes
                            else: # Fallback for other string formats
                                model_y_axes = [y.strip() for y in model_y_axes.split(',')]
                        except (ValueError, SyntaxError):
                            model_y_axes = [y.strip() for y in model_y_axes.split(',')]
                    
                    default_y_axes = [col for col in model_y_axes if col in columns]

                if not default_y_axes and len(columns) > 1:
                    # Fallback to all columns except the selected X-axis
                    x_col_name = columns[default_x_index]
                    default_y_axes = [col for col in columns if col != x_col_name]

                # --- UI for axis selection ---
                # st.markdown("##### Customize Chart Axes")
                col1, col2 = st.columns(2)
                selected_x_axis = col1.selectbox("Select X-Axis:", columns, index=default_x_index)
                
                # Ensure Y-axis choices don't include the selected X-axis
                y_axis_options = [col for col in columns if col != selected_x_axis]
                
                # Filter default_y_axes to only include valid options
                valid_default_y = [col for col in default_y_axes if col in y_axis_options]
                
                selected_y_axes = col2.multiselect("Select Y-Axis (one or more):", y_axis_options, default=valid_default_y)

                # --- Generate Chart ---
                if selected_x_axis and selected_y_axes:
                    try:
                        plot_df = df_for_chart.copy()
                        
                        # Ensure all plottable y-axis columns are numeric
                        plottable_y_axes = []
                        for col in selected_y_axes:
                            try:
                                plot_df[col] = pd.to_numeric(plot_df[col])
                                plottable_y_axes.append(col)
                            except (ValueError, TypeError):
                                st.warning(f"Column '{col}' contains non-numeric values and will be excluded from the chart.", icon="⚠️")

                        if plottable_y_axes:
                            # Rename columns for display
                            rename_map = {col: format_axis_title(col) for col in plottable_y_axes}
                            plot_df.rename(columns=rename_map, inplace=True)
                            display_y_axes = [rename_map[col] for col in plottable_y_axes]

                            fig = px.line(plot_df, x=selected_x_axis, y=display_y_axes, title=chart_name or " ", markers=True)
                            fig.update_layout(
                                template="plotly_white",
                                xaxis=dict(
                                    title=dict(
                                        text=format_axis_title(selected_x_axis),
                                        font=dict(color='black')
                                    ),
                                    tickfont=dict(color='black')
                                ),
                                yaxis=dict(
                                    title=dict(text="Value", font=dict(color='black')),
                                    tickfont=dict(color='black')),
                                legend_title_text='Metrics',
                                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                                hovermode="x unified"
                            )
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.warning("No plottable numeric Y-axis columns selected.", icon="⚠️")

                    except Exception as e:
                        logger.error(f"Failed to generate chart: {e}")
                        st.warning(f"Could not generate the chart. Please check the error: {e}", icon="⚠️")
                else:
                    st.info("Please select at least one Y-axis column to generate a chart.")
            else:
                st.info("No data available to generate a chart.")

        # ✅ Download CSV — no rerun wipe
        csv_bytes = st.session_state.get("csv_bytes")

        if csv_bytes:
            with buttons:
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
                    user_id = st.session_state.get("username", "anonymous"),
                    user_query=st.session_state.get("last_user_query"),
                    answer=st.session_state.get("last_answer"),
                    user_feedback='positive',
                    interaction_id=st.session_state.get("interaction_id")
                )
                st.success("Thanks for your feedback!")
            except Exception as e:
                st.error(f"Failed to submit feedback: {e}")

        if button2.button("👎Not Helpful", key="not_helpful_button"):
            log_to_bq(user_id = st.session_state.get("username", "anonymous"), user_query=st.session_state.get("last_user_query"), answer=st.session_state.get("last_answer"), user_feedback='negative', interaction_id=st.session_state.get("interaction_id"))
            st.warning("Feedback noted. Thanks for helping us improve!")


# ---------------------- ENTRY POINT ----------------------
if __name__ == "__main__":
    with st.sidebar:
        st.image("https://www.ltfinance.com/images/default-source/company-logo/l-t-finance-logo.png?sfvrsn=e2123fc4_1")
    main()
    # if authenticate():
    #     main()
    # else:
    #     st.stop()
