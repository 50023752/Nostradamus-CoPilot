import chainlit as cl
import os
import json
import re
from toolbox_core import ToolboxClient
import pandas as pd
import io

# --- Configuration ---
TOOLBOX_URL = os.getenv("TOOLBOX_URL", "http://127.0.0.1:5000")
TOOLSET_NAME = "my-toolset"

# print(os.getenv("CHAINLIT_AUTH_SECRET"))

# -------- 1. The New Authentication Callback --------
@cl.password_auth_callback
def auth_callback(username: str, password: str):
    """
    This function reads credentials from the environment and verifies them.
    """
    if (
        username == os.getenv("ADMIN_USERNAME")
        and password == os.getenv("ADMIN_PASSWORD")
    ):
        # If credentials match, return a cl.User object.
        return cl.User(identifier="admin")
    else:
        # If they don't match, return None.
        return None
# --------------------------------------------------


# -------- Helper Function: Parse multi-JSON response --------
def parse_tool_response(response_text: str):
    """Extracts SQL and Answer blocks from a multi-JSON string."""
    if not response_text:
        return {"SQL Generated": "", "Answer": ""}

    # --- THIS IS THE FIX ---
    # The response is a string that is itself JSON encoded.
    # We need to decode it once to get the inner multi-part JSON string.
    try:
        actual_content_string = json.loads(response_text)
    except (json.JSONDecodeError, TypeError):
        # If it's not a JSON-encoded string (e.g., already decoded), use it as is.
        actual_content_string = response_text
    # ----------------------------------------------------

    # Now, parse the inner string which contains multiple JSON objects
    json_blocks = re.findall(r'\{.*?\}', actual_content_string, flags=re.S)
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


# --- SYSTEM INSTRUCTION (Persistent Analyst Rules) ---
SYSTEM_INSTRUCTION = r"""
    You are a highly skilled and precise BigQuery data analyst for L&T Finance. Your primary goal is to generate accurate, optimized GoogleSQL queries to answer user questions about Two-Wheeler loan portfolio data.
      Your ONLY task is to return a Markdown table based on the user's question. Under NO circumstances should the word "chart" appear in your response.
      ### Guiding Principles:
      - **Prioritize Accuracy:** If a question is ambiguous or lacks detail, ask clarifying questions before generating a query. Do not make risky assumptions.
      - **Be Efficient:** Write clean, readable SQL, using Common Table Expressions (CTEs) to structure complex logic.
      - **Be User-Friendly:** If a query correctly returns no results, state that clearly (e.g., "No data was found for your criteria") instead of giving an empty answer.

      ### Domain Context:
      - **"TW"** always refers to "Two-Wheeler" loans.
      - **"Pan India"** means you should not filter by any specific region, state, or city unless explicitly asked.

      ### CRITICAL RULES TO FOLLOW:

      1.  **Date Interval Logic:** The end date of your analysis period MUST adapt based on the time granularity of the user's question.
          * **For Monthly and Quarterly analysis:** The date range MUST end on the last day of the most recent **fully completed** month or quarter.
              * Example End Date (Month): LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
          * **For Yearly analysis:** The date range MUST extend to the present day to include partial data from the current year (i.e., Year-to-Date).
              * Example End Date (Year): CURRENT_DATE()
          * **For "Last N" questions:** The start date must be the beginning of the Nth prior period.
              * Example Start Date ("last 6 months"): DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH), MONTH)

      2.  **Numeric Formatting (Crores):** All monetary values (like DISBURSALAMOUNT) MUST be reported in crores. This is a two-step process:
          * **Step 1 (Calculation):** In a CTE or subquery, calculate the raw value in crores by dividing by 10,000,000 and rounding to 2 decimal places. Use this numeric result for ALL sorting or further calculations.
          * **Step 2 (Display):** In the final SELECT statement ONLY, format the numeric crore value for clean display using this RegEx: REGEXP_REPLACE(FORMAT('%.2f', your_numeric_crore_value), r'(\.\d*?[1-9])0+$|\.0+$', r'\1').
      3.  **Safe Division:** When calculating percentages, ratios, or averages, ALWAYS use SAFE_DIVIDE() to prevent "division-by-zero" errors.
          * **Example:** SAFE_DIVIDE(SUM(CASE WHEN Net_Bounce_Flag = 1 THEN 1 ELSE 0 END), COUNT(*)) * 100

      4.  **DPD_Bucket Binning Logic:** When binning by DPD_Bucket, you MUST use a CASE statement to create these exact five buckets: 0, 1, 2, 3, and 4+. Any other binning is prohibited.

      5.  **Growth Rate Definition:** If asked for "growth rate", "change", "trend", or similar terms, you MUST calculate and display the **month-over-month (MoM) percentage change**.

      6.  **Final Output Formatting:** Your final response MUST be **ONLY** the data from the query, formatted as a Markdown table. Your response **MUST** begin with the Markdown table header (e.g., | month_start | ...) and **MUST** end with the final character of the table. **ABSOLUTELY NO** other text, narrative, summary, interpretation, or explanation is permitted, especially any mention of charts.

      7.  **Query Error Protocol:** If the generated SQL query fails to execute in BigQuery, you MUST NOT attempt to answer the user's question. Your response must be: "The query could not be completed due to the following error: [Please try again.]."   
    """


@cl.on_chat_start
async def start_chat():
    client = ToolboxClient(TOOLBOX_URL)
    cl.user_session.set("client", client)

    # --- NEW: Initialize an empty history for the user's session ---
    cl.user_session.set("history", [])
    
    await cl.Message(
        author="Data Assistant",
        content=""" ***Welcome to Nostradamus Assistant*** 
        I'm ready to answer your questions about the Two Wheeler Data.
        Type your question below to get started!
        Example questions:
        - What is the growth rate in TW disbursement of Pan India in the last 6 months?
        - What is the growth rate in TW disbursement - State wise in the last 6 months?
        - Give the disbursement Growth rate trajectory branch wise in the last 12 months - m-o-m?
        """
    ).send()

@cl.on_message
async def main(message: cl.Message):
    client = cl.user_session.get("client")
    
    user_query = message.content
    thinking_message = cl.Message(author="Data Assistant", content="Processing your query...")
    await thinking_message.send()

    # --- NEW: Retrieve history and build the context string ---
    history = cl.user_session.get("history")
    
    context_parts = []
    for q, a in history:
        context_parts.append(f"Previous Question: {q}")
        context_parts.append(f"Previous Answer: {a}")
    
    context_parts.append(f"Current Question: {user_query}")
    
    # Combine everything into one system-context-aware query
    full_query_with_context = f"{SYSTEM_INSTRUCTION}\n\n" + "\n\n".join(context_parts)

    try:
        toolset_list = await client.load_toolset(TOOLSET_NAME)
        
        if not toolset_list:
            await cl.Message(author="Error", content=f"Toolset '{TOOLSET_NAME}' is empty or could not be loaded.").send()
            return
            
        ask_data_insights_tool = toolset_list[0]

        tables_to_use = [{
            "projectId": "analytics-datapipeline-prod",
            "datasetId": "aiml_cj_nostd_mart", # Corrected dataset name from previous logs
            "tableId": "TW_NOSTD_MART_realtime"
        }]

        temperature = 0.1  # set to your desired value
        full_query_with_context_final = f"Temperature setting: {temperature}\n\n{full_query_with_context}"


        # The tool call returns a single string containing multiple JSON objects
        response_string = await ask_data_insights_tool(
            user_query_with_context=full_query_with_context_final,
            table_references=json.dumps(tables_to_use)
        )

        if not response_string:
            await cl.Message(author="Data Assistant", content="I'm sorry, I couldn't generate a response for that question.").send()
            return

        # --- DEBUGGING PRINTS ---
        print("\n\n--- 1. RAW RESPONSE FROM TOOLBOX ---")
        print(f"Type: {type(response_string)}")
        print(f"Content:\n{response_string}")
        print("------------------------------------\n")


        # Parse response
        parsed_data = parse_tool_response(response_string)

        print("--- 2. PARSED DATA DICTIONARY ---")
        print(f"Type: {type(parsed_data)}")
        print(f"Content:\n{parsed_data}")
        print("---------------------------------\n")

        # --- Final UI Construction ---
        
        # 1. Extract the data from the parsed dictionary
        answer_string = parsed_data.get("Answer", "")
        sql_query = parsed_data.get("SQL Generated", "")

        if not answer_string:
            thinking_message.content = "I received a response from the backend, but I couldn't extract a valid answer. Please check the 'Raw Backend Response' for details."
            thinking_message.elements = [cl.Text(name="Raw Backend Response", content=f"```\n{response_string}\n```", display="inline")]
            await thinking_message.update()
            return

        print("--- 3. EXTRACTED SQL ---")
        print(f"Type: {type(sql_query)}")
        print(f"Content:\n{sql_query}")
        print("------------------------\n")

        print("--- 4. EXTRACTED ANSWER ---")
        print(f"Type: {type(answer_string)}")
        print(f"Content:\n{answer_string}")
        print("---------------------------\n")

        # 2. Parse the answer string into its components
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
        
        # 3. Construct the main message content (Table + Reasoning)
        main_content = table_and_intro.strip() + "\n\n" + reasoning.strip()
        
        # --- NEW: Update the history with the latest exchange ---
        history.append((user_query, main_content))
        cl.user_session.set("history", history)
        # ----------------------------------------------------


        thinking_message.content = main_content

        # --- NEW: Create a list of actions and elements ---
        actions = []
        elements = []
        
        # Add the "View SQL" button if a query exists
        if sql_query:
            actions.append(
                cl.Action(name="view_sql", value="sql", label="🧾 View SQL", payload={"sql_query": sql_query})
            )
        
        if follow_ups:
            actions.append(cl.Action(name="view_follow_ups", value="follow_ups", label="❓ Follow-ups", payload={"follow_ups": follow_ups.strip()}))

        # --- Add CSV download button ---
        actions.append(
            cl.Action(
                name="download_csv",
                value="download_csv",
                label="Download CSV",
                payload={"answer_text": answer_string}  # pass the extracted answer
            )
        )

        # 5. Attach the elements and update the final message
        thinking_message.elements = elements
        thinking_message.actions = actions
        await thinking_message.update()

    except Exception as e:
        await cl.Message(
            author="Error",
            content=f"An unexpected error occurred: {str(e)}"
        ).send()


# --- NEW: Create a function to handle the button click ---
@cl.action_callback("view_sql")
async def on_action(action: cl.Action):
    """
    This function is called when the user clicks the 'View SQL' button.
    """
    sql_query_from_payload = action.payload.get("sql_query")
    
    if sql_query_from_payload:
        # --- FIX: Format the content as a Markdown string ---
        await cl.Message(
            author="Generated SQL",
            content=f"```sql\n{sql_query_from_payload}\n```"
        ).send()
        # ----------------------------------------------------
    else:
        await cl.Message(content="Could not retrieve the SQL query from the action.").send()

# --- NEW: Action callback for Follow-up Questions ---
@cl.action_callback("view_follow_ups")
async def on_follow_ups_action(action: cl.Action):
    follow_ups_from_payload = action.payload.get("follow_ups")
    if follow_ups_from_payload:
        await cl.Message(
            author="Suggested Follow-ups",
            content=follow_ups_from_payload
        ).send()
    else:
        await cl.Message(content="Could not retrieve the follow-up questions.").send()

# --- NEW: Action callback for CSV download ---
@cl.action_callback("download_csv")
async def on_download_csv(action: cl.Action):
    import io
    import pandas as pd

    answer_text = action.payload.get("answer_text", "")
    if not answer_text.strip():
        print("No answer_text found in payload.")
        await cl.Message("No data available to download.").send()
        return

    try:

        # Split lines and remove empty lines
        lines = [line.strip() for line in answer_text.splitlines() if line.strip()]

        # Find the first line that looks like a table header (starts with '|')
        table_start_idx = None
        for i, line in enumerate(lines):
            if line.startswith("|"):
                table_start_idx = i
                break

        if table_start_idx is None or len(lines) <= table_start_idx + 1:
            print("Could not detect a Markdown table in the response.")
            await cl.Message("Could not detect a Markdown table in the response.").send()
            return

        # Header row
        headers = [h.strip() for h in lines[table_start_idx].strip("|").split("|")]

        # Data rows (skip header + separator line)
        data_rows = []
        for row in lines[table_start_idx + 2:]:
            cells = [c.strip() for c in row.strip("|").split("|")]
            if len(cells) != len(headers):
                print(f"Skipping row (column mismatch): {cells}")
                continue
            data_rows.append(cells)

        df = pd.DataFrame(data_rows, columns=headers)

        # Optional: convert numeric columns automatically
        for col in df.columns[1:]:
            df[col] = pd.to_numeric(df[col], errors='ignore')

        # CSV in-memory
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        file_name = "copilot_result.csv"
        file_element = cl.File(name=file_name, content=csv_buffer.getvalue())
        await cl.Message(
            content="**Download CSV:** Click below to download the extracted answer -",
            elements=[file_element]
        ).send()

        print(f"CSV generated successfully: {file_name}")

    except Exception as e:
        print(f"Failed to generate CSV: {str(e)}")
        await cl.Message(f"Failed to generate CSV: {str(e)}").send()
