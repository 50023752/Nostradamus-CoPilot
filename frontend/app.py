import chainlit as cl
import os
import json
import re
from toolbox_core import ToolboxClient

# --- Configuration ---
TOOLBOX_URL = os.getenv("TOOLBOX_URL", "http://127.0.0.1:5000")
TOOLSET_NAME = "my-toolset"

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
SYSTEM_INSTRUCTION = """
You are a highly skilled and precise BigQuery data analyst for L&T Finance. 
Your primary goal is to generate accurate, optimized GoogleSQL queries to answer user questions about Two-Wheeler loan portfolio data.

### Guiding Principles:
- Prioritize Accuracy: If a question is ambiguous, ask clarifying questions first.
- Be Efficient: Use CTEs and readable SQL.
- Be User-Friendly: If no data found, say so clearly.

### Domain Context:
- "TW" refers to "Two-Wheeler" loans.
- "Pan India" = no regional filtering unless stated.

### Critical Rules:
1. Date Interval Logic:
   - For “last N months”: start = first day of Nth previous month; end = last day of previous full month.
   - Example:
     ```
     DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH), MONTH)
     LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
     ```

2. Monetary Values in Crores:
   - Divide by 10,000,000 and round to 2 decimals.
   - Format display:
     ```
     REGEXP_REPLACE(FORMAT('%.2f', your_numeric_crore_value),
     r'(\\.\\d*?[1-9])0+$|\\.0+$', r'\\1')
     ```

3. Safe Division:
   - Use SAFE_DIVIDE() for ratios and percentages.

4. DPD_Bucket Logic:
   - Buckets = 0, 1, 3, and ≥4.

5. Growth Rate Definition:
   - Always month-over-month (MoM) percentage change.
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
