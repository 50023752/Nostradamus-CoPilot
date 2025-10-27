import os
import json
import io
import pandas as pd
import chainlit as cl
from toolbox_core import ToolboxClient # type: ignore
from logger import logger
import config
from utils import *

# --- Load System Prompt on startup ---
SYSTEM_INSTRUCTION = load_system_prompt(config.SYSTEM_PROMPT_FILE)

# -------- 1. The New Authentication Callback --------
@cl.password_auth_callback
def auth_callback(username: str, password: str):
    """
    This function reads a list of user credentials from an environment
    variable and verifies them.
    The environment variable `USER_CREDENTIALS` should be a JSON string
    of a list of objects, e.g., '[{"username": "user1", "password": "pw1"}]'
    """
    # Try the new multi-user credential system first
    creds_str = os.getenv("USER_CREDENTIALS")
    if creds_str:
        try:
            user_credentials = json.loads(creds_str)
            if not isinstance(user_credentials, list):
                logger.error("USER_CREDENTIALS is not a valid JSON list.")
                return None
            
            for cred in user_credentials:
                if cred.get("username") == username and cred.get("password") == password:
                    return cl.User(identifier=username)
        
        except json.JSONDecodeError:
            logger.error("Failed to decode USER_CREDENTIALS JSON.")
            return None

    # If no credentials match, return None.
    return None


@cl.on_chat_start
async def start_chat():
    """Initializes the chat session."""
    client = ToolboxClient(config.TOOLBOX_URL)
    cl.user_session.set("client", client)

    # --- Initialize an empty history for the user's session ---
    cl.user_session.set("history", [])
    
    await cl.Message(
        author="Orion",
        content=""" ***Welcome to Orion - The Nostradamus Copilot*** 
        I'm ready to answer your questions about the Two Wheeler Data.
        Type your question below to get started!
        Example questions:
        - What is the growth rate in TW disbursement of Pan India in the last 6 months?
        - What is the GNS for 1st month region wise percentage and count?
        - Which region has the highest slippage of customers from 0 dpd last year to 30+ dpd ?
        - Could you tell me the split of high, medium and risky customers count and percentage according to the early warning score model?
        """
    ).send()


@cl.on_message
async def main(message: cl.Message):
    """Handles the main logic for processing a user's message."""
    client: ToolboxClient = cl.user_session.get("client")

    user_query = message.content
    thinking_message = cl.Message(author="Orion", content="Processing your query...")
    await thinking_message.send()

    # --- Retrieve history and build the context string ---
    history = cl.user_session.get("history")
    
    context_parts = []
    for q, a in history:
        context_parts.append(f"Previous Question: {q}")
        context_parts.append(f"Previous Answer: {a}")
    
    context_parts.append(f"Current Question: {user_query}")
    
    # Combine everything into one system-context-aware query
    full_query_with_context = f"{SYSTEM_INSTRUCTION}\n\n" + "\n\n".join(context_parts)

    try:
        toolset_list = await client.load_toolset(config.TOOLSET_NAME)
        
        if not toolset_list:
            await cl.Message(author="Error", content=f"Toolset '{config.TOOLSET_NAME}' is empty or could not be loaded.").send()
            return
            
        ask_data_insights_tool = toolset_list[0]

        tables_to_use = [{
            "projectId": config.PROJECT_ID,
            "datasetId": config.DATASET_ID_1,
            "tableId": config.TABLE_ID_1
        }]

        temperature = 0.0  # set to your desired value
        full_query_with_context_final = f"Temperature setting: {temperature}\n\n{full_query_with_context}"


        # The tool call returns a single string containing multiple JSON objects
        response_string = await ask_data_insights_tool(
            user_query_with_context=full_query_with_context_final,
            table_references=json.dumps(tables_to_use)
        )

        if not response_string:
            await cl.Message(author="Orion", content="I'm sorry, I couldn't generate a response for that question.").send()
            return

        logger.debug("--- 1. RAW RESPONSE FROM TOOLBOX ---")
        logger.debug(f"Type: {type(response_string)}")
        logger.debug(f"Content:\n{response_string}")
        logger.debug("------------------------------------")

        # Parse response
        parsed_data = parse_tool_response(response_string)

        logger.debug("--- 2. PARSED DATA DICTIONARY ---")
        logger.debug(f"Type: {type(parsed_data)}")
        logger.debug(f"Content:\n{parsed_data}")
        logger.debug("---------------------------------")

        # 1. Extract the data from the parsed dictionary
        answer_string = parsed_data.get("Answer", "")
        sql_query = parsed_data.get("SQL Generated", "")

        if not answer_string:
            thinking_message.content = "I received a response from the backend, but I couldn't extract a valid answer. Please check the 'Raw Backend Response' for details."
            thinking_message.elements = [cl.Text(name="Raw Backend Response", content=f"```\n{response_string}\n```", display="inline")]
            await thinking_message.update()
            return

        logger.debug("--- 3. EXTRACTED SQL ---")
        logger.debug(f"Type: {type(sql_query)}")
        logger.debug(f"Content:\n{sql_query}")
        logger.debug("------------------------")

        logger.debug("--- 4. EXTRACTED ANSWER ---")
        logger.debug(f"Type: {type(answer_string)}")
        logger.debug(f"Content:\n{answer_string}")
        logger.debug("---------------------------")
        
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
        
        # Send initial "Processing..." message and get its ID
        # This ensures thinking_message.id is available for logging and actions
        await thinking_message.update()
        interaction_id = thinking_message.id

        # --- Update the history with the latest exchange ---
        history.append((user_query, main_content))
        cl.user_session.set("history", history)
        # ----------------------------------------------------
        thinking_message.content = main_content

        # --- Create a list of actions and elements ---
        actions = []

        # Add the "View SQL" button if a query exists
        if sql_query:
            actions.append(
                cl.Action(name="view_sql", value="sql", label="🧾 View SQL", payload={"sql_query": sql_query})
            )
        
        if follow_ups:
            actions.append(cl.Action(name="view_follow_ups", value="follow_ups", label="❓ Follow-ups", payload={"follow_ups": follow_ups.strip()}))

        # Log the initial interaction with the message ID
        log_to_bq(user_query=user_query, answer=main_content, interaction_id=interaction_id)

        # --- Add CSV download button ---
        actions.append(
            cl.Action(
                name="download_csv",
                value="download_csv",
                label="Download CSV",
                payload={"answer_text": answer_string}
            )
        )
        # --- Add thumbs up ---
        actions.append(
            cl.Action(
                name="feedback_up",
                value="thumbs_up",
                label="👍",
                payload={"user_query": user_query, "answer": answer_string}
            )
        )
        # --- Add thumbs down ---
        actions.append(
            cl.Action(
                name="feedback_down",
                value="thumbs_down",
                label="👎",
                payload={"user_query": user_query, "answer": answer_string}
            )
        )

        # 5. Attach the elements and update the final message
        # Update the actions with the interaction_id in their payload
        for action in actions:
            action.payload["interaction_id"] = interaction_id

        # Attach the elements and update the final message
        thinking_message.actions = actions
        await thinking_message.update()
        await thinking_message.update() # Update with content and actions

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
        log_to_bq(user_query=user_query, answer=None, status="error", error_message=str(e))
        await cl.Message(
            author="Error",
            content=f"An unexpected error occurred: {str(e)}"
        ).send()


# --- Create a function to handle the SQL button click ---
@cl.action_callback("view_sql")
async def on_action(action: cl.Action):
    """
    This function is called when the user clicks the 'View SQL' button.
    """
    sql_query_from_payload = action.payload.get("sql_query")
    
    if sql_query_from_payload:
        # --- Format the content as a Markdown string ---
        await cl.Message(
            author="Generated SQL",
            content=f"```sql\n{sql_query_from_payload}\n```"
        ).send()
        # ----------------------------------------------------
    else:
        await cl.Message(content="Could not retrieve the SQL query from the action.").send()

# --- Action callback for Follow-up Questions ---
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

# --- Action callback for CSV download ---
@cl.action_callback("download_csv")
async def on_download_csv(action: cl.Action):
    answer_text = action.payload.get("answer_text", "")
    if not answer_text.strip():
        await cl.Message("No data available to download.").send()
        return

    try:
        df = markdown_table_to_df(answer_text)

        # Convert numeric columns automatically
        for col in df.columns[1:]:
            df[col] = pd.to_numeric(df[col], errors='ignore')

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        file_element = cl.File(
            name="copilot_result.csv",
            content=csv_buffer.getvalue().encode('utf-8'),
            display="inline"
        )

        await cl.Message(
            content="**Download CSV:** Click below to download the extracted answer -",
            elements=[file_element]
        ).send()

        logger.info(f"CSV generated successfully")

    except ValueError as e:
        logger.warning(f"Could not generate CSV from markdown: {e}")
        await cl.Message(f"Could not detect a valid table in the response to generate a CSV.").send()
    except Exception as e:
        logger.error(f"Failed to generate CSV: {str(e)}")
        await cl.Message(f"An error occurred while generating the CSV: {str(e)}").send()


@cl.action_callback("feedback_up")
async def handle_feedback_up(action: cl.Action):
    user = cl.user_session.get("user")
    user_id = user.identifier if user else "anonymous"
    user_query = action.payload.get("user_query")
    answer = action.payload.get("answer")
    interaction_id = action.payload.get("interaction_id")

    logger.info(f"[FEEDBACK] 👍 submitted by {user_id} (Interaction ID: {interaction_id})")

    # Update the existing row with feedback
    log_to_bq(user_query=user_query, answer=answer, user_feedback='positive', interaction_id=interaction_id)
    await cl.Message(author="Orion", content="Feedback submitted!").send()


@cl.action_callback("feedback_down")
async def handle_feedback_down(action: cl.Action):
    user = cl.user_session.get("user")
    user_id = user.identifier if user else "anonymous"
    user_query = action.payload.get("user_query")
    answer = action.payload.get("answer")
    interaction_id = action.payload.get("interaction_id")

    logger.info(f"[FEEDBACK] 👎 submitted by {user_id} (Interaction ID: {interaction_id})")

    # Update the existing row with feedback
    log_to_bq(user_query=user_query, answer=answer, user_feedback='negative', interaction_id=interaction_id)
    await cl.Message(author="Orion", content="Feedback submitted!").send()