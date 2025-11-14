import vertexai
from fastmcp import FastMCP, Client
from vertexai.generative_models import GenerativeModel
from server.bigquery_client import run_query, get_schema_and_sample
from server.utilities import *
import logging
import asyncio
import time
from google.cloud import bigquery
# from cachetools import TTLCache
from server.logger import get_logger
logger = get_logger()
from server.config import * 

SYSTEM_PROMPT_FILE = './server/system_prompt.txt'
SYSTEM_INSTRUCTION = load_system_prompt(SYSTEM_PROMPT_FILE)

# ---------------- Vertex AI ---------------- #
vertexai.init(
    project=PROJECT_ID,
    location=LOCATION
)

model = GenerativeModel(LLM_MODEL)

# ---------------- FastMCP ---------------- #
mcp = FastMCP(name="nostradamus_copilot_mcp")

# --- Caching ---
# Cache for 10 minutes with a max size of 500 entries
# query_cache = TTLCache(maxsize=500, ttl=600)

# At module level
bigquery_client = bigquery.Client(project=PROJECT_ID, location=LOCATION)

# TABLE_NAME = f"{PROJECT_ID}.{DATASET_ID_HIST}.{TABLE_ID_HIST}"

# raw_schema_sample = get_schema_and_sample(TABLE_NAME)
# TABLE_SCHEMA_SAMPLE = sanitize_payload(raw_schema_sample)
# logger.debug(f"TABLE_SCHEMA_SAMPLE sanitized: type={type(TABLE_SCHEMA_SAMPLE)}")


# ---------------- TOOLS ---------------- #
# Relevance check
# @mcp.tool()
# def check_relevance(question: str, ) -> dict:
#     """
#     Checks if the question is relevant to the available data.
#     Returns YES/NO in a structured dict.
#     """
#     try:
#         prompt = f"""
#         You are a data assistant.
#         User question: "{question}"
#         Table schema: {TABLE_SCHEMA_SAMPLE}

#         Can this question be answered using this table? YES or NO only.
#         """
#         resp = model.generate_content(prompt)
#         answer = resp.candidates[0].content.parts[0].text.strip().upper()
#         is_relevant = "YES" in answer
#         return {"relevant": is_relevant}
#     except Exception as e:
#         logger.error(f"Error in relevance check: {e}", exc_info=True)
#         return {"relevant": False}


@mcp.tool()
def get_data(question: str, dataset_id: str, table_ids: list, descriptions: list) -> dict:
    
    # --- Step 1: Prepare context ---
    context = []

    for tbl_id, desc in zip(table_ids, descriptions):
        # Build fully-qualified table name
        full_table_id = f"{PROJECT_ID}.{dataset_id}.{tbl_id}"

        # Fetch and sanitize schema + sample for this table
        schema_sample = sanitize_payload(get_schema_and_sample(full_table_id))

        # Append to context
        context.append({
            "table_id": full_table_id,
            "description": desc,
            "schema": schema_sample
        })
    context_str = json.dumps(context, indent=2)
    
    # --- Step 2: Construct prompt ---
    prompt = f"""
        You are a data routing assistant.
        The user asked: "{question}"

        Below are the available tables with their descriptions and schemas:
        {context_str}

        Choose the single most relevant table for answering this question.
        Respond ONLY with the full table_id string in this exact format:
        <project_id>.<dataset_id>.<table_id>

        Do NOT include any SQL keywords, explanations, markdown, or extra text.
        """
    
    try:
        # --- Step 3: Generate content from model ---
        resp = model.generate_content(prompt)
        cleaned_output = resp.candidates[0].content.parts[0].text.strip()
        selected_table = clean_sql_fences(cleaned_output).replace("`", "").strip()

        logger.info(f"🧭 LLM selected table: {selected_table}")

        # Step 3: Try to extract a valid table_id from any noisy output
        # Regex to capture something like analytics-datapipeline-prod.dataset.table_name
        match = re.search(
            r"([a-zA-Z0-9_-]+\.[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+)",
            cleaned_output
        )

        if match:
            selected_table = match.group(1)
        else:
            selected_table = cleaned_output

        logger.info(f"🧭 LLM selected table candidate: {selected_table}")

        # Step 4: Match with known tables (case-insensitive)
        matched = next(
            (t for t in context if t["table_id"].lower() == selected_table.lower()),
            None
        )

        # Step 5: Return schema if found, otherwise fallback
        if matched:
            return {"table_id": matched["table_id"], "schema": matched["schema"]}
        else:
            # Try partial match on table name
            partial_match = next(
                (t for t in context if t["table_id"].split(".")[-1].lower() in selected_table.lower()),
                None
            )
            if partial_match:
                return {"table_id": partial_match["table_id"], "schema": partial_match["schema"]}
            else:
                logger.warning(f"No matching table found for model output: {cleaned_output}")
                return {"error": "Model selected an unknown table_id", "raw_output": cleaned_output}


    except Exception as e:
        logger.error(f"❌ Error in get_data(): {e}", exc_info=True)
        return {"error": str(e)}

# Generate SQL from user question
@mcp.tool()
def generate_sql(question: str, tablename: str, table_schema, history: list = None) -> dict:
    """
    Generates SQL or a clarifying question using Gemini LLM based on the user question.
    It can also take conversation history for context.
    Returns a dictionary indicating if the content is SQL and the content itself.
    """
    try:
        context_str = ""
        if history:
            context_parts = []
            for q, a in history:
                context_parts.append(f"Previous Question: {q}")
                context_parts.append(f"Previous Answer: {a}")
            context_str = "\n\n".join(context_parts)

        prompt = f"""
        {context_str}
        Table: {tablename}
        User question: '{question}'
        Table schema: '''{table_schema}'''
        Instructions : {SYSTEM_INSTRUCTION}
        """

        resp = model.generate_content(prompt)
        content_raw = resp.candidates[0].content.parts[0].text.strip()
        content = clean_sql_fences(content_raw)

        # Check if the generated content is a SQL query
        content_upper = content.strip().upper()
        is_sql = content_upper.startswith("SELECT") or content_upper.startswith("WITH")

        logger.info(f"Generated content (is_sql={is_sql}):\n{content}")
        return {"is_sql": is_sql, "content": content}

    except Exception as e:
        logger.error(f"Error generating SQL: {e}", exc_info=True)
        return {"error": str(e)}

# Reflection Agent
@mcp.tool()
def reflect_sql(question: str, sql_query: str) -> dict:
    """
    Optional reflection to validate SQL correctness and alignment with the question.
    """
    try:
        prompt = f"""
        You are a SQL reviewer.
        User question: "{question}"
        Generated SQL: {sql_query}

        Check if this SQL:
        1. Is valid
        2. Queries only allowed tables/columns
        3. Correctly answers the question

        Return "VALID" if correct, otherwise suggest corrections.
        """
        resp = model.generate_content(prompt)
        review = resp.candidates[0].content.parts[0].text.strip()
        is_valid = "VALID" in review.upper()
        return {"valid": is_valid, "review": review}
    except Exception as e:
        logger.error(f"Error in SQL reflection: {e}", exc_info=True)
        return {"valid": False, "review": str(e)}

# Execute SQL and return raw results
@mcp.tool()
def execute_sql(sql_query: str) -> dict:
    """
    Executes the SQL query in BigQuery and returns results.
    """
    try:
        result = run_query(query=sql_query)
        return {"error_message": None, "raw_result": result}
    except Exception as e:
        logger.error(f"Error executing SQL: {e}", exc_info=True)
        return {"error_message": str(e), "raw_result": sql_query}

# Generate final natural language answer
@mcp.tool()
def generate_final_answer(question: str, sql_query: str, raw_result: list) -> dict:
    """
    Generates human-readable answer using Gemini LLM.
    Formats as markdown if result is tabular.
    """
    try:
        prompt = f"""
        You are an AI assistant.
        User question: "{question}"
        SQL result: {raw_result}

        Understand the user's question and generate a concise, human-readable answer accordingly. Do not summarize the results when not asked. 
        Do not change the numbers - just answer exactly how it came from the SQL query.
        If the result is tabular or user asked for some comparison, trend, growth -  format it in tabular markdown format.
        """
        resp = model.generate_content(prompt)
        final_answer = resp.candidates[0].content.parts[0].text.strip()
        return {"answer": final_answer}
    
    except Exception as e:
        
        error_msg = str(e)
        # Handle token/input size-related errors gracefully
        if any(
            kw in error_msg.lower()
            for kw in [
                "token", "context_length", "too large", "max length", "content too long", "quota"
            ]
        ):
            user_friendly_msg = (
                "The question you asked is producing a very big result which we are unable to handle at the moment. "
                "Please rephrase the question and try again."
            )
            logger.warning(f"Token limit error: {error_msg}")
            return {"error": user_friendly_msg}

        # Generic fallback for any other exceptions
        logger.error(f"Error generating SQL: {e}", exc_info=True)
        return {"error": f"Error generating SQL: {error_msg}"}



# ---------------- MCP Orchestration ---------------- #

from asyncio import Lock
_active_questions = {}


async def handle_user_question(user_question: str, history: list) -> dict:
    """
    Orchestrates the tools for a user question.
    This is a generator that yields status updates.
    """

    if user_question in _active_questions:
        logger.info(f"Duplicate active request for: '{user_question}', ignoring second call.")
        return  # or yield cached progress, or just silently skip

    start_time = time.time()
    logger.info(f"Handling user question: '{user_question}'")

    logger.info(f"History : {history}")
    
    sql_query = ""

    lock = Lock()
    _active_questions[user_question] = lock
    
    async with lock:
        try:
            async with Client(mcp) as client:
                # 0. Check Cache
                # cache_key = (user_question, tuple(map(tuple, history)))
                # if cache_key in query_cache:
                #     logger.info(f"Cache hit for question: '{user_question}'")
                #     cached_result = query_cache[cache_key]
                #     yield {"status": "final", "result": cached_result}
                #     return
                # logger.info(f"Cache miss for question: '{user_question}'")

                # # 1. Relevance check
                # yield {"status": "progress", "message": "⏳ Checking user query relevance with the data..."}
                # step_start = time.time()
                # relevance = await client.call_tool("check_relevance", {"question": user_question})
                # logger.info(f"Step 1 (check_relevance) took: {time.time() - step_start:.2f}s")
                # if not relevance.structured_content.get("relevant", False):
                #     return {"answer": "Sorry, your question is outside the scope of the available data. Please ask questions related to the table."}
                
                yield {"status": "progress", "message": "⏳ Selecting Relevant database..."}
                step_start = time.time()
                data_output = await client.call_tool("get_data", {"question": user_question, "dataset_id": DATASET_ID, "table_ids": TABLE_IDS, "descriptions": DISCRIPTIONS})
                logger.info(f"Table selection output: {data_output.structured_content}")
                logger.info(f"Step 1 (select_data) took: {time.time() - step_start:.2f}s")

                # Extract chosen table and schema
                table_id = data_output.structured_content.get("table_id")
                table_schema = data_output.structured_content.get("schema")

                if not table_id or not table_schema:
                    yield {"status": "error", "message": "No valid table selected by the model."}
                    return

                # 2. Generate SQL
                yield {"status": "progress", "message": "⏳ Generating SQL query..."}
                step_start = time.time()
                
                sql_result = await client.call_tool("generate_sql", {"question": user_question, "tablename": table_id, "table_schema": table_schema, "history": history})
                is_sql = sql_result.structured_content.get("is_sql")
                content = sql_result.structured_content.get("content")
                logger.info(f"Step 2 (generate_sql) took: {time.time() - step_start:.2f}s")

                if not is_sql:
                    logger.warning("Generated content is not a valid SQL query. Returning as answer.")
                    yield {
                        "status": "final",
                        "result": {"answer": content, "sql_query": ""},
                    }
                    return
                
                sql_query = content

                # # 3. Optional Reflection Agent
                # yield {"status": "progress", "message": "⏳Validating SQL query generated..."}
                # step_start = time.time()
                # reflection = await client.call_tool("reflect_sql", {"question": user_question, "sql_query": sql_query})
                # logger.info(f"Step 3 (reflect_sql) took: {time.time() - step_start:.2f}s")
                # if not reflection.structured_content.get("valid", False):
                #     return {"answer": f"Generated SQL failed validation: {reflection.structured_content.get('review')}"}
                # logger.info(f"Reflection agent validated {reflection.structured_content.get('review')}")

                # 4. Execute SQL
                yield {"status": "progress", "message": "⏳ Executing SQL query..."}
                step_start = time.time()
                try:
                    execution_result = await client.call_tool("execute_sql", {"sql_query": sql_query})
                    error_message = execution_result.structured_content.get("error_message")
                    if error_message:
                        error_payload = f'Error in executing - {error_message}'
                        yield {"status": "error", "message": error_payload}
                        return
                    
                    raw_data = execution_result.structured_content.get("raw_result")
                    logger.info(f"Step 4 (execute_sql) took: {time.time() - step_start:.2f}s")
                    logger.info(f"Executed SQL, Result: {raw_data}")
                except Exception as e:
                    error_payload = f'Error in executing - {e}'
                    yield {"status": "error", "message": error_payload}
                    return

                # 5. Generate final answer
                yield {"status": "progress", "message": "⏳ Generating final answer..."}
                step_start = time.time()
                final_answer = await client.call_tool(
                    "generate_final_answer",
                    {"question": user_question, "sql_query": sql_query, "raw_result": raw_data}
                )
                logger.info(f"Step 5 (generate_final_answer) took: {time.time() - step_start:.2f}s")
                logger.info(f"Total orchestration time: {time.time() - start_time:.2f}s")
                
                final_result = final_answer.structured_content
                final_result['sql_query'] = sql_query
                logger.info(f"Final Answer: {final_result}")

                # Cache the final result
                # query_cache[cache_key] = final_result

                yield {"status": "final", "result": final_result}
                                                                                                                                                                                                
        except Exception as e:
            logger.error(f"Error running MCP orchestration: {e}", exc_info=True)
            yield {"status": "error", "message": str(e)}
            yield {"status": "final", "result": {"answer": f"An error occurred: {e}", "sql_query": sql_query}}
        
        finally:
            _active_questions.pop(user_question, None)