from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
from server.mcp_handler import handle_user_question
import json
import os
from server.logger import get_logger

logger = get_logger()
logger.info("Backend server initialized.")

app = FastAPI(title="MCP Server with Tools")

@app.get("/")
def root():
    return {"message": "MCP Server running"}

async def response_generator(user_question: str, history: list):
    """Wraps the handler generator to stream JSON objects."""
    async for chunk in handle_user_question(user_question, history):
        yield f"data: {json.dumps(chunk)}\n\n"

@app.post("/mcp/query")
async def mcp_query(request: Request):
    data = await request.json()
    user_question = data.get("question", "").strip()
    history = data.get("history", [])
    if not user_question:
        return JSONResponse(
            status_code=400,
            content={"status": "error", "message": "No question provided"}
        )

    return StreamingResponse(response_generator(user_question, history), media_type="text/event-stream")
