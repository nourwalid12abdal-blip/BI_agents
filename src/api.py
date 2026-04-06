# src/api.py
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from src.agent.graph import build_graph, initial_state
import logging
import uvicorn

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="BI Agent API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

graph = build_graph()


class AskRequest(BaseModel):
    question:    str
    needs_chart: bool = True


class AskResponse(BaseModel):
    question:   str
    response:   str
    chart_spec: dict | None = None
    intent:     str  = ""
    source:     str  = ""
    success:    bool = True
    error:      str | None = None


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/ask", response_model=AskResponse)
def ask(req: AskRequest):
    logger.info(f"[/ask] question='{req.question}'")
    try:
        state = initial_state(req.question)
        state["needs_chart"] = req.needs_chart
        result = graph.invoke(state)

        return AskResponse(
            question=req.question,
            response=result.get("response") or "",
            chart_spec=result.get("chart_spec"),
            intent=result.get("intent", ""),
            source=result.get("source", ""),
            success=result.get("success", False),
            error=result.get("final_error"),
        )
    except Exception as e:
        logger.error(f"[/ask] failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run("src.api:app", host="0.0.0.0", port=8000, reload=True)