# src/agent/llm.py

from langchain_openai import ChatOpenAI
from config.settings import settings
from functools import lru_cache


@lru_cache(maxsize=1)
def get_llm(temperature: float = 0.0) -> ChatOpenAI:
    """
    Returns a cached LangChain ChatOpenAI instance pointed at Groq.

    Every node that needs an LLM calls this function.
    Never instantiate ChatOpenAI directly in a node file.

    lru_cache means the model is initialized once per process —
    not rebuilt on every question.

    Args:
        temperature: 0.0 for understand + planners (deterministic)
                     0.1 for format + charter (slightly more natural)

    Models available on Groq (free tier):
        llama-3.3-70b-versatile   ← best overall, recommended
        llama-3.1-8b-instant      ← fastest, lower quality
        mixtral-8x7b-32768        ← good for long context
        gemma2-9b-it              ← lightweight alternative
    """
    return ChatOpenAI(
        model=settings.groq_model,
        openai_api_key=settings.groq_api_key,
        openai_api_base="https://api.groq.com/openai/v1",
        temperature=temperature,
        max_tokens=1024,
    )
