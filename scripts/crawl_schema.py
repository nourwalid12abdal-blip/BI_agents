# scripts/crawl_schema.py
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.schema.crawler import crawl
from src.schema.schema_store import save, get_schema_summary_for_llm

if __name__ == "__main__":
    print("Starting schema crawl...\n")
    graph = crawl()
    path  = save(graph)
    print(f"Graph saved to: {path}")
    print("\n" + get_schema_summary_for_llm(graph))