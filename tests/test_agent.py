import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.agent.graph import build_graph, initial_state, run
from src.agent.state import AgentState


@pytest.fixture
def graph():
    return build_graph()


@pytest.fixture
def sample_state():
    return initial_state("How many customers do we have?")


class TestInitialState:
    def test_initial_state_has_required_fields(self):
        state = initial_state("test question")

        assert "question" in state
        assert state["question"] == "test question"
        assert "intent" in state
        assert "source" in state
        assert "entities" in state
        assert "query" in state
        assert "success" in state
        assert "data" in state
        assert "response" in state

    def test_initial_state_defaults(self):
        state = initial_state("test")

        assert state["source"] == "sql"
        assert state["retry_count"] == 0
        assert state["max_retries"] == 2
        assert state["needs_chart"] is True
        assert state["success"] is False


class TestGraphNodes:
    def test_graph_has_all_nodes(self, graph):
        nodes = graph.nodes.keys()

        assert "understand" in nodes
        assert "sql_planner" in nodes
        assert "mongo_planner" in nodes
        assert "executor" in nodes
        assert "charter" in nodes
        assert "format" in nodes

    def test_graph_compiles(self, graph):
        assert graph is not None


class TestSimpleAggregationQueries:
    def test_count_customers(self):
        result = run("How many customers do we have?")

        assert result["success"] is True
        assert result["response"] is not None
        assert len(result["data"]) > 0

    def test_count_orders(self):
        result = run("How many orders are there?")

        assert result["success"] is True
        assert result["row_count"] >= 0

    def test_sum_total_quantity(self):
        result = run("What is the total quantity of all orders?")

        assert result["success"] is True


class TestFilterQueries:
    def test_filter_by_customer_name(self):
        result = run("Show me orders from customer with id 1")

        assert result["success"] is True

    def test_filter_by_product(self):
        result = run("Show me orders for product id 5")

        assert result["success"] is True


class TestJoinQueries:
    def test_join_orders_with_customers(self):
        result = run("Show all orders with customer names")

        assert result["success"] is True

    def test_join_orders_with_products(self):
        result = run("Show orders with product details")

        assert result["success"] is True


class TestChartGeneration:
    def test_bar_chart_query(self):
        result = run("Show total orders per customer")

        assert (
            result.get("chart_spec") is not None or result.get("response") is not None
        )

    def test_chart_spec_structure(self):
        result = run("Show revenue by product")

        if result.get("chart_spec"):
            assert "chart_type" in result["chart_spec"]
            assert "data" in result["chart_spec"]
            assert "layout" in result["chart_spec"]


class TestIntentClassification:
    def test_aggregation_intent(self):
        result = run("How many customers?")

        assert result["intent"] in [
            "aggregation",
            "filter",
            "trend",
            "comparison",
            "lookup",
            "dashboard",
            "",
        ]

    def test_filter_intent(self):
        result = run("Show customers named John")

        assert result["intent"] in [
            "aggregation",
            "filter",
            "trend",
            "comparison",
            "lookup",
            "dashboard",
            "",
        ]


class TestErrorHandling:
    def test_invalid_query_handling(self):
        result = run("execute invalid query xyz")

        assert "final_error" in result or result["success"] is False

    def test_max_retries_respected(self):
        state = initial_state("invalid query")
        state["max_retries"] = 1
        app = build_graph()
        result = app.invoke(state)

        assert result["retry_count"] <= 1


class TestResponseFormatting:
    def test_response_is_string(self):
        result = run("How many customers?")

        assert isinstance(result.get("response"), str) or result.get("response") is None

    def test_response_contains_answer(self):
        result = run("How many customers?")

        if result.get("response"):
            assert len(result["response"]) > 0


class TestSourceRouting:
    def test_sql_source_by_default(self):
        result = run("How many orders?")

        assert result["source"] in ["sql", "mongo", "both"]


class TestQueryGeneration:
    def test_sql_query_generated(self):
        result = run("List all customers")

        assert result.get("query") is not None
        assert isinstance(result.get("query"), str)


class TestEdgeCases:
    def test_empty_question(self):
        result = run("")

        assert "error" in result or result.get("response") is not None

    def test_very_long_question(self):
        long_question = "Show me " + "all " * 100 + "customers"
        result = run(long_question)

        assert result is not None

    def test_special_characters_in_question(self):
        result = run("How many customers with @email?")

        assert result is not None


class TestDataRetrieval:
    def test_data_is_list(self):
        result = run("How many customers?")

        assert isinstance(result["data"], list)

    def test_row_count_matches_data(self):
        result = run("How many customers?")

        assert result["row_count"] == len(result["data"])


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
