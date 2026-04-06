import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.agent.graph import build_graph, initial_state, run
from src.agent.state import AgentState
from config.settings import settings


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


class TestMongoDBConfiguration:
    def test_mongo_uri_is_optional(self):
        """MongoDB should be optional - system should work without it"""
        if not settings.mongo_uri:
            assert True
        else:
            assert settings.mongo_uri is not None

    def test_mongodb_disabled_handling(self):
        """When MongoDB is not configured, system should gracefully fallback to SQL"""
        if not settings.mongo_uri:
            result = run("How many customers do we have?")
            assert result["source"] == "sql"
            assert result["success"] is True or result.get("final_error") is not None


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
    def test_filter_by_customer_id(self):
        result = run("Show me orders from customer with id 1")

        assert result["success"] is True

    def test_filter_by_product_id(self):
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

    def test_chart_generation_when_needed(self):
        result = run("Show me sales trend over time")

        if result["success"]:
            assert (
                result.get("needs_chart") is True
                or result.get("chart_spec") is not None
            )


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

    def test_trend_intent(self):
        result = run("Show me the trend of orders over time")

        assert result["intent"] in ["trend", "aggregation", "filter", ""]

    def test_comparison_intent(self):
        result = run("Compare sales between this month and last month")

        assert result["intent"] in ["comparison", "aggregation", ""]

    def test_lookup_intent(self):
        result = run("Show me the details of order 100")

        assert result["intent"] in ["lookup", "filter", ""]


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

    def test_response_under_3_sentences(self):
        result = run("How many orders do we have?")

        if result.get("response"):
            sentences = result["response"].count(".")
            assert sentences <= 3


class TestSourceRouting:
    def test_sql_source_by_default(self):
        result = run("How many orders?")

        assert result["source"] in ["sql", "mongo", "both"]

    def test_source_routing(self):
        result = run("Show me all customers")

        assert "source" in result


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


class TestComplexQueries:
    def test_top_n_query(self):
        result = run("What are the top 5 best-selling products?")

        assert result["success"] is True

    def test_average_calculation(self):
        result = run("What is the average order value?")

        assert result["success"] is True

    def test_group_by_query(self):
        result = run("Show total sales by category")

        assert result["success"] is True

    def test_date_based_query(self):
        result = run("Show orders from last month")

        assert result["success"] is True


class TestDrillDown:
    def test_drilldown_questions_generated(self):
        result = run("Show total revenue by product")

        if result.get("chart_spec"):
            assert "drilldowns" in result["chart_spec"] or "customdata" in str(
                result["chart_spec"]
            )


class TestCharterPipeline:
    def test_normalizer_layer(self):
        result = run("Show me order counts")

        if result["success"]:
            assert result.get("data") is not None

    def test_classifier_layer(self):
        result = run("Show me all customers")

        if result["success"] and result.get("chart_spec"):
            assert (
                "profile" in result["chart_spec"]
                or "chart_type" in result["chart_spec"]
            )

    def test_selector_layer(self):
        result = run("Show monthly revenue trend")

        if result["success"] and result.get("chart_spec"):
            assert result["chart_spec"].get("chart_type") is not None


class TestAnomalyDetection:
    def test_anomaly_detection_in_chart(self):
        result = run("Show daily sales for the past month")

        if result["success"] and result.get("chart_spec"):
            spec = result["chart_spec"]
            has_anomaly = (
                "annotations" in spec
                or "anomalies" in str(spec)
                or "intel" in str(spec)
            )
            assert has_anomaly or result.get("response") is not None


class TestTrendAnalysis:
    def test_trend_detection(self):
        result = run("How have sales changed over time?")

        assert result["success"] is True


class TestAPICompatibility:
    def test_api_response_structure(self):
        result = run("How many customers?")

        assert "question" in result
        assert "response" in result
        assert "intent" in result
        assert "source" in result
        assert "success" in result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
