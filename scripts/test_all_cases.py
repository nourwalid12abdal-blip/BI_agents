#!/usr/bin/env python3
# scripts/test_all_cases.py
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import requests
import json

BASE_URL = "http://localhost:8000"

TEST_CASES = [
    # Basic tests
    {
        "question": "list all customers",
        "expected_entities": ["customers"],
        "expected_sql_patterns": ["SELECT", "FROM customers"],
        "expected_row_count_range": (10, 15),
        "expected_chart_type": None,
        "test": "single table",
    },
    {
        "question": "order list of customers",
        "expected_entities": ["customers", "orders"],
        "expected_sql_patterns": ["SELECT", "FROM customers", "JOIN", "FROM orders"],
        "expected_row_count_range": (30, 40),
        "expected_chart_type": None,
        "test": "two table join",
    },
    {
        "question": "total orders per customer",
        "expected_entities": ["customers", "orders"],
        "expected_sql_patterns": [
            "SELECT",
            "FROM customers",
            "JOIN",
            "GROUP BY",
            "COUNT",
        ],
        "expected_row_count_range": (10, 15),
        "expected_chart_type": "bar",
        "expected_x_column": "name",
        "expected_y_column": "total_orders",
        "test": "aggregation with join",
    },
    # Intermediate tests
    {
        "question": "products that have never been ordered",
        "expected_entities": ["products", "orders"],
        "expected_sql_patterns": [
            "SELECT",
            "FROM products",
            "LEFT JOIN",
            "WHERE.*NULL",
        ],
        "expected_row_count_range": (1, 10),
        "expected_chart_type": "bar",
        "test": "LEFT JOIN WHERE NULL",
    },
    {
        "question": "customers who spent more than 100",
        "expected_entities": ["customers", "orders", "products"],
        "expected_sql_patterns": [
            "SELECT",
            "FROM customers",
            "JOIN",
            "HAVING",
            ">.*100",
        ],
        "expected_row_count_range": (1, 10),
        "expected_chart_type": "bar",
        "test": "3-table join with HAVING",
    },
    {
        "question": "most popular product",
        "expected_entities": ["orders", "products"],
        "expected_sql_patterns": [
            "SELECT",
            "FROM products",
            "JOIN",
            "GROUP BY",
            "ORDER BY",
            "DESC",
            "LIMIT",
        ],
        "expected_row_count_range": (1, 5),
        "expected_chart_type": "bar",
        "test": "aggregation ranking",
    },
    {
        "question": "customer with most orders",
        "expected_entities": ["customers", "orders"],
        "expected_sql_patterns": [
            "SELECT",
            "FROM customers",
            "JOIN",
            "GROUP BY",
            "ORDER BY",
            "DESC",
            "LIMIT",
        ],
        "expected_row_count_range": (1, 3),
        "expected_chart_type": "kpi",
        "test": "ORDER BY LIMIT",
    },
    {
        "question": "average order value per customer",
        "expected_entities": ["customers", "orders", "products"],
        "expected_sql_patterns": [
            "SELECT",
            "FROM customers",
            "JOIN",
            "GROUP BY",
            "AVG",
        ],
        "expected_row_count_range": (10, 15),
        "expected_chart_type": "bar",
        "test": "3-table AVG",
    },
    {
        "question": "products under 500 with their order counts",
        "expected_entities": ["products", "orders"],
        "expected_sql_patterns": [
            "SELECT",
            "FROM products",
            "JOIN",
            "WHERE",
            "GROUP BY",
        ],
        "expected_row_count_range": (1, 15),
        "expected_chart_type": "bar",
        "test": "WHERE + GROUP BY",
    },
    # Advanced tests
    {
        "question": "top 3 customers by total spending",
        "expected_entities": ["customers", "orders", "products"],
        "expected_sql_patterns": [
            "SELECT",
            "FROM customers",
            "JOIN",
            "ORDER BY",
            "DESC",
            "LIMIT 3",
        ],
        "expected_row_count_range": (1, 5),
        "expected_chart_type": "bar",
        "test": "top N with sum",
    },
    {
        "question": "monthly revenue trend",
        "expected_entities": ["orders", "products"],
        "expected_sql_patterns": ["SELECT", "FROM orders", "GROUP BY", "strftime"],
        "expected_row_count_range": (1, 12),
        "expected_chart_type": "line",
        "test": "date grouping",
    },
    {
        "question": "products ordered by more than 2 customers",
        "expected_entities": ["products", "orders", "customers"],
        "expected_sql_patterns": [
            "SELECT",
            "FROM products",
            "JOIN",
            "GROUP BY",
            "HAVING",
            "COUNT",
        ],
        "expected_row_count_range": (1, 10),
        "expected_chart_type": "bar",
        "test": "HAVING COUNT",
    },
    {
        "question": "customers with zero orders",
        "expected_entities": ["customers", "orders"],
        "expected_sql_patterns": [
            "SELECT",
            "FROM customers",
            "LEFT JOIN",
            "WHERE.*NULL",
        ],
        "expected_row_count_range": (1, 10),
        "expected_chart_type": "bar",
        "test": "LEFT JOIN NULL",
    },
    # Edge cases
    {
        "question": "show me everything",
        "expected_entities": ["customers", "orders", "products"],
        "expected_sql_patterns": ["SELECT"],
        "expected_row_count_range": (10, 40),
        "expected_chart_type": None,
        "test": "ambiguous",
    },
    # MongoDB tests
    {
        "question": "show me user login events",
        "expected_entities": ["user_events"],
        "expected_source": "mongo",
        "expected_sql_patterns": ["$match"],
        "expected_row_count_range": (1, 10),
        "expected_chart_type": None,
        "test": "MongoDB filter",
    },
    {
        "question": "count user logins by device",
        "expected_entities": ["user_events"],
        "expected_source": "mongo",
        "expected_sql_patterns": ["$group", "$count"],
        "expected_row_count_range": (1, 10),
        "expected_chart_type": "bar",
        "test": "MongoDB aggregation",
    },
    {
        "question": "average rating per product",
        "expected_entities": ["product_reviews"],
        "expected_source": "mongo",
        "expected_sql_patterns": ["$group", "$avg"],
        "expected_row_count_range": (1, 15),
        "expected_chart_type": "bar",
        "test": "MongoDB aggregation",
    },
]


def test_question(question: str):
    """Send a question to the API and return the result."""
    try:
        response = requests.post(
            f"{BASE_URL}/ask",
            json={"question": question, "needs_chart": True},
            timeout=60,
        )
        if response.status_code == 200:
            return response.json()
        else:
            return {"error": f"HTTP {response.status_code}", "detail": response.text}
    except Exception as e:
        return {"error": str(e)}


def main():
    print("=" * 80)
    print("RUNNING ALL TEST CASES")
    print("=" * 80)

    results = []
    for i, test in enumerate(TEST_CASES, 1):
        print(f"\n[{i}/{len(TEST_CASES)}] Testing: {test['question']}")
        print(f"       Test type: {test['test']}")

        result = test_question(test["question"])

        if "error" in result:
            print(f"       ❌ ERROR: {result['error']}")
            status = "FAIL"
        else:
            entities = result.get("entities", [])
            query = result.get("query", "")
            success = result.get("success", False)

            if success:
                print(f"       ✅ Entities: {entities}")
                print(f"       ✅ Query: {query[:100]}...")
                status = "PASS"
            else:
                print(f"       ❌ Failed: {result.get('error', 'Unknown')}")
                status = "FAIL"

        results.append(
            {
                "test_num": i,
                "question": test["question"],
                "test_type": test["test"],
                "status": status,
                "result": result,
            }
        )

        print(f"       Status: {status}")

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    passed = sum(1 for r in results if r["status"] == "PASS")
    failed = sum(1 for r in results if r["status"] == "FAIL")

    print(f"Total: {len(results)} | Passed: {passed} | Failed: {failed}")

    if failed > 0:
        print("\nFailed tests:")
        for r in results:
            if r["status"] == "FAIL":
                print(
                    f"  - {r['question']}: {r['result'].get('error', 'Unknown error')}"
                )

    return passed, failed


if __name__ == "__main__":
    main()
