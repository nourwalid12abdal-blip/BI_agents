# scripts/seed_complex_data.py
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from config.settings import settings
from datetime import datetime, timedelta


def seed_complex_sql():
    engine = create_engine(settings.sql_db_url)
    with engine.connect() as conn:
        conn.execute(
            text("""
            CREATE TABLE IF NOT EXISTS customers (
                id    INTEGER PRIMARY KEY,
                name  TEXT NOT NULL,
                email TEXT NOT NULL
            )
        """)
        )
        conn.execute(
            text("""
            CREATE TABLE IF NOT EXISTS products (
                id    INTEGER PRIMARY KEY,
                name  TEXT NOT NULL,
                price REAL NOT NULL
            )
        """)
        )
        conn.execute(
            text("""
            CREATE TABLE IF NOT EXISTS orders (
                id          INTEGER PRIMARY KEY,
                customer_id INTEGER REFERENCES customers(id),
                product_id  INTEGER REFERENCES products(id),
                quantity    INTEGER,
                created_at  TEXT
            )
        """)
        )

        for table in ["orders", "products", "customers"]:
            conn.execute(text(f"DELETE FROM {table}"))

        customers = [
            (1, "Alice", "alice@example.com"),
            (2, "Bob", "bob@example.com"),
            (3, "Sara", "sara@example.com"),
            (4, "Mike", "mike@example.com"),
            (5, "Lena", "lena@example.com"),
            (6, "John", "john@example.com"),
            (7, "Emma", "emma@example.com"),
            (8, "David", "david@example.com"),
            (9, "Olivia", "olivia@example.com"),
            (10, "James", "james@example.com"),
            (11, "Sophia", "sophia@example.com"),
            (12, "William", "william@example.com"),
        ]
        conn.execute(
            text(
                "INSERT INTO customers (id,name,email) VALUES "
                + ",".join(
                    f"({id},'{name}','{email}')" for id, name, email in customers
                )
            )
        )

        products = [
            (1, "MacBook Pro", 2499.99),
            (2, "iPhone 15", 999.99),
            (3, "iPad Air", 599.99),
            (4, "AirPods Pro", 249.99),
            (5, "Apple Watch", 399.99),
            (6, "Dell XPS 15", 1799.99),
            (7, "Samsung TV", 1299.99),
            (8, "Sony Headphones", 349.99),
            (9, "Kindle", 139.99),
            (10, "Nintendo Switch", 299.99),
            (11, "PlayStation 5", 499.99),
            (12, "Webcam HD", 89.99),
            (13, "Mechanical Keyboard", 159.99),
            (14, "Gaming Mouse", 79.99),
            (15, "USB-C Hub", 49.99),
        ]
        conn.execute(
            text(
                "INSERT INTO products (id,name,price) VALUES "
                + ",".join(f"({id},'{name}',{price})" for id, name, price in products)
            )
        )

        orders = [
            (1, 1, 1, 1, "2024-01-05"),
            (2, 1, 4, 2, "2024-01-10"),
            (3, 1, 11, 1, "2024-01-15"),
            (4, 2, 2, 1, "2024-01-08"),
            (5, 2, 6, 1, "2024-01-20"),
            (6, 3, 3, 2, "2024-01-12"),
            (7, 3, 5, 1, "2024-02-01"),
            (8, 5, 10, 1, "2024-01-25"),
            (9, 5, 14, 2, "2024-02-05"),
            (10, 6, 7, 1, "2024-02-10"),
            (11, 6, 8, 1, "2024-02-15"),
            (12, 7, 9, 3, "2024-02-20"),
            (13, 7, 13, 1, "2024-02-25"),
            (14, 8, 12, 2, "2024-03-01"),
            (15, 8, 15, 4, "2024-03-05"),
            (16, 9, 1, 1, "2024-03-10"),
            (17, 9, 2, 1, "2024-03-12"),
            (18, 9, 4, 2, "2024-03-15"),
            (19, 10, 5, 1, "2024-03-20"),
            (20, 10, 3, 1, "2024-03-22"),
            (21, 11, 6, 1, "2024-03-25"),
            (22, 11, 11, 1, "2024-03-28"),
            (23, 12, 7, 2, "2024-04-01"),
            (24, 1, 2, 1, "2024-04-05"),
            (25, 2, 3, 1, "2024-04-10"),
            (26, 3, 1, 1, "2024-04-15"),
            (27, 5, 4, 1, "2024-04-20"),
            (28, 6, 9, 2, "2024-04-25"),
            (29, 7, 10, 1, "2024-05-01"),
            (30, 8, 5, 1, "2024-05-05"),
            (31, 9, 6, 1, "2024-05-10"),
            (32, 10, 8, 1, "2024-05-15"),
            (33, 11, 12, 3, "2024-05-20"),
            (34, 12, 14, 2, "2024-05-25"),
            (35, 1, 15, 5, "2024-06-01"),
        ]
        conn.execute(
            text(
                "INSERT INTO orders (id,customer_id,product_id,quantity,created_at) VALUES "
                + ",".join(f"({o[0]},{o[1]},{o[2]},{o[3]},'{o[4]}')" for o in orders)
            )
        )
        conn.commit()

    print("Complex SQL seeded:")
    print("  - 12 customers")
    print("  - 15 products")
    print("  - 35 orders")
    print("  - Customers with 0 orders: #4 (Mike)")
    print("  - Products never ordered: #16+ (none in range)")


def print_tables():
    engine = create_engine(settings.sql_db_url)
    with engine.connect() as conn:
        for table in ["customers", "products", "orders"]:
            result = conn.execute(text(f"SELECT * FROM {table}"))
            rows = result.fetchall()
            print(f"\n=== {table.upper()} ({len(rows)} rows) ===")
            print(" | ".join(result.keys()))
            for row in rows[:5]:
                print(" | ".join(str(item) for item in row))
            if len(rows) > 5:
                print(f"... and {len(rows) - 5} more rows")


if __name__ == "__main__":
    seed_complex_sql()
    print_tables()
