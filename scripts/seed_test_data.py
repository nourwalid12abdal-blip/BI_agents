# scripts/seed_test_data.py
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from pymongo import MongoClient
from config.settings import settings
from datetime import datetime, timedelta


# -----------------------
# SQL SEEDING
# -----------------------
def seed_sql():
    engine = create_engine(settings.sql_db_url)
    with engine.connect() as conn:
        # --- Create tables ---
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

        # --- Clear old data ---
        for table in ["orders", "products", "customers"]:
            conn.execute(text(f"DELETE FROM {table}"))

        # --- Seed customers ---
        customers = [
            (1, "Alice", "alice@example.com"),
            (2, "Bob", "bob@example.com"),
            (3, "Sara", "sara@example.com"),
            (4, "Mike", "mike@example.com"),  # Will have NO orders
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

        # --- Seed products ---
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

        # --- Seed orders ---
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
    print("SQL seeded — 3 tables, 35 orders inserted")


# -----------------------
# MONGODB SEEDING
# -----------------------
def seed_mongo():
    try:
        print(f"Connecting to MongoDB at {settings.mongo_uri}...")
        client = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=5000)
        db = client[settings.mongo_db_name]

        # --- user_events ---
        db.user_events.drop()
        user_events = [
            {
                "user_id": 1,
                "event_type": "login",
                "event_timestamp": "2024-01-01T10:00:00",
                "session_id": "s1",
                "device": "mobile",
                "location": "US",
            },
            {
                "user_id": 1,
                "event_type": "view_product",
                "event_timestamp": "2024-01-01T10:05:00",
                "session_id": "s1",
                "device": "mobile",
                "location": "US",
            },
            {
                "user_id": 1,
                "event_type": "add_to_cart",
                "event_timestamp": "2024-01-01T10:10:00",
                "session_id": "s1",
                "device": "mobile",
                "location": "US",
            },
            {
                "user_id": 2,
                "event_type": "login",
                "event_timestamp": "2024-01-02T11:00:00",
                "session_id": "s2",
                "device": "desktop",
                "location": "UK",
            },
            {
                "user_id": 2,
                "event_type": "view_product",
                "event_timestamp": "2024-01-02T11:15:00",
                "session_id": "s2",
                "device": "desktop",
                "location": "UK",
            },
            {
                "user_id": 3,
                "event_type": "login",
                "event_timestamp": "2024-01-03T12:00:00",
                "session_id": "s3",
                "device": "tablet",
                "location": "CA",
            },
            {
                "user_id": 3,
                "event_type": "purchase",
                "event_timestamp": "2024-01-03T12:30:00",
                "session_id": "s3",
                "device": "tablet",
                "location": "CA",
            },
            {
                "user_id": 4,
                "event_type": "login",
                "event_timestamp": "2024-01-04T13:00:00",
                "session_id": "s4",
                "device": "mobile",
                "location": "US",
            },
            {
                "user_id": 5,
                "event_type": "view_product",
                "event_timestamp": "2024-01-05T14:00:00",
                "session_id": "s5",
                "device": "desktop",
                "location": "AU",
            },
            {
                "user_id": 1,
                "event_type": "logout",
                "event_timestamp": "2024-01-01T10:15:00",
                "session_id": "s1",
                "device": "mobile",
                "location": "US",
            },
            {
                "user_id": 6,
                "event_type": "login",
                "event_timestamp": "2024-01-06T09:00:00",
                "session_id": "s6",
                "device": "mobile",
                "location": "US",
            },
            {
                "user_id": 7,
                "event_type": "login",
                "event_timestamp": "2024-01-07T10:00:00",
                "session_id": "s7",
                "device": "desktop",
                "location": "DE",
            },
            {
                "user_id": 8,
                "event_type": "login",
                "event_timestamp": "2024-01-08T11:00:00",
                "session_id": "s8",
                "device": "mobile",
                "location": "FR",
            },
            {
                "user_id": 9,
                "event_type": "login",
                "event_timestamp": "2024-01-09T12:00:00",
                "session_id": "s9",
                "device": "desktop",
                "location": "JP",
            },
            {
                "user_id": 10,
                "event_type": "login",
                "event_timestamp": "2024-01-10T13:00:00",
                "session_id": "s10",
                "device": "mobile",
                "location": "IN",
            },
        ]
        db.user_events.insert_many(user_events)

        # --- product_reviews ---
        db.product_reviews.drop()
        product_reviews = [
            {
                "product_id": 1,
                "user_id": 1,
                "rating": 5,
                "review_text": "Amazing performance!",
                "created_at": "2024-01-02",
            },
            {
                "product_id": 2,
                "user_id": 2,
                "rating": 4,
                "review_text": "Great phone, a bit expensive",
                "created_at": "2024-01-03",
            },
            {
                "product_id": 1,
                "user_id": 3,
                "rating": 5,
                "review_text": "Best laptop I have ever owned",
                "created_at": "2024-01-04",
            },
            {
                "product_id": 3,
                "user_id": 1,
                "rating": 3,
                "review_text": "Average tablet",
                "created_at": "2024-01-05",
            },
            {
                "product_id": 2,
                "user_id": 4,
                "rating": 4,
                "review_text": "Very fast",
                "created_at": "2024-01-06",
            },
            {
                "product_id": 1,
                "user_id": 5,
                "rating": 5,
                "review_text": "Perfect for work",
                "created_at": "2024-01-07",
            },
            {
                "product_id": 4,
                "user_id": 6,
                "rating": 5,
                "review_text": "Best audio quality",
                "created_at": "2024-01-08",
            },
            {
                "product_id": 5,
                "user_id": 7,
                "rating": 4,
                "review_text": "Nice watch",
                "created_at": "2024-01-09",
            },
            {
                "product_id": 6,
                "user_id": 8,
                "rating": 5,
                "review_text": "Stunning screen",
                "created_at": "2024-01-10",
            },
            {
                "product_id": 7,
                "user_id": 9,
                "rating": 3,
                "review_text": "Too big",
                "created_at": "2024-01-11",
            },
        ]
        db.product_reviews.insert_many(product_reviews)

        print(
            f"Mongo seeded — user_events: {db.user_events.count_documents({})} docs, "
            f"product_reviews: {db.product_reviews.count_documents({})} docs"
        )
        client.close()
    except Exception as e:
        print(f"Mongo seeding failed: {e}")


# -----------------------
# MAIN
# -----------------------
if __name__ == "__main__":
    # Seed both databases
    seed_sql()
    seed_mongo()
