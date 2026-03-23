# scripts/seed_test_data.py
from sqlalchemy import create_engine, text
from pymongo import MongoClient
from config.settings import settings
def seed_sql():
    engine = create_engine(settings.sql_db_url)
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS cust (
                id    INTEGER PRIMARY KEY,
                name  TEXT NOT NULL,
                email TEXT NOT NULL
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS prod (
                id    INTEGER PRIMARY KEY,
                name  TEXT NOT NULL,
                price REAL NOT NULL
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ord (
                id          INTEGER PRIMARY KEY,
                customer_id INTEGER REFERENCES customers(id),
                product_id  INTEGER REFERENCES products(id),
                quantity    INTEGER,
                created_at  TEXT
            )
        """))
        for table in ["ord", "prod", "cust"]:
            conn.execute(text(f"DELETE FROM {table}"))       
        conn.execute(text("INSERT INTO cust VALUES (1,'Alice','alice@example.com'),(2,'Bob','bob@example.com'),(3,'Sara','sara@example.com')"))
        conn.execute(text("INSERT INTO prod VALUES (1,'Laptop',1200.00),(2,'Phone',800.00),(3,'Tablet',450.00)"))
        conn.execute(text("INSERT INTO ord VALUES (1,1,1,2,'2024-01-10'),(2,2,2,1,'2024-01-12'),(3,1,3,1,'2024-01-15'),(4,3,1,1,'2024-01-18')"))
        conn.commit()
        
    print("SQL seeded — 3 tables, sample rows inserted")


def print_table(table_name: str):
    engine = create_engine(settings.sql_db_url)
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT * FROM {table_name}"))
        rows = result.fetchall()  # fetch all rows

        print(f"\nTable: {table_name}")
        if not rows:
            print("Empty")
            return

        # Print column names
        print(" | ".join(result.keys()))
        # Print rows
        for row in rows:
            print(" | ".join(str(item) for item in row))




def seed_mongo():
    client = MongoClient(settings.mongo_uri)
    #print("MONGO URI:", settings.mongo_uri)
    db = client[settings.mongo_db_name]

    db.user_events.drop()
    db.user_events.insert_many([
        {"customer_id": 1, "event": "page_view",   "page": "/laptops",  "duration_sec": 45},
        {"customer_id": 1, "event": "add_to_cart", "product_id": 1,     "quantity": 2},
        {"customer_id": 2, "event": "page_view",   "page": "/phones",   "duration_sec": 30},
        {"customer_id": 2, "event": "checkout",    "total": 800.00,     "status": "completed"},
        {"customer_id": 3, "event": "page_view",   "page": "/tablets",  "duration_sec": 60},
        {"customer_id": 3, "event": "add_to_cart", "product_id": 3,     "quantity": 1},
    ])

    db.product_catalog.drop()
    db.product_catalog.insert_many([
        {"product_id": 1, "name": "Laptop", "tags": ["electronics","computing"], "specs": {"ram_gb": 16, "storage_gb": 512}},
        {"product_id": 2, "name": "Phone",  "tags": ["electronics","mobile"],   "specs": {"storage_gb": 256, "camera_mp": 48}},
        {"product_id": 3, "name": "Tablet", "tags": ["electronics","portable"], "specs": {"ram_gb": 8,  "storage_gb": 128}},
    ])

    print(f"Mongo seeded — user_events: {db.user_events.count_documents({})} docs, product_catalog: {db.product_catalog.count_documents({})} docs")

if __name__ == "__main__":
    seed_sql()
    # print_table("customers")
    # print_table("products")
    # print_table("orders")
    seed_mongo()