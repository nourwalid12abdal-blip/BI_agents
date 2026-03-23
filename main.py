from src.connectors.sql_connector import SQLConnector
from config.settings import settings

sql = SQLConnector(settings.sql_db_url)
print('SQL connected:', sql.test_connection())
print('SQL tables:', list(sql.get_schema().keys()))
print(sql._get_row_count("customers"))
