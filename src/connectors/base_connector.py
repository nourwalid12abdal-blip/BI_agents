# src/connectors/base_connector.py
from abc import ABC, abstractmethod
from typing import Any

class BaseConnector(ABC):

    @abstractmethod
    def get_schema(self) -> dict:
        """Return a structured dict describing all tables/collections and their fields."""
        pass

    @abstractmethod
    def run_query(self, query: Any, **kwargs) -> list[dict]:
        """Execute a read-only query and return results as a list of dicts."""
        pass

    @abstractmethod
    def test_connection(self) -> bool:
        """Return True if the connection is alive."""
        pass

    @abstractmethod
    def close(self):
        """Close the connection cleanly."""
        pass