import time
import requests
from typing import Optional


class SigmaClient:
    """Client for the Sigma Computing REST API v2."""

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        base_url: str = "https://aws-api.sigmacomputing.com",
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.base_url = base_url.rstrip("/")
        self._access_token: Optional[str] = None
        self._token_expiry: float = 0

    def _get_access_token(self) -> str:
        if self._access_token and time.time() < self._token_expiry - 60:
            return self._access_token

        resp = requests.post(
            f"{self.base_url}/v2/auth/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        self._access_token = data["access_token"]
        self._token_expiry = time.time() + data.get("expires_in", 3600)
        return self._access_token

    def _headers(self) -> dict:
        return {"Authorization": f"Bearer {self._get_access_token()}"}

    # -------------------------------------------------------------------------
    # Datasets / Data Models
    # -------------------------------------------------------------------------

    def list_datasets(self, limit: int = 50, page_token: Optional[str] = None) -> dict:
        """List all datasets (data models) accessible to this client."""
        params: dict = {"limit": limit}
        if page_token:
            params["page"] = page_token
        resp = requests.get(
            f"{self.base_url}/v2/datasets",
            headers=self._headers(),
            params=params,
        )
        resp.raise_for_status()
        return resp.json()

    def get_dataset(self, dataset_id: str) -> dict:
        """Return metadata for a single dataset."""
        resp = requests.get(
            f"{self.base_url}/v2/datasets/{dataset_id}",
            headers=self._headers(),
        )
        resp.raise_for_status()
        return resp.json()

    def get_dataset_columns(self, dataset_id: str) -> list:
        """Return the column definitions for a dataset."""
        resp = requests.get(
            f"{self.base_url}/v2/datasets/{dataset_id}/columns",
            headers=self._headers(),
        )
        resp.raise_for_status()
        return resp.json().get("entries", [])

    # -------------------------------------------------------------------------
    # Querying
    # -------------------------------------------------------------------------

    def query_dataset(
        self,
        dataset_id: str,
        column_ids: Optional[list] = None,
        filters: Optional[list] = None,
        order_by: Optional[list] = None,
        limit: int = 500,
    ) -> dict:
        """
        Run a query against a dataset and return rows.

        Args:
            dataset_id:  The dataset (data model) ID.
            column_ids:  Column IDs to select; None returns all columns.
            filters:     List of filter dicts, e.g.
                           [{"columnId": "col_abc", "filterType": "eq", "values": ["foo"]}]
            order_by:    List of sort dicts, e.g.
                           [{"columnId": "col_abc", "direction": "asc"}]
            limit:       Maximum number of rows to return.

        Returns:
            Dict with keys "schema" (column metadata) and "rows" (list of value lists).
        """
        payload: dict = {"datasetId": dataset_id, "limit": limit}
        if column_ids:
            payload["columnIds"] = column_ids
        if filters:
            payload["filters"] = filters
        if order_by:
            payload["orderBy"] = order_by

        resp = requests.post(
            f"{self.base_url}/v2/queries",
            headers={**self._headers(), "Content-Type": "application/json"},
            json=payload,
        )
        resp.raise_for_status()
        return resp.json()

    def query_to_dataframe(self, dataset_id: str, **kwargs):
        """Convenience wrapper: returns query results as a pandas DataFrame."""
        try:
            import pandas as pd
        except ImportError as exc:
            raise ImportError("Install pandas to use query_to_dataframe()") from exc

        result = self.query_dataset(dataset_id, **kwargs)
        schema = result.get("schema", [])
        rows = result.get("rows", [])
        col_names = [col.get("name", col.get("columnId")) for col in schema]
        return pd.DataFrame(rows, columns=col_names)
