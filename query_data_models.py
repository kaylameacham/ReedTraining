"""Example script: list Sigma data models and query one."""
import os
from dotenv import load_dotenv
from sigma_client import SigmaClient

load_dotenv()

client = SigmaClient(
    client_id=os.environ["SIGMA_CLIENT_ID"],
    client_secret=os.environ["SIGMA_CLIENT_SECRET"],
    base_url=os.environ.get("SIGMA_BASE_URL", "https://aws-api.sigmacomputing.com"),
)

# --- 1. List available data models -------------------------------------------
print("=== Available Data Models ===")
result = client.list_datasets()
datasets = result.get("entries", [])
for ds in datasets:
    print(f"  {ds.get('datasetId', ''):<40}  {ds.get('name', '(unnamed)')}")

if not datasets:
    print("  (no datasets found — check credentials and permissions)")
    raise SystemExit

# --- 2. Inspect columns of the first dataset ---------------------------------
first_id = datasets[0]["datasetId"]
print(f"\n=== Columns in '{datasets[0].get('name')}' ===")
columns = client.get_dataset_columns(first_id)
for col in columns:
    print(
        f"  {col.get('columnId', ''):<30}  "
        f"{col.get('name', ''):<30}  "
        f"{col.get('dataType', '')}"
    )

# --- 3. Query the dataset and load into a DataFrame --------------------------
print(f"\n=== Sample rows from '{datasets[0].get('name')}' ===")
df = client.query_to_dataframe(first_id, limit=10)
print(df.to_string(index=False))

# --- 4. Filtered query example -----------------------------------------------
# Uncomment and fill in a real columnId to try a filtered query:
#
# df_filtered = client.query_to_dataframe(
#     first_id,
#     column_ids=["col_id_1", "col_id_2"],
#     filters=[{"columnId": "col_id_1", "filterType": "eq", "values": ["some_value"]}],
#     order_by=[{"columnId": "col_id_1", "direction": "asc"}],
#     limit=100,
# )
# print(df_filtered)
