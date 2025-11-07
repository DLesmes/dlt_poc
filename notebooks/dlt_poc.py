# %% [markdown]
# # dtl code notebook
# dtl
import os
from dotenv import load_dotenv
load_dotenv()
# origin database
import dlt
import sqlalchemy as sa
from dlt.sources.sql_database import sql_table
from itertools import islice
SOURCE_DB = os.getenv("DATABASE_URL")  # e.g. postgresql+psycopg://user:pass@host:5432/dbname
# model
from typing import Any, Optional
from dataclasses import dataclass, asdict
import uuid
from datetime import datetime
import json
import psycopg

# %% [markdown]
# ## query finction fro JSOB filtering
def query_adapter_callback(query: sa.Select, table: sa.Table, incremental=None, engine=None) -> sa.Select:
    # Only apply to table "states" in schema "poc"
    if table.name == "states" and table.schema == "poc":
        # Build JSONB key→value filters
        cond = sa.and_(
            table.c.extracted_data["trace_id"].astext == "fabpqz0l-7g2h-11ee-be56-0242ac120002",
            table.c.extracted_data["doc_id"].astext == "afsds-dsafs-fsdf-fs",
            table.c.extracted_data["workflow_id"].astext == "afsds-dsafs-fsdf-fs_wf_sdsf",
            table.c.extracted_data["tenant_id"].astext == "tt",
            table.c.extracted_data["step_id"].astext == "S01"
        )
        return query.where(cond)
    # For other tables, don’t modify query
    return query

# %% [markdown]
# ## Define the resource
states_resource = sql_table(
    credentials=SOURCE_DB,
    schema="poc",
    table="states",
    query_adapter_callback=query_adapter_callback,
    chunk_size=1000
)
# %% [markdown]
# listing not ull results
a = [row for row in islice(states_resource, 2) if row is not None]
a
# %% [markdown]
# ## creating a class to hold the data
# ## using dataclass
@dataclass
class  ExtractedData:
    trace_id: str
    doc_id: str
    workflow_id: str
    tenant_id: str
    step_id: str
    canonical_schema: dict[str, Any]

@dataclass
class StateModel:
    state_id: uuid.UUID
    extracted_data: ExtractedData
    created_at: datetime | None
    updated_at: datetime | None

def _to_dict_or_empty(value):
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return {}
    return {}

# %% [markdown]
# ### function to convert row to StateModel
def to_state_model(row: dict) -> StateModel:
    if not row:
        raise ValueError("Empty row")
    ed = _to_dict_or_empty(row.get("extracted_data"))
    extracted = ExtractedData(
        trace_id=ed.get("trace_id"),
        doc_id=ed.get("doc_id"),
        workflow_id=ed.get("workflow_id"),
        tenant_id=ed.get("tenant_id"),
        step_id=ed.get("step_id"),
        canonical_schema=ed.get("canonical_schema", ed) # fallback to whole JSON if nested key missing
    )
    return StateModel(
        state_id=row["state_id"],
        extracted_data=extracted,
        created_at=row.get("created_at"),
        updated_at=row.get("updated_at"),
    )

# %% [markdown]
# ## listing state objects
states = [to_state_model(row) for row in islice(states_resource, 5) if row is not None]
print(states)
# %% [markdown]
# ## data extracted from the first state object
states[0]
# %% [markdown]
# ## updating the extracted data in the first state object
states[0].extracted_data.canonical_schema['parties']['shipper']['name'] = {
    "value": "ABC Logistics",
    "confidence": 0.98
}
states[0]
# %% [markdown]
# ## Load updated state object back to the database using raw SQL
# This approach avoids dlt adding internal columns to the table

# %% [markdown]
# ### Prepare the updated data
updated_state = states[0]

# Convert the extracted_data back to a JSONB-compatible dict
updated_extracted_data = asdict(updated_state.extracted_data)

# %% [markdown]
# ### Update the record directly with SQL
# Convert SQLAlchemy-style URL to plain PostgreSQL DSN
dest_db = SOURCE_DB.replace("postgresql+psycopg://", "postgresql://").split("?")[0]

with psycopg.connect(dest_db) as conn:
    with conn.cursor() as cur:
        # Update the record using the state_id as the key
        cur.execute("""
            UPDATE poc.states
            SET extracted_data = %s,
                updated_at = NOW()
            WHERE state_id = %s
        """, (
            json.dumps(updated_extracted_data),  # Convert dict to JSON string
            str(updated_state.state_id)
        ))

        rows_affected = cur.rowcount
        conn.commit()

        print(f"✓ Successfully updated {rows_affected} row(s)")
        print(f"  State ID: {updated_state.state_id}")
        print(f"  Updated extracted_data with new canonical_schema")

