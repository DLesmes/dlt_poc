# %% [markdown]
# # dtl code notebook
import os
from dotenv import load_dotenv
load_dotenv()
import dlt
import sqlalchemy as sa
from dlt.sources.sql_database import sql_table
from itertools import islice
SOURCE_DB = os.getenv("DATABASE_URL")  # e.g. postgresql+psycopg://user:pass@host:5432/dbname
# %% [markdown]
# ## query finction fro JSOB filtering
def query_adapter_callback(query: sa.Select, table: sa.Table, incremental=None, engine=None) -> sa.Select:
    # Only apply to table "states" in schema "poc"
    if table.name == "states" and table.schema == "poc":
        # Build JSONB key→value filters
        cond = sa.and_(
            table.c.canonical_schema["trace_id"].astext == "fabpqz0l-7g2h-11ee-be56-0242ac120002",
            table.c.canonical_schema["doc_id"].astext == "afsds-dsafs-fsdf-fs",
            table.c.canonical_schema["workflow_id"].astext == "afsds-dsafs-fsdf-fs_wf_sdsf",
            table.c.canonical_schema["tenant_id"].astext == "tt",
            table.c.canonical_schema["step_id"].astext == "S01"
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
class State(dlt.Resource):
    state_id: str
    trace_id: str
    doc_id: str
    workflow_id: str
    tenant_id: str
    step_id: str
    canonical_schema: dict
    created_at: str | None = None
    updated_at: str | None = None
# %% [markdown]
# ## listing state objects
states = [State(**row) for row in islice(states_resource, 2) if row is not None]
states
# %% [markdown
