# %% [markdown]
# # SQLModel + dlt POC
# Combining dlt extraction with SQLModel ORM updates

# %%
# Imports
import os
from dotenv import load_dotenv
load_dotenv()

import dlt
import sqlalchemy as sa
from dlt.sources.sql_database import sql_table
from itertools import islice
import json
from typing import Any, Optional
from datetime import datetime
import uuid

# SQLModel imports
from sqlmodel import Field, Session, SQLModel, create_engine, Column, select
from sqlalchemy.dialects.postgresql import UUID, JSONB

SOURCE_DB = os.getenv("DATABASE_URL")

# %% [markdown]
# ## Define SQLModel classes for the database table

# %%
class State(SQLModel, table=True):
    """SQLModel representation of poc.states table"""
    __tablename__ = "states"
    __table_args__ = {"schema": "poc"}

    state_id: uuid.UUID = Field(
        sa_column=Column(UUID(as_uuid=True), primary_key=True)
    )
    extracted_data: dict[str, Any] = Field(
        sa_column=Column(JSONB, nullable=False)
    )
    created_at: Optional[datetime] = Field(default=None)
    updated_at: Optional[datetime] = Field(default=None)

# %% [markdown]
# ## dlt query adapter for JSONB filtering

# %%
def query_adapter_callback(query: sa.Select, table: sa.Table, incremental=None, engine=None) -> sa.Select:
    """Filter states table by JSONB fields"""
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
    return query

# %% [markdown]
# ## Extract data using dlt

# %%
# Create dlt resource
states_resource = sql_table(
    credentials=SOURCE_DB,
    schema="poc",
    table="states",
    query_adapter_callback=query_adapter_callback,
    chunk_size=1000
)

# Extract rows (raw dictionaries from dlt)
raw_rows = [row for row in islice(states_resource, 5) if row is not None]
print(f"Extracted {len(raw_rows)} rows from dlt")
raw_rows

# %% [markdown]
# ## Transform: Convert raw rows to SQLModel objects

# %%
# Helper function to ensure extracted_data is a dict
def ensure_dict(value):
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

# Convert raw dlt rows to SQLModel State objects
states = []
for raw_row in raw_rows:
    state = State(
        state_id=raw_row["state_id"],
        extracted_data=ensure_dict(raw_row.get("extracted_data")),
        created_at=raw_row.get("created_at"),
        updated_at=raw_row.get("updated_at")
    )
    states.append(state)

print(f"Converted {len(states)} rows to SQLModel objects")
states[0] if states else None

# %% [markdown]
# ## View extracted data from first state

# %%
if states:
    first_state = states[0]
    print(f"State ID: {first_state.state_id}")
    print(f"Extracted Data Keys: {list(first_state.extracted_data.keys())}")
    print(f"\nCanonical Schema:")
    print(json.dumps(first_state.extracted_data.get('canonical_schema', {}), indent=2))

# %% [markdown]
# ## Transform: Update the canonical_schema

# %%
if states:
    # Modify the extracted_data in-memory
    if 'canonical_schema' not in states[0].extracted_data:
        states[0].extracted_data['canonical_schema'] = {}

    if 'parties' not in states[0].extracted_data['canonical_schema']:
        states[0].extracted_data['canonical_schema']['parties'] = {}

    if 'shipper' not in states[0].extracted_data['canonical_schema']['parties']:
        states[0].extracted_data['canonical_schema']['parties']['shipper'] = {}

    # Update shipper name
    states[0].extracted_data['canonical_schema']['parties']['shipper']['name'] = {
        "value": "ABC Logistics",
        "confidence": 0.98
    }

    print("✓ Updated canonical_schema with new shipper name")
    print(f"\nNew shipper data:")
    print(json.dumps(
        states[0].extracted_data['canonical_schema']['parties']['shipper']['name'],
        indent=2
    ))

# %% [markdown]
# ## Load: Update the database using SQLModel ORM

# %%
if states:
    # Convert SQLAlchemy-style URL to plain PostgreSQL connection string
    db_url = SOURCE_DB.replace("postgresql+psycopg://", "postgresql+psycopg2://")

    # Create SQLModel engine
    engine = create_engine(db_url, echo=False)

    # Update using SQLModel session
    with Session(engine) as session:
        # Fetch the existing record from DB
        existing_state = session.get(State, states[0].state_id)

        if existing_state:
            # Update the fields
            existing_state.extracted_data = states[0].extracted_data
            existing_state.updated_at = datetime.now()

            # Commit the changes
            session.add(existing_state)
            session.commit()
            session.refresh(existing_state)

            print(f"✓ Successfully updated state in database")
            print(f"  State ID: {existing_state.state_id}")
            print(f"  Updated at: {existing_state.updated_at}")
            print(f"  Updated extracted_data with new canonical_schema")
        else:
            print(f"✗ State with ID {states[0].state_id} not found in database")

# %% [markdown]
# ## Verify the update by querying the database

# %%
if states:
    with Session(engine) as session:
        # Query the updated record
        statement = select(State).where(State.state_id == states[0].state_id)
        result = session.exec(statement).first()

        if result:
            print("✓ Verification successful - Record retrieved from database:")
            print(f"  State ID: {result.state_id}")
            print(f"  Updated at: {result.updated_at}")
            print(f"\n  Shipper name from DB:")
            shipper_name = result.extracted_data.get('canonical_schema', {}).get('parties', {}).get('shipper', {}).get('name')
            print(f"  {json.dumps(shipper_name, indent=2)}")
