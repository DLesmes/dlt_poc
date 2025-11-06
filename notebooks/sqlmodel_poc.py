# %% [markdown]
# ## SQLModel Notebook
import os
from dotenv import load_dotenv
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
import uuid
from uuid import uuid4
from datetime import datetime
from sqlmodel import SQLModel, Field, create_engine, Session, select, text, Column, DateTime
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.schema import CreateSchema
engine = create_engine(DATABASE_URL, echo=True)
# %% [markdown]
# ## getting all schemas
with Session(engine) as session:
    result = session.exec(text(
        "SELECT schema_name FROM information_schema.schemata;"
    ))
    for row in result:
        print(row)
# %% [markdown]
# ## getting all tables
with Session(engine) as session:
    for table in SQLModel.metadata.tables:
        print(table)
# %% [markdown]
# ## create schema
schema_name = "poc"
with engine.connect() as conn:
    conn.execute(CreateSchema(schema_name, if_not_exists=True))
    conn.commit()
SQLModel.metadata.create_all(engine)
# %% [markdown]
# ## getting all schemas
with Session(engine) as session:
    result = session.exec(text(
        "SELECT schema_name FROM information_schema.schemata;"
    ))
    for row in result:
        print(row)
# %%
# ##DDL - Data Definition Language
class State(SQLModel, table=True):
    __tablename__ = "states"
    __table_args__ = (
        {"schema": schema_name},
    )
    state_id: uuid.UUID = Field(
        default_factory=uuid4,
        sa_column=Column(
            "state_id", 
            UUID(as_uuid=True), 
            primary_key=True, 
            nullable=False,
            comment="Primary key for the states table"
        )
    )
    canonical_schema: dict = Field(
        sa_column=Column(
            "canonical_schema", 
            JSONB, 
            nullable=False,
            comment="A JSONB column to store the canonical schema"
        )
    )
    created_at: datetime | None = Field(
        default=None,
        sa_column=Column(
            "created_at",
            DateTime(timezone=True),
            nullable=False,
            server_default=text("now()"),
            comment="Insertion timestamp."
        )
    )
    updated_at: datetime | None = Field(
        default=None,
        sa_column=Column(
            "updated_at",
            DateTime(timezone=True),
            nullable=False,
            server_default=text("now()"),
            comment="Update timestamp."
        )
    )
# %% [markdown]
# ## create tables
SQLModel.metadata.create_all(engine)
# %% [markdown]
# ## getting all schemas
with Session(engine) as session:
    result = session.exec(text(
        "SELECT schema_name FROM information_schema.schemata;"
    ))
    for row in result:
        print(row)
# %% [markdown]
# ## getting all tables
with Session(engine) as session:
    for table in SQLModel.metadata.tables:
        print(table)
# %% [markdown]
# ## rollback - drop tables
SQLModel.metadata.drop_all(engine, tables=[State.__table__])
t = SQLModel.metadata.tables.get("poc.states")
if t is not None:
    SQLModel.metadata.remove(t) # affects only in-memory metadata
print(list(SQLModel.metadata.tables.keys()))
# %% [markdown]
# ## verify drop tables
with Session(engine) as session:
    try:
        result = session.exec(text(
            "SELECT * FROM poc.states;"
        ))
    except Exception as e:
        print(e)
# %% [markdown]
# ## getting all schemas
with Session(engine) as session:
    result = session.exec(text(
        "SELECT schema_name FROM information_schema.schemata;"
    ))
    for row in result:
        print(row)
# %% [markdown]
# ## getting all tables
with Session(engine) as session:
    for table in SQLModel.metadata.tables:
        print(table)
# %% [markdown]
# # addinng records
# ## create models
class State(SQLModel, table=True):
    __tablename__ = "states"
    __table_args__ = (
        {"schema": schema_name},
    )
    state_id: uuid.UUID = Field(
        default_factory=uuid4,
        sa_column=Column(
            "state_id", 
            UUID(as_uuid=True), 
            primary_key=True, 
            nullable=False,
            comment="Primary key for the states table"
        )
    )
    canonical_schema: dict = Field(
        sa_column=Column(
            "canonical_schema", 
            JSONB, 
            nullable=False,
            comment="A JSONB column to store the canonical schema"
        )
    )
    created_at: datetime | None = Field(
        default=None,
        sa_column=Column(
            "created_at",
            DateTime(timezone=True),
            nullable=False,
            server_default=text("now()"),
            comment="Insertion timestamp."
        )
    )
    updated_at: datetime | None = Field(
        default=None,
        sa_column=Column(
            "updated_at",
            DateTime(timezone=True),
            nullable=False,
            server_default=text("now()"),
            comment="Update timestamp."
        )
    )
# %% [markdown]
# ## recreate table
SQLModel.metadata.create_all(engine)
# %% [markdown]
# ## getting all tables
with Session(engine) as session:
    for table in SQLModel.metadata.tables:
        print(table)
# %% [markdown]
# ## inserting records
state_1 = State(
    canonical_schema={
        "trace_id": "fabpqz0l-7g2h-11ee-be56-0242ac120002",
        "doc_id": "afsds-dsafs-fsdf-fs",
        "workflow_id": "afsds-dsafs-fsdf-fs_wf_sdsf",
        "tenant_id": "tt",
        "step_id": "S01",
        "extracted_data": {
            "schema_name": "invoice_canonical_schema",
            "document_type": "invoice",
            "identifiers": {
                "bol_number": "",
                "pro_number": "",
                "scac": ""
            },
            "parties": {
                "shipper": {},
                "consignee": {},
                "bill_to": {}
            },
            "line_items": [
                {
                    "description": "",
                    "quantity": 0,
                    "weight": 0.0,
                    "class": "",
                    "nmfc": ""
                }
            ]
        }
    }
)

state_2 = State(
    canonical_schema={
        "trace_id": "fabpqz0l-7g2h-11ee-be56-0242ac120002",
        "doc_id": "afsds-dsafs-fsdf-fssdf",
        "workflow_id": "afsds-dsafs-fsdfdf-fs_wf_sdsf",
        "tenant_id": "tt",
        "step_id": "S03",
        "extracted_data": {
            "schema_name": "",
            "document_type": "bill_of_lading",
            "identifiers": {
                "bol_number": "",
                "pro_number": "",
                "scac": ""
            },
            "parties": {
                "shipper": {},
                "consignee": {},
                "bill_to": {}
            },
            "shipment": {
                "origin": {},
                "destination": {},
                "pickup_date": None,
                "delivery_date": None,
                "freight_terms": ""
            },
            "line_items": [
                {
                    "description": "",
                    "quantity": 0,
                    "weight": 0.0,
                    "class": "",
                    "nmfc": ""
                }
            ]
        }
    }
)
with Session(engine) as session:
    session.add(state_1)
    session.add(state_2)
    session.commit()
# %% [markdown]
# ## querying records
with Session(engine) as session:
    states = session.exec(select(State)).all()
    for state in states:
        print(state)
# %%
state.canonical_schema
# %%
