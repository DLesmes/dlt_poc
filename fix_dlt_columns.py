"""
This script drops dlt internal columns from the states table
so they can be recreated with nullable=True configuration.
"""
import os
from dotenv import load_dotenv
import psycopg

load_dotenv()
db_url = os.getenv('DATABASE_URL')

# Convert SQLAlchemy URL to plain PostgreSQL DSN
db_url = db_url.replace('postgresql+psycopg://', 'postgresql://').split('?')[0]

print("Fixing dlt columns in poc.states table...")

with psycopg.connect(db_url) as conn:
    with conn.cursor() as cur:
        # Drop the problematic columns if they exist
        print("Dropping _dlt_load_id column...")
        cur.execute("""
            ALTER TABLE poc.states
            DROP COLUMN IF EXISTS _dlt_load_id;
        """)

        print("Dropping _dlt_id column...")
        cur.execute("""
            ALTER TABLE poc.states
            DROP COLUMN IF EXISTS _dlt_id;
        """)

        conn.commit()
        print("✓ Columns dropped successfully!")
        print("\nNow you can run your dlt pipeline again.")
