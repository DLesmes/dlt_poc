import os
from dotenv import load_dotenv
import psycopg

load_dotenv()
db_url = os.getenv('DATABASE_URL')

# Convert SQLAlchemy URL to plain PostgreSQL DSN
db_url = db_url.replace('postgresql+psycopg://', 'postgresql://').split('?')[0]

with psycopg.connect(db_url) as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name, is_nullable, data_type
            FROM information_schema.columns
            WHERE table_schema = 'poc'
              AND table_name = 'states'
              AND column_name LIKE '_dlt%'
            ORDER BY column_name;
        """)

        print('Current dlt columns in poc.states:')
        rows = cur.fetchall()
        if rows:
            for row in rows:
                print(f'  {row[0]}: nullable={row[1]}, type={row[2]}')
        else:
            print('  No _dlt columns found')
