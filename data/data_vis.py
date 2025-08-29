import psycopg2
import pandas as pd
import os
from sqlalchemy import create_engine, text
from datetime import datetime

def get_connection():
    return psycopg2.connect(
        host='localhost',
        database='supply_chain_optimizer',
        user=os.getenv('USER')
    )

def get_sqlalchemy_engine():
    user = os.getenv('USER')
    return create_engine(f'postgresql://{user}@localhost:5432/supply_chain_optimizer')

def print_table_sample(engine, table_name, limit=10, file=None):
    try:
        df = pd.read_sql(f'SELECT * FROM {table_name} LIMIT {limit};', engine)
        header = f"\nTABLE: {table_name.upper()} (showing up to {limit} rows)\n" + "-" * 60 + "\n"
        content = df.to_string(index=False) + "\n"
        if file:
            file.write(header)
            file.write(content)
        else:
            print(header)
            print(content)
    except Exception as e:
        error_msg = f"Could not read table {table_name}: {e}\n"
        if file:
            file.write(error_msg)
        else:
            print(error_msg)

def get_table_counts(engine, table_names):
    counts = {}
    with engine.connect() as conn:
        for table in table_names:
            try:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table};"))
                counts[table] = result.scalar()
            except Exception as e:
                counts[table] = f"Error: {e}"
    return counts

def explore_data():
    engine = get_sqlalchemy_engine()

    table_names = [
        "products",
        "suppliers",
        "inventory",
        "product_suppliers",
        "purchase_orders",
        "order_items",
        "demand_history",
        "supply_events"
    ]

    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    output_file = f"database_report_{timestamp}.txt"

    with open(output_file, 'w') as f:
        f.write("CURRENT STATE OF SUPPLY CHAIN DATABASE\n")
        f.write("=" * 60 + "\n")

        # Table counts summary
        f.write("\nTABLE ROW COUNTS\n")
        f.write("-" * 60 + "\n")
        counts = get_table_counts(engine, table_names)
        for table, count in counts.items():
            f.write(f"{table:20}: {count}\n")

        # Print table samples
        for table in table_names:
            print_table_sample(engine, table, limit=10, file=f)

    print(f"\nDatabase report saved to: {output_file}")

if __name__ == "__main__":
    explore_data()
