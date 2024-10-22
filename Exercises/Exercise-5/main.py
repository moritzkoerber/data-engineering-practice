import os
from collections import namedtuple

import psycopg2

Table = namedtuple("Table", ["create_query", "index_column"])

accounts = Table(
    """
CREATE TABLE IF NOT EXISTS accounts (
    customer_id integer PRIMARY KEY,
    first_name text,
    last_name text,
    address_1 text,
    address_2 text,
    city text,
    state text,
    zip_code integer,
    join_date date
)
""",
    "customer_id",
)

products = Table(
    """
CREATE TABLE IF NOT EXISTS products (
    product_id integer PRIMARY KEY,
    product_code text,
    product_description text
)
""",
    "product_id",
)

transactions = Table(
    """
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id text PRIMARY KEY,
    transaction_date date,
    product_id integer,
    product_code text,
    product_description text,
    quantity integer,
    account_id integer,
FOREIGN KEY(product_id)
    REFERENCES products(product_id),
FOREIGN KEY(account_id)
    REFERENCES accounts(customer_id)
)
""",
    "transaction_id",
)

table_dict = {"accounts": accounts, "products": products, "transactions": transactions}


def drop_table(table_name: str) -> str:
    return f"""
DROP TABLE IF EXISTS {table_name} CASCADE
"""


def create_index(
    index_name: str, table_name: str, column_names: int | list[str]
) -> str:
    return f"""
CREATE INDEX {index_name} ON {table_name} ({column_names})
"""


def copy_from_csv(file_name: str, table_name: str, cursor: psycopg2.extensions.cursor):
    with open(f"data/{file_name}", "r") as f:
        next(f)
        cursor.copy_from(f, table_name, sep=",")


def main():
    with psycopg2.connect(
        host="postgresdb",
        database="exercise_database",
        user="exercise_user",
    ) as conn, conn.cursor() as cur:
        for csv_file in os.listdir("data"):
            table_name = csv_file.removesuffix(".csv")
            cur.execute(drop_table(table_name))
            table = table_dict[table_name]
            cur.execute(table.create_query)
            copy_from_csv(csv_file, table_name, cur)
            cur.execute(create_index(table_name, table_name, table.index_column))
    conn.close()


if __name__ == "__main__":
    main()
