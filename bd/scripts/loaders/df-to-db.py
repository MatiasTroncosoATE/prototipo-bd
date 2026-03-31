import polars as pl
import psycopg2
from psycopg2.extras import execute_values
from typing import Any

# ---------------------------------------------------------------------------
# Polars dtype  →  PostgreSQL type
# ---------------------------------------------------------------------------
POLARS_TO_PG: dict[type, str] = {
    pl.Int8:    "SMALLINT",
    pl.Int16:   "SMALLINT",
    pl.Int32:   "INTEGER",
    pl.Int64:   "BIGINT",
    pl.UInt8:   "SMALLINT",
    pl.UInt16:  "INTEGER",
    pl.UInt32:  "BIGINT",
    pl.UInt64:  "NUMERIC(20,0)",
    pl.Float32: "REAL",
    pl.Float64: "DOUBLE PRECISION",
    pl.Boolean: "BOOLEAN",
    pl.Utf8:    "TEXT",
    pl.String:  "TEXT",
    pl.Date:    "DATE",
    pl.Datetime:"TIMESTAMP",
    pl.Duration:"INTERVAL",
    pl.Time:    "TIME",
    pl.Binary:  "BYTEA",
}
 
 
def polars_dtype_to_pg(dtype: pl.DataType) -> str:
    """Map a Polars DataType to its PostgreSQL equivalent."""
    base = type(dtype)
    if base in POLARS_TO_PG:
        return POLARS_TO_PG[base]
    # Fallback for nested / unknown types
    return "TEXT"


# ---------------------------------------------------------------------------
# Core function
# ---------------------------------------------------------------------------
def df_to_table(
    df: pl.DataFrame,
    table_name: str,
    conn_params: dict[str, Any],
) -> None:
    """
    Write a Polars DataFrame to a PostgreSQL table.
 
    Parameters
    ----------
    df          : Polars DataFrame to persist.
    table_name  : Target table name (without schema prefix).
    conn_params : psycopg2 connection keyword arguments, e.g.
                  {"host": "localhost", "dbname": "mydb",
                   "user": "postgres", "password": "secret"}.
    Raises
    ------
    ValueError  : if `if_exists` is not a recognised option.
    """

    if df.is_empty():
        print(f"[df_to_table] DataFrame is empty – nothing to write to '{table_name}'.")
        return
 
 
    # Build CREATE TABLE statement from the Polars schema
    col_defs = ", ".join(
        f'"{col}" {polars_dtype_to_pg(dtype)}'
        for col, dtype in df.schema.items()
    )
    create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({col_defs});"
 
    # Prepare rows as a list of tuples (Python-native values)
    columns = df.columns
    rows = [tuple(row) for row in df.iter_rows()]
 
    insert_sql = (
        f"INSERT INTO {qualified} "
        f"({', '.join(f'\"{ c}\"' for c in columns)}) "
        f"VALUES %s"
    )
    # Fix extra space in column quoting
    insert_sql = (
        f"INSERT INTO {qualified} "
        f"({', '.join(f'\"{c}\"' for c in columns)}) "
        f"VALUES %s"
    )
 
    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {qualified};")
                print(f"[df_to_table] Dropped existing table '{table_name}'.")
 
            cur.execute(create_sql)
 
            execute_values(cur, insert_sql, rows)
            print(
                f"[df_to_table] Inserted {len(rows)} row(s) into '{schema}.{table_name}'."
            )
 
        conn.commit()
