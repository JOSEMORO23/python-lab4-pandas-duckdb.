from __future__ import annotations
import duckdb

INPUT = "data/sales.csv"

def first_query() -> None:
    con = duckdb.connect()
    q = """
    SELECT
      COUNT(*) AS total_rows,
      COUNT(*) FILTER (WHERE CAST(amount AS DOUBLE) > 0) AS positive_rows
    FROM read_csv_auto(?)
    """
    print("\n=== B.1: Conteo directo CSV ===")
    print(con.execute(q, [INPUT]).df())
    con.close()

def full_sql_flow() -> None:
    con = duckdb.connect()

    # Usamos ? como placeholder y se lo pasamos en execute([...]).
    query = """
    WITH base AS (
      SELECT
        CAST(order_id AS INTEGER) AS order_id,
        CAST(date AS DATE) AS date,
        COALESCE(NULLIF(customer, ''), 'Unknown') AS customer,
        LOWER(category) AS category,
        CAST(amount AS DOUBLE) AS amount,
        CAST(quantity AS DOUBLE) AS quantity
      FROM read_csv_auto(?)
    ),
    clean AS (
      SELECT
        *,
        amount / NULLIF(quantity, 0) AS rev_per_item
      FROM base
      WHERE date IS NOT NULL AND amount IS NOT NULL AND quantity IS NOT NULL
    ),
    pos AS (
      SELECT * FROM clean WHERE amount > 0
    ),
    enriched AS (
      SELECT
        DATE_TRUNC('month', date) AS month,
        category,
        amount,
        quantity,
        order_id
      FROM pos
    )
    SELECT
      month,
      category,
      SUM(amount)        AS total_amount,
      AVG(quantity)      AS avg_quantity,
      COUNT(order_id)    AS orders
    FROM enriched
    GROUP BY 1, 2
    ORDER BY 1, 2
    """

    print("\n=== B.2: Resumen SQL ===")
    df_sql = con.execute(query, [INPUT]).df()
    print(df_sql)

    # Exportar (pasamos el mismo parámetro para el subquery)
    con.execute("COPY (" + query + ") TO 'data/summary_duckdb.csv' (HEADER, DELIMITER ',')", [INPUT])
    con.execute("COPY (" + query + ") TO 'data/summary_duckdb.parquet' (FORMAT 'parquet')", [INPUT])

    con.close()
    print("\nExportado: data/summary_duckdb.csv / data/summary_duckdb.parquet")

if __name__ == "__main__":
    first_query()
    full_sql_flow()
