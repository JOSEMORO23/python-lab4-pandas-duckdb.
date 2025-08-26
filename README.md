# python-lab4-pandas-duckdb.

# Lab 4 — Análisis Tabular con pandas y DuckDB

## Objetivos
- pandas: lectura CSV, limpieza, columnas derivadas, groupby y exporte.
- DuckDB: consultas SQL directas sobre CSV, agregaciones y exporte.

## Comparación
- **pandas**: ideal para exploración en memoria, `assign`, `groupby`, notebooks.
parte A — pandas
🔹 Script: src/pandas_flow.py

Flujo:

Lectura y exploración (pd.read_csv, df.info(), nulos).

Limpieza/derivadas:

date → datetime

category → .str.title()

customer → fillna("Unknown")

Derivada: rev_per_item = amount / quantity

Elimina nulos críticos (dropna).

Agrupación/filtros:

Filtra amount > 0

Columna derivada month

groupby(["month","category"]).agg(sum, mean, count)

- **DuckDB**: SQL embebido, lee CSV/Parquet directo, eficiente para agregaciones y joins, buena portabilidad.

Parte B — DuckDB (SQL embebido)
🔹 Script: src/duckdb_flow.py

Flujo:

Consulta directa al CSV con read_csv_auto: conteo de filas y de amount > 0.

Transformaciones en SQL:

CTEs: base → clean → pos → enriched.

Normalización (LOWER(category)), COALESCE en customer.

Derivada: rev_per_item = amount / quantity.

Filtro amount > 0.

Agrupación GROUP BY month, category.

Exportes:

data/summary_duckdb.csv

data/summary_duckdb.parquet

## Ejecutar
```bash
python src/pandas_flow.py
python src/duckdb_flow.py
