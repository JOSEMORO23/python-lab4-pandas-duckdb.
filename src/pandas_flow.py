from __future__ import annotations
import pandas as pd

def run(input_path: str = "data/sales.csv") -> None:
    # A.1 Lectura y exploración
    df = pd.read_csv(input_path)
    print("\n=== A.1: Exploración ===")
    print("Shape:", df.shape)
    print("\nTipos:")
    print(df.dtypes)
    print("\nNulos:")
    print(df.isna().sum())
    print("\nHead:")
    print(df.head())

    # A.2 Derivadas y limpieza
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["category"] = df["category"].str.title()
    df["customer"] = df["customer"].fillna("Unknown")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["rev_per_item"] = df["amount"] / df["quantity"]
    df_clean = df.dropna(subset=["date", "amount", "quantity"])

    # A.3 Agrupaciones y filtros
    df_pos = df_clean.query("amount > 0")
    df_pos["month"] = df_pos["date"].dt.to_period("M").dt.to_timestamp()
    summary = (
        df_pos.groupby(["month", "category"], as_index=False)
        .agg(
            total_amount=("amount", "sum"),
            avg_quantity=("quantity", "mean"),
            orders=("order_id", "count"),
        )
        .sort_values(["month", "category"])
        .reset_index(drop=True)
    )
    print("\n=== A.3: Resumen ===")
    print(summary)

    # Exportes
    summary.to_csv("data/summary_pandas.csv", index=False)
    summary.to_parquet("data/summary_pandas.parquet", index=False)
    print("\nExportado: data/summary_pandas.csv / .parquet")

if __name__ == "__main__":
    run()
