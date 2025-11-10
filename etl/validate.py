import os
import pandas as pd
import numpy as np
from datetime import datetime
from .utils import get_engine, log

def validate_enriched_data():
    """Valida datos enriquecidos y genera reporte de control en la carpeta raíz /reports."""
    engine = get_engine()
    df = pd.read_sql('SELECT * FROM spotify_most_streamed_enriched', engine)
    log(f"Validando {len(df)} registros...")

    df["release_date"] = df["release_date"].fillna("Unknown")
    df = df[df["duration_ms"].isna() | (df["duration_ms"] >= 30000)]
    df = df.drop_duplicates(subset=["Artist", "Track"], keep="first")

    current_dir = os.getcwd()
    base_dir = os.path.abspath(os.path.join(current_dir, "..")) if "notebooks" in current_dir else current_dir
    reports_dir = os.path.join(base_dir, "reports/great_expectations")
    os.makedirs(reports_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_dir = os.path.join(os.path.expanduser("~"), "ETL_reports", "great_expectations")
    os.makedirs(safe_dir, exist_ok=True)
    summary_path = os.path.join(safe_dir, f"summary_{timestamp}.txt")

    print(f"Guardando resumen en: {summary_path}")
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(f"Filas después de validación: {len(df)}\n")
        f.write(f"Columnas: {len(df.columns)}\n")
        f.write(f"Fecha de validación: {timestamp}\n")

    df.to_sql("spotify_most_streamed_enriched_cleaned", engine, if_exists="replace", index=False)
    log(f"Validación completada. Guardado 'spotify_most_streamed_enriched_cleaned' y resumen en {summary_path}")
