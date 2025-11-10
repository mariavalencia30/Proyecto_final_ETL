import pandas as pd
import numpy as np
from .utils import get_engine, log

def transform_clean_data():
    """Limpia, normaliza y transforma los datos crudos para análisis posterior."""
    engine = get_engine()
    df = pd.read_sql('SELECT * FROM spotify_most_streamed_clean', engine)
    log(f"Leyendo {len(df)} filas desde spotify_most_streamed_clean")

    # Conversión numérica general
    for col in df.columns:
        if any(k in col.lower() for k in ['stream','count','view','like','score','popularity','rank','spin','post']):
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce')

    # Fechas
    for col in [c for c in df.columns if 'date' in c.lower() or 'release' in c.lower()]:
        df[col] = pd.to_datetime(df[col], errors='coerce')

    # Rank en formato numérico
    if 'All Time Rank' in df.columns:
        df['All Time Rank'] = pd.to_numeric(df['All Time Rank'].astype(str).str.replace(',', ''), errors='coerce')

    # Valores nulos y duplicados
    df.fillna({'Explicit Track': 0}, inplace=True)
    df.fillna('Unknown', inplace=True)
    df.drop_duplicates(inplace=True)

    df.to_sql('spotify_most_streamed_clean', engine, if_exists='replace', index=False)
    log("Datos transformados y actualizados en spotify_most_streamed_clean.")
