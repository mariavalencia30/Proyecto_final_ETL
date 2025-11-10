import pandas as pd
from .utils import get_engine, log

def finalize_curated_table():
    """Carga los datos validados a la tabla final del modelo."""
    engine = get_engine()
    source_table = "spotify_most_streamed_enriched_cleaned"
    final_table = "spotify_final_curated"

    log(f"Leyendo tabla validada: {source_table}")
    df = pd.read_sql(f'SELECT * FROM "{source_table}"', engine)

    if df.empty:
        raise ValueError("La tabla validada está vacía. No se puede cargar al destino final.")

    df.to_sql(final_table, engine, if_exists="replace", index=False)
    log(f"Tabla final '{final_table}' actualizada exitosamente ({len(df)} filas).")
