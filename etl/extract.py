import pandas as pd
from .utils import get_engine, log

def extract_raw_to_table():
    """Extrae datos base desde la tabla original y los guarda como 'clean'."""
    engine = get_engine()
    log("Extrayendo datos base de Railway: spotify_most_streamed_2024")
    df = pd.read_sql('SELECT * FROM spotify_most_streamed_2024', engine)
    df.to_sql('spotify_most_streamed_clean', engine, if_exists='replace', index=False)
    log("Tabla spotify_most_streamed_clean creada correctamente.")
