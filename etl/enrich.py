import os, time, json, requests, shutil
import pandas as pd
from dotenv import load_dotenv
from .utils import get_engine, log

# ======================================================
# CONFIGURACIÓN GLOBAL
# ======================================================
load_dotenv()

API_KEY = os.getenv("LASTFM_API_KEY")
API_URL = "http://ws.audioscrobbler.com/2.0/"
CACHE_DIR = os.path.join(os.path.expanduser("~"), "ETL_cache", "lastfm")
engine = get_engine()

# ======================================================
# FUNCIONES AUXILIARES
# ======================================================
def safe_int(value):
    """Convierte un valor a int, reemplazando texto inválido por 0."""
    try:
        if value in (None, "", "Unknown", "N/A"):
            return 0
        return int(float(value))
    except Exception:
        return 0

def fetch_track_info(artist, track):
    """Consulta la API de LastFM con caché local."""
    safe_artist = artist.replace("/", "_").replace(" ", "_")
    safe_track = track.replace("/", "_").replace(" ", "_")
    cache_file = os.path.join(CACHE_DIR, f"{safe_artist}__{safe_track}.json")

    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    params = {
        "method": "track.getInfo",
        "api_key": API_KEY,
        "artist": artist,
        "track": track,
        "format": "json",
    }

    try:
        resp = requests.get(API_URL, params=params, timeout=10)
        data = resp.json()
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return data
    except Exception as e:
        log(f"Error consultando API LastFM ({artist} - {track}): {e}")
        return {}

def parse_track_info(data):
    """Extrae los campos relevantes del JSON de LastFM."""
    if not data or "track" not in data:
        return {
            "duration_ms": 0,
            "album": "Unknown",
            "release_date": "Unknown",
            "listeners": 0,
            "playcount": 0,
            "tags": "Unknown",
        }

    t = data["track"]
    album = t.get("album", {}).get("title", "Unknown")
    duration = safe_int(t.get("duration"))
    release = t.get("wiki", {}).get("published", "Unknown")
    listeners = safe_int(t.get("listeners"))
    playcount = safe_int(t.get("playcount"))

    tags_list = t.get("toptags", {}).get("tag", [])
    tags = (
        ", ".join([tg.get("name", "") for tg in tags_list[:3]])
        if isinstance(tags_list, list)
        else "Unknown"
    )

    return {
        "duration_ms": duration,
        "album": album,
        "release_date": release,
        "listeners": listeners,
        "playcount": playcount,
        "tags": tags,
    }

# ======================================================
# ETL PRINCIPAL
# ======================================================
from concurrent.futures import ThreadPoolExecutor, as_completed

def enrich_with_lastfm():
    """Enriquece la tabla spotify_most_streamed_clean con datos de LastFM usando paralelismo y guardado por lotes."""
    print(f"Usando carpeta de caché: {CACHE_DIR}")
    os.makedirs(CACHE_DIR, exist_ok=True)

    df = pd.read_sql("SELECT * FROM spotify_most_streamed_clean", engine)

    #  Filtrar duplicados y registros vacíos
    df = df.drop_duplicates(subset=["Artist", "Track"])
    df = df[(df["Artist"].notnull()) & (df["Track"].notnull())]

    #  Preparar lotes
    batch_size = 500
    total = len(df)
    log(f"Procesando {total} canciones en bloques de {batch_size}...")

    for start in range(0, total, batch_size):
        end = min(start + batch_size, total)
        batch = df.iloc[start:end]
        enriched_batch = []

        # ⚡ Paralelismo por bloque
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(fetch_and_parse, row): idx
                for idx, row in batch.iterrows()
            }

            for future in as_completed(futures):
                result = future.result()
                if result:
                    enriched_batch.append(result)

        save_batch_safely(enriched_batch, end, total)

    shutil.rmtree(CACHE_DIR, ignore_errors=True)
    log(" Enriquecimiento completado correctamente.")

def fetch_and_parse(row):
    """Consulta y parsea una fila individual."""
    artist, track = row["Artist"], row["Track"]
    log(f"Procesando: {artist} - {track}")
    try:
        data = fetch_track_info(artist, track)
        parsed = parse_track_info(data)
        return {**row.to_dict(), **parsed}
    except Exception as e:
        log(f" Error en {artist} - {track}: {e}")
        return None

# ======================================================
# GUARDADO SEGURO
# ======================================================
def save_batch_safely(records, current, total):
    """Guarda un lote de registros garantizando la limpieza de tipos."""
    df_temp = pd.DataFrame(records)
    df_temp = df_temp.replace(["Unknown", "N/A", "", None], pd.NA)

    possible_numeric = [
        c for c in df_temp.columns
        if any(k in c.lower() for k in [
            "stream", "count", "view", "rank", "score", "popularity",
            "spin", "post", "listener", "duration", "playcount"
        ])
    ]

    for col in possible_numeric:
        df_temp[col] = pd.to_numeric(df_temp[col], errors="coerce")

    bad_cells = df_temp[possible_numeric].applymap(lambda x: isinstance(x, str))
    if bad_cells.any().any():
        log("Filas con texto en columnas numéricas detectadas. Serán omitidas.")
        df_temp = df_temp.loc[~bad_cells.any(axis=1)]

    df_temp[possible_numeric] = df_temp[possible_numeric].fillna(0)

    if "release_date" in df_temp.columns:
        df_temp["release_date"] = df_temp["release_date"].astype(str).replace("Unknown", pd.NA)
        df_temp["release_date"] = pd.to_datetime(df_temp["release_date"], errors="coerce")

    try:
        df_temp.to_sql("spotify_most_streamed_enriched", engine, if_exists="append", index=False)
        log(f"Guardado parcial {current}/{total} registros procesados correctamente.")
    except Exception as e:
        log(f"Error insertando lote {current}/{total}: {e}")

if __name__ == "__main__":
    enrich_with_lastfm()
