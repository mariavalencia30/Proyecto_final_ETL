import os, pandas as pd, seaborn as sns, matplotlib.pyplot as plt
from sqlalchemy import text
from .utils import get_engine, log

def generate_reports():
    """Genera reportes CSV y figuras de análisis."""
    engine = get_engine()
    current_dir = os.getcwd()
    base_dir = os.path.abspath(os.path.join(current_dir, "..")) if "notebooks" in current_dir else current_dir

    reports_dir = os.path.join(base_dir, "reports/sql_results")
    figs_dir = os.path.join(base_dir, "reports/figures")
    os.makedirs(reports_dir, exist_ok=True)
    os.makedirs(figs_dir, exist_ok=True)

    queries = {
        "01_top_tracks.csv": """SELECT "Track","Artist","Spotify Streams" FROM spotify_most_streamed_enriched_cleaned ORDER BY "Spotify Streams" DESC LIMIT 10;""",
        "02_streams_evolution.csv": """SELECT DATE_TRUNC('month',"Release Date") AS month, SUM("Spotify Streams") AS total_streams FROM spotify_most_streamed_enriched_cleaned GROUP BY month ORDER BY month;""",
        "03_playlist_distribution.csv": """SELECT "Spotify Playlist Count", COUNT(*) AS track_count FROM spotify_most_streamed_enriched_cleaned GROUP BY "Spotify Playlist Count" ORDER BY track_count DESC;""",
        "04_duration_popularity.csv": """SELECT "duration_ms","Spotify Popularity" FROM spotify_most_streamed_enriched_cleaned WHERE "duration_ms" IS NOT NULL;""",
        "05_top_artists.csv": """SELECT "Artist", SUM("Spotify Streams") AS total_streams FROM spotify_most_streamed_enriched_cleaned GROUP BY "Artist" ORDER BY total_streams DESC LIMIT 10;"""
    }

    for name, q in queries.items():
        df = pd.read_sql(q, engine)
        df.to_csv(os.path.join(reports_dir, name), index=False)

    df_all = pd.read_sql('SELECT * FROM spotify_most_streamed_enriched_cleaned', engine)
    df_all['release_date'] = pd.to_datetime(df_all['release_date'], errors='coerce')
    sns.set(style="whitegrid")

    # Figuras
    top_artists = df_all.groupby('Artist')['Spotify Streams'].sum().sort_values(ascending=False).head(10)
    plt.figure(figsize=(12,6))
    sns.barplot(x=top_artists.values, y=top_artists.index, palette="viridis")
    plt.title("Top 10 Artistas por Spotify Streams")
    plt.tight_layout()
    plt.savefig(os.path.join(figs_dir, "01_top_artists.png"))
    plt.close()

    monthly = df_all.groupby(df_all['release_date'].dt.to_period('M'))['Spotify Streams'].sum()
    plt.figure(figsize=(12,6))
    monthly.plot(marker='o')
    plt.title("Evolución de Streams por Mes")
    plt.tight_layout()
    plt.savefig(os.path.join(figs_dir, "02_streams_evolution.png"))
    plt.close()

    plt.figure(figsize=(8,6))
    sns.scatterplot(data=df_all, x="duration_ms", y="Spotify Popularity", alpha=0.6)
    plt.title("Duración vs Popularidad Spotify")
    plt.tight_layout()
    plt.savefig(os.path.join(figs_dir, "03_duration_popularity.png"))
    plt.close()

    corr = df_all[["Spotify Streams","YouTube Views","TikTok Views","Apple Music Playlist Count"]].corr()
    plt.figure(figsize=(8,6))
    sns.heatmap(corr, annot=True, fmt=".2f", cmap="coolwarm")
    plt.title("Correlación Multicanal")
    plt.tight_layout()
    plt.savefig(os.path.join(figs_dir, "04_multichannel_correlation.png"))
    plt.close()

    log("Reportes y figuras generados correctamente.")
