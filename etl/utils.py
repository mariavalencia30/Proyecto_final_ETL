import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime

# ======================================================
# CONFIGURACIÓN BASE
# ======================================================
load_dotenv()

DB_URI = os.getenv("DB_URI")
if not DB_URI:
    raise ValueError("No se encontró la variable DB_URI en el archivo .env")

# ======================================================
# CONEXIÓN A BASE DE DATOS
# ======================================================
def get_engine():
    """Retorna el motor de conexión SQLAlchemy reutilizable."""
    return create_engine(DB_URI, pool_pre_ping=True)

# ======================================================
# LOGGING SIMPLE
# ======================================================
def log(message: str):
    """Imprime mensajes con marca de tiempo uniforme."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {message}")

# ======================================================
# FUNCIONES AUXILIARES DE BASE DE DATOS
# ======================================================
def test_connection():
    """Verifica si la conexión a la base de datos es válida."""
    engine = get_engine()
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        log("Conexión a la base de datos verificada correctamente.")
        return True
    except Exception as e:
        log(f"Error al conectar a la base de datos: {e}")
        return False


def table_exists(table_name: str) -> bool:
    """Verifica si existe una tabla específica en la base de datos."""
    engine = get_engine()
    with engine.connect() as conn:
        query = text(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = :table
            );
            """
        )
        result = conn.execute(query, {"table": table_name}).scalar()
        return bool(result)


__all__ = ["get_engine", "log", "test_connection", "table_exists"]
