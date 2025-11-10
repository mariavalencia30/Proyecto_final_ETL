
# Proyecto 3 — ETL Spotify con Airflow y Python

Este proyecto implementa un **pipeline ETL completo** sobre datos de Spotify, enriquecidos con información adicional de LastFM.

Puede ejecutarse de dos maneras:

1. **Manual** , utilizando un notebook que reproduce todo el flujo paso a paso.
2. **Automatizada** , mediante **Apache Airflow** desplegado con  **Docker Compose** .

---

## Estructura del proyecto

```
proyecto_3
│   .env
│   docker-compose.yml
│   entrypoint.sh
│   README.md
│   requirements.txt
│
├───dags
│   │   main_etl_spotify.py
│
├───data
│   ├───curated
│   │       spotify_most_streamed_enriched_cleaned.csv
│   │
│   └───raw
│           MostStreamedSpotifySongs2024.csv
│
├───etl
│   │   enrich.py
│   │   extract.py
│   │   load.py
│   │   report.py
│   │   transform.py
│   │   utils.py
│   │   validate.py
│   │   __init__.py
│
├───notebooks
│       01_ETL_Project_First_Delivery.ipynb
│       02_ETL_Project_Second_Delivery.ipynb
│       03_ETL_Project_Final_Delivery.ipynb
│
└───reports
    ├───figures
    │       01_top_artists.png
    │       02_streams_evolution.png
    │       03_duration_popularity.png
    │       04_multichannel_correlation.png
    │
    ├───great_expectations
    │       summary_20250927_151352.txt
    │       summary_20251109_084019.txt
    │       summary_20251109_153325.txt
    │       validation_report_20250927_151352.html
    │       validation_report_20250927_151352.json
    │
    └───sql_results
            01_top_tracks.csv
            02_streams_evolution.csv
            03_playlist_distribution.csv
            04_duration_popularity.csv
            05_top_artists.csv
            06_multichannel.csv
```

---

## Descripción general

El pipeline realiza un proceso **Extract → Transform → Enrich → Validate → Load → Report** sobre los datos de Spotify.

1. **extract.py:** carga y normaliza los datos crudos ubicados en `data/raw/`.
2. **transform.py:** limpia, estandariza y ajusta formatos.
3. **enrich.py:** agrega información proveniente de LastFM usando paralelismo, caché local y guardado por bloques para acelerar el proceso.
4. **validate.py:** valida la calidad de los datos con Great Expectations y genera reportes en una ruta segura del usuario.
5. **load.py:** guarda el resultado final en el área `data/curated/`.
6. **report.py:** genera visualizaciones y reportes en la carpeta `reports/`.

---

## Opción 1 — Ejecución manual (sin Airflow)

Para ejecutar el pipeline de forma manual, usa el notebook:

`notebooks/03_ETL_Project_Final_Delivery.ipynb`

Este archivo ejecuta todos los módulos de la carpeta `etl/` en orden, acompañados de explicaciones y celdas de resultados.

Es ideal para demostraciones rápidas o validaciones sin levantar contenedores.

El enriquecimiento con LastFM puede tardar varios minutos según la cantidad de canciones. El código ha sido optimizado con paralelismo y caché para acelerar esta etapa.

---

## Opción 2 — Ejecución con Airflow (vía Docker Compose)

Si se prefiere una ejecución completamente orquestada y reproducible, el entorno se puede levantar con  **Docker Compose** , el cual incluye:

* Apache Airflow 2.10.0 (Python 3.12)
* PostgreSQL 15 como base de datos de metadatos

### 1. Requisitos previos

* Python 3.12 (probado con éxito).

  Las versiones 3.14 y 3.10 presentaron incompatibilidades de paquetes.

  Otras versiones (3.11, 3.13) podrían funcionar, pero no fueron probadas.
* Docker y Docker Compose instalados.
* Archivo `.env` configurado con las variables necesarias (por ejemplo, la `FERNET_KEY` de Airflow).

# 1. Crear entorno virtual
python -m venv venv

# 2. Activarlo
source venv/bin/activate # En Windows/mac

# 3. Instalar cryptography dentro del entorno
pip install cryptography

Para generar una FERNET_KEY válida:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Agrega el valor resultante en tu archivo `.env` como:

```
AIRFLOW__CORE__FERNET_KEY=<tu_clave_generada>
```

---

### 2. Permisos del entrypoint

Antes de levantar el entorno, asegúrate de dar permisos de ejecución al archivo `entrypoint.sh`.

El archivo tiene formato **LF** (Linux), no **CRLF** (Windows), por lo que en Windows debe ajustarse o ejecutarse desde WSL:

```bash
chmod +x entrypoint.sh
```

---

### 3. Levantar el entorno con Docker Compose

Ejecuta el siguiente comando en la raíz del proyecto:

```bash
docker compose up --build
```

Esto realizará automáticamente los siguientes pasos:

* Instalar las dependencias del archivo `requirements.txt`.
* Inicializar la base de datos de Airflow.
* Crear un usuario administrador (`admin` / `admin`).
* Iniciar el servidor web y el scheduler.

Cuando todo esté listo, puedes acceder a la interfaz web de Airflow en:

```
http://localhost:8080
```

Credenciales por defecto:

```
Usuario: admin
Contraseña: admin
```

---

### 4. Ejecución del DAG

En la interfaz de Airflow aparecerá un único DAG:

`spotify_etl_full_pipeline`

Este DAG orquesta las siguientes tareas:

```
extract_raw_to_table
→ transform_clean_data
→ enrich_with_lastfm
→ validate_enriched_data
→ load_final_table
→ generate_reports
```

Cada tarea llama a las funciones correspondientes de la carpeta `etl/`.

Al finalizar, los resultados estarán disponibles en:

* `data/curated/spotify_most_streamed_enriched_cleaned.csv`
* `reports/` (gráficos, validaciones, salidas SQL y reportes)

Cada tarea ejecuta funciones independientes y reproducibles, optimizadas para ejecución en paralelo, validación robusta y generación de reportes automáticos.

---

 Decisiones de diseño destacadas
Paralelismo en enriquecimiento: se usa ThreadPoolExecutor para acelerar llamadas a la API de LastFM.
Caché persistente: evita llamadas repetidas y acelera ejecuciones futuras.
Validación robusta: se usa Great Expectations y rutas seguras para evitar errores de permisos.
Modularidad total: cada etapa del ETL es una función independiente, reutilizable en notebooks o Airflow.
Compatibilidad cross-platform: rutas, permisos y dependencias están diseñadas para funcionar en Linux, macOS y WSL.

---

## Archivos clave

| Archivo                                 | Descripción                                              |
| --------------------------------------- | --------------------------------------------------------- |
| `.env`                                | Variables de entorno del proyecto (incluye FERNET_KEY)    |
| `docker-compose.yml`                  | Define los servicios de Airflow y PostgreSQL              |
| `entrypoint.sh`                       | Script de inicialización automática del entorno Airflow |
| `requirements.txt`                    | Lista de dependencias de Python                           |
| `main_etl_spotify.py`                 | DAG principal de Airflow con el flujo completo            |
| `03_ETL_Project_Final_Delivery.ipynb` | Ejecución manual equivalente del ETL                     |

---

## Consejos prácticos

* Usa **Python 3.12** para evitar errores de compatibilidad con dependencias.
* Si trabajas en Windows, asegúrate de convertir los finales de línea de `entrypoint.sh` a  **LF** .
* Si necesitas limpiar el entorno por errores previos, ejecuta:

```bash
docker compose down -v
```

---

## Resultado esperado

Al completar la ejecución, se obtiene:

* Un entorno Airflow funcional y reproducible.
* Un pipeline ETL modular y orquestado.
* Datos transformados, enriquecidos y validados.
* Reportes automáticos y visualizaciones generadas.

---

**Tecnologías utilizadas:**

Python 3.12, Apache Airflow, Docker, PostgreSQL, Great Expectations

**Fecha:** Noviembre 2025
