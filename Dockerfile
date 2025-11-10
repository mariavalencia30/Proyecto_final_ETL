FROM apache/airflow:2.10.0-python3.12

USER root
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    libjpeg-dev \
    zlib1g-dev \
    libpng-dev \
    python3-dev \
    libffi-dev \
    libxml2-dev \
    libxslt1-dev

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
