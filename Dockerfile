FROM apache/airflow:3.2.2

USER airflow

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.2.2/constraints-3.12.txt"
