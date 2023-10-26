FROM quay.io/astronomer/astro-runtime:9.4.0
ENV AIRFLOW_CONN_CRATEDB_CONNECTION=postgresql://airflow:abc12345@34.86.222.51/doc?sslmode=disable
