FROM quay.io/astronomer/astro-runtime:9.4.0
ENV AIRFLOW_CONN_CRATEDB_CONNECTION=postgresql://Crate:@34.86.222.51:4200/doc?sslmode=disable
