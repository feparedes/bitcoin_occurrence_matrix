FROM apache/airflow:2.7.0

RUN pip install dbt-postgres
RUN pip install scipy

USER root

RUN apt-get -y update
RUN apt-get -y install git

RUN mkdir -p /project
RUN mkdir -p /raw_data

COPY dbt/ /project/dbt/
COPY raw_data/ /raw_data/
