FROM python:3.13-slim
WORKDIR /usr/src/app
ENV PYTHONPATH=/usr/src/app

COPY ./data_pipelines ./data_pipelines
COPY ./requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

RUN useradd -M -s /bin/bash data_pipelines && chown -R data_pipelines:data_pipelines /usr/src/app
USER data_pipelines

ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__DAGS_FOLDER=/usr/src/app/data_pipelines
ENV AIRFLOW_HOME=/usr/src/app/airflow

CMD [ "airflow", "standalone" ]
