FROM apache/airflow:slim-2.10.3-python3.9

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

COPY ./entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER airflow

# Set Env variables from docker compose args for creating airflow admin.
ARG AIRFLOW_ADMIN_USER
ARG AIRFLOW_ADMIN_FIRST_NAME
ARG AIRFLOW_ADMIN_LAST_NAME
ARG AIRFLOW_ADMIN_EMAIL
ARG AIRFLOW_ADMIN_PASS

ENV AIRFLOW_ADMIN_USER ${AIRFLOW_ADMIN_USER}
ENV AIRFLOW_ADMIN_FIRST_NAME ${AIRFLOW_ADMIN_FIRST_NAME}
ENV AIRFLOW_ADMIN_LAST_NAME ${AIRFLOW_ADMIN_LAST_NAME}
ENV AIRFLOW_ADMIN_EMAIL ${AIRFLOW_ADMIN_EMAIL}
ENV AIRFLOW_ADMIN_PASS ${AIRFLOW_ADMIN_PASS}


COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["./entrypoint.sh"]