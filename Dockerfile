FROM python:3.11-slim

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev curl git \
    firefox-esr xvfb \
    libgtk-3-0 libdbus-glib-1-2 libxt6 libx11-xcb1 \
    fonts-liberation libasound2 wget procps \
    && rm -rf /var/lib/apt/lists/*

RUN ln -sf /usr/bin/firefox-esr /usr/bin/firefox

RUN wget -q "https://github.com/mozilla/geckodriver/releases/download/v0.35.0/geckodriver-v0.35.0-linux64.tar.gz" \
    -O /tmp/geckodriver.tar.gz && \
    tar -xzf /tmp/geckodriver.tar.gz -C /usr/local/bin/ && \
    chmod +x /usr/local/bin/geckodriver && \
    rm /tmp/geckodriver.tar.gz

RUN useradd -m -s /bin/bash airflow && \
    mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins && \
    chown -R airflow:airflow /opt/airflow

USER airflow
ENV AIRFLOW_HOME=/opt/airflow
ENV PATH="/home/airflow/.local/bin:${PATH}"

RUN pip install --no-cache-dir --user \
    "apache-airflow==2.10.5" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt"

RUN pip install --no-cache-dir --user \
    selenium==4.27.1 requests psycopg2-binary

COPY --chown=airflow:airflow dags/ /opt/airflow/dags/
COPY --chown=airflow:airflow start.sh /opt/airflow/start.sh
RUN chmod +x /opt/airflow/start.sh

WORKDIR /opt/airflow
EXPOSE 8080

CMD ["/opt/airflow/start.sh"]