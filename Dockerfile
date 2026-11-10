FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY requirements.txt /tmp/pulsecart-requirements.txt
USER airflow
RUN pip install --no-cache-dir -r /tmp/pulsecart-requirements.txt

WORKDIR /opt/pulsecart
ENV PULSECART_HOME=/opt/pulsecart
ENV PYTHONPATH=/opt/pulsecart/src

