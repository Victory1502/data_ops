FROM apache/spark-py:v3.4.0

USER root
RUN apt-get update && apt-get install -y python3-pip && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir pandas

USER 1001
WORKDIR /app
COPY . /app