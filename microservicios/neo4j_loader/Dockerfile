FROM mirror.gcr.io/library/python:3.10-slim

# Java is required by Spark.
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless curl procps ca-certificates \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

WORKDIR /app


COPY requirements.txt .


RUN pip install --no-cache-dir -r requirements.txt

# We take the Delta Lake JARs into the image so Spark does NOT try to download them at runtime.
# This keeps the container fully offline at run time and avoids Ivy/Maven network issues.
RUN mkdir -p /opt/spark/jars && \
    curl -fSL https://maven-central.storage-download.googleapis.com/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar \
      -o /opt/spark/jars/delta-spark_2.12-3.2.0.jar && \
    curl -fSL https://maven-central.storage-download.googleapis.com/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar \
      -o /opt/spark/jars/delta-storage-3.2.0.jar


COPY . .


CMD ["python", "main.py"]