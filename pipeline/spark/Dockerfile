FROM apache/spark:3.5.3
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 \
    && rm -rf /var/lib/apt/lists/*
COPY forex_stream.py /opt/forex/forex_stream.py
RUN mkdir -p /tmp/spark-checkpoints && chown -R 185:0 /tmp/spark-checkpoints
ENV PYSPARK_PYTHON=python3
USER 185
ENTRYPOINT ["/opt/spark/bin/spark-submit"]
CMD ["--master", "local[2]", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3", "/opt/forex/forex_stream.py"]
