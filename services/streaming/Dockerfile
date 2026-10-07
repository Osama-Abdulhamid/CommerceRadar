FROM apache/spark:3.5.7-java17-python3

USER root
RUN mkdir -p /state /ivy \
    && chown -R 185:0 /state /ivy

COPY --chown=185:0 services/streaming/main.py /opt/spark/work-dir/main.py

USER 185
