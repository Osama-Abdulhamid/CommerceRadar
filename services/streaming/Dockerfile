FROM apache/spark:3.5.7-java17-python3

USER root
RUN mkdir -p /state /ivy \
    && chown -R 185:0 /state /ivy

COPY services/streaming/requirements.txt /tmp/requirements.txt
RUN python3 -m pip install --no-cache-dir -r /tmp/requirements.txt

COPY --chown=185:0 contracts/v1/product_observation.schema.json /opt/spark/work-dir/product_observation.schema.json
COPY --chown=185:0 services/streaming/main.py /opt/spark/work-dir/main.py

USER 185
