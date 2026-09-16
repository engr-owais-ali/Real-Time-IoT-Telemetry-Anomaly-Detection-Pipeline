FROM spark:4.2.0-python3

USER root

RUN python3 -m pip install \
    --no-cache-dir \
    "psycopg[binary]"

RUN mkdir -p \
    /app/streaming \
    /checkpoints/dedup \
    /checkpoints/features \
    && chown -R 185:185 /app /checkpoints

COPY --chown=185:185 streaming/ /app/streaming/

USER 185

WORKDIR /app