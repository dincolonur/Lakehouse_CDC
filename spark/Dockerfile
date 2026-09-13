# Built on a plain OpenJDK base + pip-installed PySpark, instead of
# apache/spark-py, whose Docker Hub tag naming (e.g. "v3.5.1") turned out not
# to resolve reliably. `pip install pyspark==<version>` is a well-defined,
# always-correct pin straight from PyPI, and the pyspark wheel bundles the
# full Spark runtime (including spark-submit) plus installs it on PATH.
#
# Delta Lake and the Kafka connector are NOT installed here via pip -- they
# are resolved from Maven at `spark-submit --packages ...` time (see
# docker-compose.yml's spark-streamer command and scripts/run_demo.sh).
# See spark/jobs/spark_common.py for why that has to be a spark-submit CLI
# flag rather than a pip package or an in-script .config() call.
FROM eclipse-temurin:17-jre-jammy

ENV PYSPARK_VERSION=3.5.1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/python3 /usr/bin/python

RUN pip3 install --no-cache-dir pyspark==${PYSPARK_VERSION}

WORKDIR /opt/jobs

# Kept alive by docker-compose's `command: tail -f /dev/null` (for the `spark`
# admin container) or run continuously with `--mode stream` (for
# `spark-streamer`); jobs are run via `spark-submit --packages ... <script>.py`.
