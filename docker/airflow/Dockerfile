# Airflow image with a JRE and a matching PySpark.
#
# The DAG drives the pipeline via BashOperator (see dags/medallion_pipeline_dag.py),
# invoking `python -m billups_pipeline.pipeline` directly. The package
# self-configures its SparkSession from env vars and connects to the cluster via
# SPARK_MASTER. The Spark DRIVER therefore runs inside THIS container, so it
# needs both PySpark and a JVM.
#
# We deliberately do NOT install apache-airflow-providers-apache-spark: the DAG
# uses BashOperator (not SparkSubmitOperator), and the current provider requires
# apache-airflow>=2.11, which would try to upgrade Airflow to 3.x and trigger an
# unsolvable pip backtrack. PySpark alone (only needs py4j) keeps the build fast
# and the Airflow runtime pinned at 2.10.5.
FROM apache/airflow:2.10.5

# Java is required by the Spark driver but is not in the base Airflow image.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Pre-create the shared Iceberg warehouse mount point world-writable. A Docker
# named volume inherits ownership/permissions from the image path it is first
# mounted onto, so making it 0777 here lets both Airflow (uid 50000) and the
# root-running Jupyter container write to the same shared catalog.
RUN mkdir -p /opt/billups/warehouse && chmod 777 /opt/billups/warehouse

USER airflow

# Pin PySpark to the cluster version so the Airflow-side driver matches the
# spark-master / spark-worker (3.5.6). pyspark only pulls in py4j, so this does
# not disturb Airflow's own dependency set.
RUN pip install --no-cache-dir "pyspark==3.5.6"
