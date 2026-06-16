# JupyterLab built ON TOP of the exact Spark image used by the cluster so the
# driver (Jupyter) and the executors run identical Spark 3.5.6 / Scala 2.12.
# Using jupyter/pyspark-notebook instead would risk a Spark version mismatch.
FROM apache/spark:3.5.6

# pip install + writing the workdir needs root; the base image runs as "spark".
USER root

# JupyterLab for interactive exploration; pandas for small local dataframes
# and toPandas() previews of Spark results.
RUN pip install --no-cache-dir \
        jupyterlab==4.* \
        pandas

# Pre-create the shared Iceberg warehouse mount point world-writable so the
# named volume is usable by every service regardless of which one starts first
# (Airflow runs as uid 50000, this container as root). See airflow Dockerfile.
RUN mkdir -p /opt/billups/warehouse && chmod 777 /opt/billups/warehouse

# Notebooks are mounted here from the host (./notebooks) at runtime.
WORKDIR /opt/billups/notebooks

# JupyterLab UI.
EXPOSE 8888
