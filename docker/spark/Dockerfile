# Spark master/worker image.
#
# Identical to apache/spark:3.5.6 except it pre-creates the shared Iceberg
# warehouse mount point as world-writable. A Docker named volume inherits its
# permissions from the image path it is FIRST mounted onto; baking 0777 here
# guarantees that whichever container initializes the `warehouse` volume, the
# Spark EXECUTORS (which run here as uid 185 `spark` and write the actual
# Parquet data files) can create directories in it.
#
# Combined with `fs.permissions.umask-mode=000` (set in the SparkSession and
# spark-defaults.conf), every directory Iceberg creates stays writable by the
# driver (Airflow, uid 50000), Jupyter (root) and the executors alike.
FROM apache/spark:3.5.6

USER root
RUN mkdir -p /opt/billups/warehouse && chmod 777 /opt/billups/warehouse
USER spark
