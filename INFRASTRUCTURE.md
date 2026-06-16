# Infrastructure

Local Docker Compose stack for the `billups` PySpark medallion-architecture
pipeline. Everything runs on a single machine and shares one Apache Iceberg
warehouse.

## Services & ports

| Service        | Image / build                         | Host port | Purpose                                   |
| -------------- | ------------------------------------- | --------- | ----------------------------------------- |
| `spark-master` | `docker/spark/Dockerfile`             | 7077      | Spark standalone master RPC               |
|                |                                       | 8080      | Spark **master** web UI                   |
| `spark-worker` | `docker/spark/Dockerfile`             | 8081      | Spark **worker** web UI (~2 cores / 2G)   |
| `jupyter`      | `docker/jupyter/Dockerfile`           | 8888      | JupyterLab (Spark driver), no auth token  |
| `airflow`      | `docker/airflow/Dockerfile`           | 8090      | Airflow webserver UI                      |

URLs (after `docker compose up`):

- Spark master UI: <http://localhost:8080>
- Spark worker UI: <http://localhost:8081>
- JupyterLab:      <http://localhost:8888>  (no token — local dev only)
- Airflow:         <http://localhost:8090>  (login **`admin` / `admin`**)

> Port note: Airflow's internal webserver listens on 8080, which collides with
> the Spark master UI. We keep the Spark master UI on host `8080` and remap
> Airflow to host **`8090`**.

## How they connect

- The Spark cluster is **standalone**: `spark-worker` registers with
  `spark://spark-master:7077` over the shared `billups` Docker network.
- `jupyter` and the Airflow DAG tasks act as **Spark drivers**. They set
  `SPARK_MASTER=spark://spark-master:7077` and submit work to the cluster.
- The DAG `billups_medallion_pipeline` (in `dags/`) runs three sequential
  `BashOperator` tasks — `bronze -> silver -> gold` — each invoking
  `python -m billups_pipeline.pipeline --layers <layer>`. BashOperator is used
  (rather than `SparkSubmitOperator`) because the `billups_pipeline` package
  self-configures its `SparkSession` from environment variables, so there is no
  spark-submit classpath/`--conf` juggling — and it avoids pulling in the Spark
  provider (see "Build notes" below).

## Airflow auth

We deliberately do **not** use `airflow standalone`: it generates a *random*
admin password (written to `standalone_admin_password.txt`) and ignores the
`_AIRFLOW_WWW_USER_*` env vars, so there is no stable login. Instead the airflow
service command runs `airflow db migrate`, creates a fixed **`admin` / `admin`**
user via `airflow users create`, then starts the scheduler + webserver. It uses
the default SQLite metadata DB with the SequentialExecutor — fine for this
manual, sequential DAG.

## Shared Iceberg warehouse & permissions

The Iceberg catalog `billups` uses a **hadoop** (filesystem) catalog, so the
warehouse is just a directory. A single named Docker volume `warehouse` is
mounted at the **identical path `/opt/billups/warehouse`** in `spark-master`,
`spark-worker`, `jupyter`, and `airflow`. No external metastore or REST catalog
is required.

Because the Spark **driver** (Airflow, uid 50000) and the **executors**
(spark-worker, uid 185) write to that one volume as different users, two things
make cross-user writes work:

1. **All images pre-create `/opt/billups/warehouse` as `0777`.** A Docker named
   volume inherits permissions from the image path it is first mounted onto, so
   whichever container initializes the volume leaves it writable. This is why
   `spark-master`/`spark-worker` use a tiny custom image (`docker/spark/Dockerfile`)
   instead of `apache/spark:3.5.6` directly.
2. **`spark.hadoop.fs.permissions.umask-mode=000`** (set both in
   `billups_pipeline.session.build_spark()` and in `conf/spark-defaults.conf`),
   so every directory/file Iceberg creates stays world-writable.

## Mounts & environment

All Spark-capable services receive:

- `warehouse` named volume -> `/opt/billups/warehouse` (read/write)
- `./data` -> `/opt/billups/data` (**read-only** — the pipeline only reads input)
- `./src` -> `/opt/billups/src` (read-only)
- `./conf` -> `/opt/billups/conf` (read-only; `SPARK_CONF_DIR` points here so
  `spark-defaults.conf` is honored for spark-submit / Jupyter parity)

Key env vars (consumed by `billups_pipeline.config`):

- `BILLUPS_DATA_DIR=/opt/billups/data`
- `BILLUPS_WAREHOUSE=/opt/billups/warehouse`
- `SPARK_MASTER=spark://spark-master:7077`
- `BILLUPS_ICEBERG_PACKAGES=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1`
- `PYTHONPATH=/opt/billups/src` (the package is **mounted, not pip-installed**)

## Versions & build notes

- **Spark 3.5.6**, Scala 2.12 — chosen as the latest 3.5.x line with mature
  Apache **Iceberg 1.9.1** support.
- Iceberg runtime jar: `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1`.
- Airflow `2.10.5` with `pyspark==3.5.6` and a JRE (the Spark driver runs in the
  Airflow container). We intentionally do **not** install
  `apache-airflow-providers-apache-spark`: its current release requires
  `apache-airflow>=2.11`, which would try to upgrade Airflow to 3.x and trigger
  an unsolvable pip dependency backtrack. The DAG uses `BashOperator`, so the
  provider is unnecessary.

## Caveats

- **First run needs internet.** `spark.jars.packages` downloads the Iceberg
  runtime jar from Maven Central on first session start; it is cached under
  `~/.ivy2` afterwards.
- Jupyter has **no auth token** and Airflow uses a trivial `admin/admin` login —
  acceptable for local dev only, not for any shared/remote deployment.
- SQLite + SequentialExecutor: fine for this manual, sequential DAG, but not for
  high task parallelism.
- The `./data` mount is read-only; all pipeline output lands in the shared
  `warehouse` volume.

## Quick start

```bash
docker compose config -q     # validate the compose file
docker compose up -d --build # build images and start the stack
# Trigger the DAG from the Airflow UI (http://localhost:8090, admin/admin) or:
docker compose exec airflow airflow dags trigger billups_medallion_pipeline
```
