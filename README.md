# Billups — Data Engineering Case Study

A PySpark **medallion-architecture** pipeline (Bronze → Silver → Gold) over
**Apache Iceberg** tables that joins `merchants` with `historical_transactions`
and answers the five case-study questions. The stack runs locally in a
virtualenv or on a Dockerized **Spark cluster** orchestrated by **Apache
Airflow** with **JupyterLab** wired to the cluster.

- **Report (answers to Q1–Q5):** [`report below`](#report--answers) and the executable
  [`notebooks/questions.ipynb`](notebooks/questions.ipynb).
- **Infrastructure details:** [`INFRASTRUCTURE.md`](INFRASTRUCTURE.md).
- **Transformations are written with the PySpark DataFrame API** (no Spark SQL),
  per the brief.

---

## 1. Architecture

```
 merchants-subset.csv ─┐                         ┌── billups.bronze.merchants
                       ├─► BRONZE (raw landing) ──┤
 historical_txns/*.parquet                        └── billups.bronze.transactions
                       │
                       ▼
            SILVER (typed, cleaned, conformed)
              • billups.silver.merchants      ← deduped to 1 row / merchant_id
              • billups.silver.transactions   ← parsed timestamps, time dims,
                                                 null category → "Unknown category"
                       │
                       ▼
              GOLD (analytics-ready join)
              • billups.gold.transactions_enriched
                 transactions ⟕ merchants on merchant_id
                 merchant_name = coalesce(merchant_name, merchant_id)
                       │
                       ▼
          questions.ipynb  (one cell per question)
```

Each layer is an idempotent, create-or-replace Iceberg (format v2) table in the
`billups` catalog (a hadoop/filesystem catalog — no external metastore needed).

| Layer  | Table                                   | Rows        | Notes                                  |
| ------ | --------------------------------------- | ----------- | -------------------------------------- |
| Bronze | `billups.bronze.transactions`           | 7,274,367   | Parquet landed verbatim                |
| Bronze | `billups.bronze.merchants`              | 334,696     | CSV landed as strings                  |
| Silver | `billups.silver.transactions`           | 7,274,367   | Typed + time dimensions + clean category |
| Silver | `billups.silver.merchants`              | 334,633     | Deduplicated to one row per `merchant_id` |
| Gold   | `billups.gold.transactions_enriched`    | 7,274,367   | Join (no fan-out); 34,570 rows use id-as-name |

---

## 2. Repository layout

```
billups/
├── src/billups_pipeline/        # the pipeline package
│   ├── config.py                # env-driven settings + table identifiers
│   ├── session.py               # SparkSession wired for the Iceberg catalog
│   ├── schemas.py               # explicit raw read schema (merchants CSV)
│   ├── transforms.py            # PURE DataFrame transforms + Q1–Q4 logic
│   ├── bronze.py / silver.py / gold.py   # one module per layer
│   ├── io_utils.py              # Iceberg namespace + write helpers
│   └── pipeline.py              # CLI: `python -m billups_pipeline.pipeline`
├── notebooks/questions.ipynb    # answers — one cell per question
├── dags/medallion_pipeline_dag.py   # Airflow DAG: bronze → silver → gold
├── tests/test_transforms.py     # unit tests for the transforms (pytest)
├── docker-compose.yml           # Spark master/worker + Jupyter + Airflow
├── docker/ , conf/              # Dockerfiles + spark-defaults.conf
├── scripts/run_questions.py     # console sanity report
├── .pre-commit-config.yaml      # ruff lint/format + hygiene + pytest (pre-push)
├── .github/workflows/ci.yml     # CI: pre-commit lint + pytest
├── Makefile                     # `make help` for all operations
└── data/                        # input datasets (place provided files here)
```

---

## 3. Running it

### Option A — locally (fastest)

Requires Python 3.9+ and a JDK (Java 17 works).

```bash
make install     # venv + deps (incl. pyspark==3.5.6)
make pipeline    # build bronze → silver → gold Iceberg tables in ./warehouse
make test        # run the unit tests
make questions   # print Q1–Q5 results to the console
make lab         # open JupyterLab on notebooks/questions.ipynb
```

> The first Spark run downloads the Iceberg runtime jar from Maven Central
> (cached afterwards under `~/.ivy2`).

### Option B — full Docker stack (Spark cluster + Airflow + Jupyter)

```bash
make validate    # docker compose config -q
make up          # build & start spark-master, spark-worker, jupyter, airflow
make trigger     # run the billups_medallion_pipeline DAG
```

Then open:

- **Airflow** <http://localhost:8090> (`admin`/`admin`) — trigger / watch the DAG
- **JupyterLab** <http://localhost:8888> — run `notebooks/questions.ipynb`
- **Spark master UI** <http://localhost:8080>, **worker UI** <http://localhost:8081>

See [`INFRASTRUCTURE.md`](INFRASTRUCTURE.md) for the service topology,
the shared Iceberg warehouse volume, ports, and versions.

---

## 4. Data model & assumptions

The transactions and merchants files **both** carry `city_id` and `state_id`.
Because the questions analyse *where sales happen* — and the Q2 example shows the
same merchant in multiple states (impossible if we used the merchant's single
registered state) — **all geography in the analysis is transaction-level**
(`city_id`, `state_id` from `historical_transactions`). The merchant's own
location is still carried into Gold as `merchant_city_id` / `merchant_state_id`.

Assumptions (also noted inline where relevant):

1. **Merchant name fallback** — Gold left-joins transactions to merchants;
   when a `merchant_id` has no merchant row, `merchant_id` itself is used as the
   name (34,570 such transactions).
2. **Merchant dedup** — the merchants extract contains duplicate `merchant_id`s;
   Silver keeps one deterministic row per id (lowest `merchant_group_id`,
   `city_id`, `merchant_category_id`) so the Gold join cannot fan out. Verified:
   Gold row count == transaction row count (7,274,367).
3. **Categories** — `null`/blank `category` → `"Unknown category"`; **no records
   are dropped**.
4. **Month** = calendar month of `purchase_date` (e.g. `Oct 2017`).
   **Hour** = hour of `purchase_date` on a 24h clock (`1300`, `0800`).
5. **"Purchase Total"** = `sum(purchase_amount)`; **"No of sales"** = transaction
   count.
6. `purchase_amount` is treated as the monetary value already in the parquet
   (no rescaling).

**Dataset shape (computed):** 7,274,367 transactions spanning **Jan 2017 –
Feb 2018** (14 months), **307 cities**, **25 states**, **4 categories**
(`A`, `B`, `C`, `Unknown category`). Notably the **average ticket is ~20,100
across every city, category and month** — the spread between merchants/cities is
driven by **transaction volume**, not ticket size.

---

## Report — Answers

> Figures below were produced by running the pipeline on the full dataset
> (`make pipeline && make questions`). The notebook reproduces every table.

### Q1 — Top 5 merchants by purchase total, per city, per month

`q1_top_merchants_by_city_month` aggregates `(month, city, merchant)` →
`sum(purchase_amount)`, `count(*)`, then ranks within each `(month, city)` and
keeps the top 5 (**21,374 rows** total). Sample (Jan 2017):

| Month    | City | Merchant            | Purchase Total | No of sales |
| -------- | ---- | ------------------- | -------------: | ----------: |
| Jan 2017 | 1    | Cesar Hall inc      |     41,693,919 |       2,076 |
| Jan 2017 | 1    | Mary Gray 7 inc     |     24,014,490 |       1,206 |
| Jan 2017 | 1    | Kathie Sughrue inc  |     22,636,282 |       1,131 |
| Jan 2017 | 1    | Steven Russell inc  |     19,316,188 |         953 |
| Jan 2017 | 1    | Maxine Flores inc   |     15,509,701 |         756 |
| Jan 2017 | 4    | Louise Cole 2 inc   |      1,730,987 |          88 |
| Jan 2017 | 4    | Nathaniel Stewart inc |    1,245,798 |          63 |

### Q2 — Average sale amount per merchant per state (largest first)

`q2_avg_sale_by_merchant_state` groups by `(merchant, state_id)` →
`avg(purchase_amount)`, ordered descending. The **literal** ranking is dominated
by merchants with a **single** transaction (average == that one sale, all
≈ 39,9xx) — a small-sample artifact:

| Merchant            | State ID | Average Amount | No of sales |
| ------------------- | -------- | -------------: | ----------: |
| Martha Tyrrell inc  | 7        |       39,937.6 |           1 |
| Julie Mckelvey inc  | 9        |       39,727.6 |           1 |
| Jennifer Pool inc   | 24       |       39,658.7 |           1 |

A more meaningful **high average-ticket** view (merchants with ≥ 500 sales, top
per state) is included in the notebook — those averages cluster tightly at
~20,800–21,000, consistent with the uniform ticket size:

| Merchant             | State ID | Average Amount | No of sales |
| -------------------- | -------- | -------------: | ----------: |
| Cynthia Vargas 2 inc | 9        |       21,024.5 |         582 |
| Richard Avalos inc   | 5        |       20,962.5 |         569 |
| Janice Whiteman inc  | 13       |       20,938.9 |         560 |

### Q3 — Top 3 hours by total purchase_amount, per category

| Product Category | Hour |
| ---------------- | ---- |
| A                | 1200 |
| A                | 1300 |
| A                | 1700 |
| B                | 1300 |
| B                | 1200 |
| B                | 1400 |
| C                | 1700 |
| C                | 1600 |
| C                | 1500 |
| Unknown category | 0000 |
| Unknown category | 1400 |
| Unknown category | 1300 |

A, B, C all peak in the **midday-to-late-afternoon** window (12:00–17:00).
`Unknown category`'s top "hour" is **00:00**, which is a **data-quality
artifact**: many transactions carry date-only timestamps (`… 00:00:00`), piling
onto midnight. The genuine intraday signal is the 12:00–17:00 band.

### Q4 — Where are the most popular merchants, and does city relate to category?

Popularity = number of sales transactions. `q4_popular_merchants_by_city`
reports each merchant's total transactions and its **primary city** (where it
records the most):

| Merchant           | Total sales | Primary city | Sales in primary city |
| ------------------ | ----------: | -----------: | --------------------: |
| John Miller 7 inc  |     279,377 |           69 |               264,587 |
| Kathie Sughrue inc |     106,946 |            1 |               106,941 |
| Cesar Hall inc     |      90,106 |            1 |                82,128 |
| Todd Turner 3 inc  |      45,912 |           69 |                45,912 |

The most popular merchants concentrate in a few **high-volume hub cities**,
above all **city 69** (1.21M transactions) and **city 1** (0.65M) — together ~25%
of all transactions.

**Correlation between `city_id` and `category`:** Cramér's V = **0.181** on the
city×category contingency table (χ² ≈ 715k, N = 7.27M) — a **weak but
statistically real** association. The mechanism is visible in the category mix
of the busiest cities: most are **A-dominant (~54–60%)**, but **city 1 is
B-dominant (69% B)** — an outlier that drives much of the association. So
location does shift the category mix, but only modestly; category is not
strongly determined by city.

### Q5 — Advice for a new merchant

> Strictly from the historical transactions. The uniform ~20,100 ticket means
> **opportunity ≈ transaction volume**, so the advice optimizes for volume.

**a. Which cities?** Focus on the high-volume hubs — **city 69** and **city 1**
first, then **19, 158, 17, 331**. City 69 alone carries 1.21M transactions
(≈ 24.3B in sales); the top 5 cities account for the bulk of national volume.

**b. Which categories?** Sell **category A**, and add **B**. By revenue:
A = 52.9%, B = 40.1%, C = 6.3%, Unknown = 0.6% — A and B together are ~93% of the
market. (If targeting **city 1**, lead with **B**, which dominates there.)

**c. Seasonal periods?** Sales **grow steadily through 2017 and peak in
December** (852k transactions / ~17.1B — the year's high), with the **Oct–Dec
ramp** clearly the strongest stretch; Jan–Feb 2018 stay elevated but ease off the
December peak. Plan inventory/staffing for a **Q4 holiday surge**.

| Month    | Revenue (B) | Txns    |
| -------- | ----------: | ------: |
| Jul 2017 |        10.1 | 503,313 |
| Oct 2017 |        12.6 | 627,410 |
| Nov 2017 |        14.2 | 706,032 |
| **Dec 2017** |    **17.1** | **852,200** |
| Jan 2018 |        14.3 | 713,701 |

**d. Opening hours?** Trade roughly **09:00 → 22:00**, with **core hours
11:00–20:00** capturing the vast majority of sales (peak at **13:00**). Volume is
negligible 02:00–06:00. *(Discount the midnight spike — it is timestamp
truncation, not real demand; see Q3.)*

**e. Accept installment payments?** **Only short plans (≤ 2 installments);
decline longer ones.** Model (stated assumptions): gross margin = 25% of price,
defaulters pay exactly half, and the 22.9% monthly default hazard compounds over
the *n*-month horizon, so `P(default) = 1 − (1 − 0.229)ⁿ` and expected profit
factor = `0.25 − 0.5·P(default)`:

| Installments | P(default) | Profit factor | Verdict          |
| -----------: | ---------: | ------------: | ---------------- |
| cash / 1     |      0.000 |       +0.250  | full margin      |
| 2            |      0.406 |       +0.047  | marginal         |
| 3            |      0.542 |       −0.021  | **loss**         |
| 6            |      0.790 |       −0.145  | **heavy loss**   |
| 12           |      0.956 |       −0.228  | **near-total loss** |

Across the actual installment transactions, expected profit turns **negative
from 3 monthly installments onward**. Recommendation: offer **cash or at most a
2-month plan**, price longer plans to cover default risk, or require a deposit.
*Sensitivity:* if 22.9% is a **flat** per-sale default rate (not compounding
monthly), every installment sale keeps a +0.135 profit factor and plans become
acceptable — so this conclusion hinges on the "per month" reading.

---

## 5. Testing

`make test` runs the pytest suite (`tests/test_transforms.py`, 10 tests) against
a local SparkSession on tiny in-memory inputs — no Iceberg or real data needed.
Coverage: merchant dedup, null/blank-category cleaning, time-dimension
derivation, the name-fallback join, no-fan-out on duplicate merchants, and each
of Q1–Q4's ranking/aggregation logic.

The notebook is also validated headless via `make notebook-run`.

## 6. Continuous integration & linting

[`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs on every push / PR
with two jobs:

- **lint** — runs the `pre-commit` hooks (ruff lint + ruff-format + file-hygiene
  checks) via `pre-commit/action`.
- **test** — sets up JDK 17 + Python 3.11, installs `.[dev]`, and runs `pytest`.

Locally, the same checks are available through `make`:

```bash
make lint        # ruff check + ruff format --check
make format      # auto-fix lint + format
make precommit   # run all pre-commit hooks across the repo (mirrors CI lint)
make test        # pytest
```

To enable the git hooks locally (lint on commit, tests on push):

```bash
pre-commit install --install-hooks      # lint/format/hygiene on `git commit`
pre-commit install --hook-type pre-push # unit tests on `git push`
```

Ruff is configured in `pyproject.toml` (`[tool.ruff]`); the hook set lives in
`.pre-commit-config.yaml`.
