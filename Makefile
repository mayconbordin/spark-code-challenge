# Billups medallion pipeline — convenience targets.
#
# Two ways to run the pipeline:
#   * Locally in a virtualenv:  make install && make pipeline && make test
#   * On the Docker stack:      make up  (then trigger the Airflow DAG)
#
# Run `make help` for the full list.

VENV := .venv
PY   := $(VENV)/bin/python
PIP  := $(VENV)/bin/pip

# Local run configuration (absolute paths so it works from any kernel CWD).
export BILLUPS_DATA_DIR  := $(CURDIR)/data
export BILLUPS_WAREHOUSE := $(CURDIR)/warehouse
export SPARK_MASTER      ?= local[*]
export SPARK_LOCAL_IP    ?= 127.0.0.1

.DEFAULT_GOAL := help

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
.PHONY: install
install: $(VENV)/.installed ## Create the venv and install the package + dev deps

$(VENV)/.installed: pyproject.toml requirements.txt
	python3 -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	$(PIP) install -e .
	touch $@

# ---------------------------------------------------------------------------
# Pipeline (local)
# ---------------------------------------------------------------------------
.PHONY: pipeline bronze silver gold
pipeline: install ## Build all medallion layers (bronze -> silver -> gold) locally
	$(PY) -m billups_pipeline.pipeline --layers all

bronze: install ## Build only the bronze layer
	$(PY) -m billups_pipeline.pipeline --layers bronze

silver: install ## Build only the silver layer
	$(PY) -m billups_pipeline.pipeline --layers silver

gold: install ## Build only the gold layer
	$(PY) -m billups_pipeline.pipeline --layers gold

.PHONY: questions
questions: install ## Print Q1-Q5 results to the console (sanity report)
	$(PY) scripts/run_questions.py

# ---------------------------------------------------------------------------
# Tests & notebook
# ---------------------------------------------------------------------------
.PHONY: test
test: install ## Run the unit test suite
	$(PY) -m pytest -q

.PHONY: lint
lint: install ## Lint + format-check with ruff
	$(PY) -m ruff check .
	$(PY) -m ruff format --check .

.PHONY: format
format: install ## Auto-fix lint issues and format the code
	$(PY) -m ruff check --fix .
	$(PY) -m ruff format .

.PHONY: precommit
precommit: install ## Run all pre-commit hooks across the repo (what CI runs)
	$(PY) -m pre_commit run --all-files

.PHONY: notebook-run
notebook-run: install ## Execute the questions notebook headless (verifies it runs)
	$(PY) -m jupyter nbconvert --to notebook --execute \
		--ExecutePreprocessor.timeout=900 \
		--output executed_questions.ipynb notebooks/questions.ipynb

.PHONY: lab
lab: install ## Launch JupyterLab locally against the existing warehouse
	$(PY) -m jupyter lab --notebook-dir=notebooks

# ---------------------------------------------------------------------------
# Docker stack
# ---------------------------------------------------------------------------
.PHONY: validate
validate: ## Validate the docker-compose file
	docker compose config -q && echo "docker-compose.yml OK"

.PHONY: build
build: ## Build the Docker images
	docker compose build

.PHONY: up
up: ## Start the full stack (Spark cluster + Jupyter + Airflow)
	docker compose up -d --build

.PHONY: down
down: ## Stop the stack (keep the warehouse volume)
	docker compose down

.PHONY: clean-stack
clean-stack: ## Stop the stack AND delete the warehouse volume
	docker compose down -v

.PHONY: logs
logs: ## Tail logs from all services
	docker compose logs -f

.PHONY: trigger
trigger: ## Trigger the medallion DAG on the running Airflow container
	docker compose exec airflow airflow dags trigger billups_medallion_pipeline

# ---------------------------------------------------------------------------
# Housekeeping
# ---------------------------------------------------------------------------
.PHONY: clean
clean: ## Remove the local warehouse and build artifacts (keeps the venv)
	rm -rf warehouse/* spark-warehouse metastore_db derby.log \
		notebooks/executed_questions.ipynb src/*.egg-info .pytest_cache

.PHONY: clean-all
clean-all: clean ## Also remove the virtualenv
	rm -rf $(VENV)

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'
