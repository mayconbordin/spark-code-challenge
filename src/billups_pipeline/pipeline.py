"""CLI entrypoint that orchestrates the medallion layers.

Examples
--------
    python -m billups_pipeline.pipeline --layers all
    python -m billups_pipeline.pipeline --layers bronze silver
    python -m billups_pipeline.pipeline --layers gold

Each layer is also invoked individually by the Airflow DAG (one task per layer).
"""

from __future__ import annotations

import argparse
import logging

from .bronze import run_bronze
from .config import load_settings
from .gold import run_gold
from .session import build_spark
from .silver import run_silver

LAYERS = ("bronze", "silver", "gold")

log = logging.getLogger(__name__)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Billups medallion pipeline")
    parser.add_argument(
        "--layers",
        nargs="+",
        default=["all"],
        help="Layers to run: any of bronze/silver/gold, or 'all'.",
    )
    return parser.parse_args(argv)


def resolve_layers(requested: list[str]) -> list[str]:
    """Expand 'all' and preserve canonical bronze->silver->gold order."""
    if "all" in requested:
        return list(LAYERS)
    invalid = [layer for layer in requested if layer not in LAYERS]
    if invalid:
        raise ValueError(f"Unknown layer(s): {invalid}. Choose from {LAYERS}.")
    return [layer for layer in LAYERS if layer in requested]


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    args = _parse_args(argv)
    layers = resolve_layers(args.layers)

    settings = load_settings()
    spark = build_spark(settings)
    log.info("Running layers %s (master=%s)", layers, settings.master)

    runners = {"bronze": run_bronze, "silver": run_silver, "gold": run_gold}
    try:
        for layer in layers:
            runners[layer](spark, settings)
    finally:
        spark.stop()
    log.info("Pipeline finished: %s", layers)


if __name__ == "__main__":
    main()
