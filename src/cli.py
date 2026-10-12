"""Command-line entry point for every pipeline step.

Usage (from the project root):
  python -m src.cli verify-source
  python -m src.cli ingest --mode incremental        (or --mode full, --force, --stop-after N)
  python -m src.cli transform-staging
  python -m src.cli build-curated
  python -m src.cli load-postgres
  python -m src.cli train
  python -m src.cli evaluate
  python -m src.cli reproducibility-check
  python -m src.cli run-all                          (every step above, in order)
  python -m src.cli --run-id my_run_1 <command>      (share one run id across steps)
"""
import argparse
import sys
import time

from src import evaluate, ingest, storage, train, transform
from src.common import RunLogger, new_run_id

# the order run-all executes (Airflow uses the same steps as separate tasks)
PIPELINE = ["ingest", "transform-staging", "build-curated", "load-postgres",
            "train", "evaluate", "reproducibility-check"]


def build_parser():
    parser = argparse.ArgumentParser(prog="python -m src.cli")
    parser.add_argument("--run-id", default=None,
                        help="reuse one run id across steps (Airflow passes this)")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("verify-source", help="check source files against the manifest")
    for name in ["ingest", "run-all"]:
        p = sub.add_parser(name, help="copy source data into raw" if name == "ingest"
                           else "run the whole pipeline in order")
        p.add_argument("--mode", choices=["full", "incremental"], default="incremental")
        p.add_argument("--force", action="store_true",
                       help="rebuild all incremental batches from scratch")
        p.add_argument("--stop-after", type=int, default=None,
                       help="simulate a crash after N new batches (recovery demo)")
    sub.add_parser("transform-staging", help="raw -> staging (clean, type, quarantine)")
    sub.add_parser("build-curated", help="staging -> curated (weekly join + features)")
    sub.add_parser("load-postgres", help="curated -> PostgreSQL (rerun-safe upsert)")
    sub.add_parser("train", help="curated -> split, labels, random forests, predictions")
    sub.add_parser("evaluate", help="predictions -> outputs/metrics.json")
    sub.add_parser("reproducibility-check", help="compare metrics with expected baseline")
    return parser


def run_step(step, run_id, logger, args):
    """Call the function that implements one pipeline step."""
    if step == "verify-source":
        ingest.verify_sources(logger)
    elif step == "ingest":
        if args.mode == "full":
            ingest.ingest_full(run_id, logger)
        else:
            ingest.ingest_incremental(run_id, logger, force=args.force, stop_after=args.stop_after)
    elif step == "transform-staging":
        transform.run_staging(run_id, logger)
    elif step == "build-curated":
        transform.run_curated(run_id, logger)
    elif step == "load-postgres":
        storage.load_curated(run_id, logger)
    elif step == "train":
        train.run_training(run_id, logger)
    elif step == "evaluate":
        evaluate.evaluate_model(run_id, logger)
    elif step == "reproducibility-check":
        evaluate.reproducibility_check(run_id, logger)


def main(argv=None):
    args = build_parser().parse_args(argv)
    run_id = args.run_id or new_run_id()
    steps = PIPELINE if args.command == "run-all" else [args.command]
    for step in steps:
        logger = RunLogger(step, run_id)
        logger.info("step started")
        started = time.perf_counter()
        try:
            run_step(step, run_id, logger, args)
        except Exception as exc:
            logger.error("step failed", error_type=type(exc).__name__, error=str(exc),
                         duration_s=round(time.perf_counter() - started, 2))
            return 1          # nonzero exit code = failure (Airflow marks the task failed)
        logger.info("step finished", duration_s=round(time.perf_counter() - started, 2))
    return 0


if __name__ == "__main__":
    sys.exit(main())