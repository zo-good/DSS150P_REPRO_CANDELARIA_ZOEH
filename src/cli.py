"""Command-line entry point for every pipeline step.

Usage (from the project root):
  python -m src.cli run-all                          the whole pipeline, in order
  python -m src.cli run-all --inject-failure negative_cases     failure-injection demo
  python -m src.cli <step>                           one step (see the list below)
  python -m src.cli --run-id my_run_1 <step>         share one run id across steps

Steps: verify-source, ingest [--mode full|incremental --force --stop-after N],
validate-raw, transform-staging, build-curated [--inject-failure X], load-postgres,
validate-curated, train, evaluate, reproducibility-check, publish-outputs, benchmark
"""
import argparse
import sys
import time

from src import benchmark, evaluate, ingest, publish, storage, train, transform, validate
from src.common import RunLogger, new_run_id

# the order run-all executes; the Airflow DAG runs the same steps as separate tasks
PIPELINE = ["ingest", "validate-raw", "transform-staging", "build-curated", "load-postgres",
            "validate-curated", "train", "evaluate", "reproducibility-check", "publish-outputs"]

STEPS = {
    "verify-source": lambda run_id, log, a: ingest.verify_sources(log),
    "ingest": lambda run_id, log, a: (
        ingest.ingest_full(run_id, log) if a.mode == "full"
        else ingest.ingest_incremental(run_id, log, force=a.force, stop_after=a.stop_after)),
    "validate-raw": lambda run_id, log, a: validate.run_validate_raw(run_id, log),
    "transform-staging": lambda run_id, log, a: transform.run_staging(run_id, log),
    "build-curated": lambda run_id, log, a: transform.run_curated(run_id, log, inject=a.inject_failure),
    "load-postgres": lambda run_id, log, a: storage.load_curated(run_id, log),
    "validate-curated": lambda run_id, log, a: validate.run_validate_curated(run_id, log),
    "train": lambda run_id, log, a: train.run_training(run_id, log),
    "evaluate": lambda run_id, log, a: evaluate.evaluate_model(run_id, log),
    "reproducibility-check": lambda run_id, log, a: evaluate.reproducibility_check(run_id, log),
    "publish-outputs": lambda run_id, log, a: publish.run_publish(run_id, log),
    "benchmark": lambda run_id, log, a: benchmark.run_benchmark(run_id, log),
}


def build_parser():
    parser = argparse.ArgumentParser(prog="python -m src.cli")
    parser.add_argument("--run-id", default=None,
                        help="reuse one run id across steps (Airflow passes this)")
    sub = parser.add_subparsers(dest="command", required=True)
    for name in [*STEPS, "run-all"]:
        p = sub.add_parser(name)
        p.add_argument("--mode", choices=["full", "incremental"], default="incremental",
                       help="ingest: full refresh or only new batches")
        p.add_argument("--force", action="store_true", help="ingest: rebuild all batches")
        p.add_argument("--stop-after", type=int, default=None,
                       help="ingest: simulate a crash after N new batches")
        p.add_argument("--inject-failure", choices=sorted(transform.INJECTIONS), default=None,
                       help="build-curated: corrupt this run's curated table on purpose (demo)")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    run_id = args.run_id or new_run_id()
    steps = PIPELINE if args.command == "run-all" else [args.command]
    for step in steps:
        logger = RunLogger(step, run_id)
        logger.info("step started")
        started = time.perf_counter()
        try:
            STEPS[step](run_id, logger, args)
        except Exception as exc:
            logger.error("step failed", error_type=type(exc).__name__, error=str(exc),
                         duration_s=round(time.perf_counter() - started, 2))
            return 1          # nonzero exit code = failure (Airflow marks the task failed)
        logger.info("step finished", duration_s=round(time.perf_counter() - started, 2))
    return 0


if __name__ == "__main__":
    sys.exit(main())