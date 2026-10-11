"""Command-line entry point for every pipeline step.

Usage (from the project root):
  python -m src.cli verify-source
  python -m src.cli ingest --mode incremental
  python -m src.cli ingest --mode full
  python -m src.cli --run-id my_run_1 ingest --mode incremental --stop-after 2
  python -m src.cli transform-staging
  python -m src.cli build-curated
"""
import argparse
import sys

from src import ingest, transform
from src.common import RunLogger, new_run_id


def build_parser():
    parser = argparse.ArgumentParser(prog="python -m src.cli")
    parser.add_argument("--run-id", default=None,
                        help="reuse one run id across steps (Airflow passes this)")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("verify-source", help="check source files against the manifest")

    p_ingest = sub.add_parser("ingest", help="copy source data into the raw layer")
    p_ingest.add_argument("--mode", choices=["full", "incremental"], default="incremental")
    p_ingest.add_argument("--force", action="store_true",
                          help="rebuild all incremental batches from scratch")
    p_ingest.add_argument("--stop-after", type=int, default=None,
                          help="simulate a crash after N new batches (recovery demo)")
    sub.add_parser("transform-staging", help="raw -> staging (clean, type, quarantine)")
    sub.add_parser("build-curated", help="staging -> curated (weekly join + features)")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    run_id = args.run_id or new_run_id()
    logger = RunLogger(args.command, run_id)
    logger.info("step started", command=args.command)
    try:
        if args.command == "verify-source":
            ingest.verify_sources(logger)
        elif args.command == "ingest":
            if args.mode == "full":
                ingest.ingest_full(run_id, logger)
            else:
                ingest.ingest_incremental(run_id, logger, force=args.force,
                                          stop_after=args.stop_after)
        elif args.command == "transform-staging":
            transform.run_staging(run_id, logger)
        elif args.command == "build-curated":
            transform.run_curated(run_id, logger)
    except Exception as exc:
        logger.error("step failed", error_type=type(exc).__name__, error=str(exc))
        return 1
    logger.info("step finished")
    return 0


if __name__ == "__main__":
    sys.exit(main())