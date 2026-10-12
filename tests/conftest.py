"""Shared test fixtures.

`sandbox_project` copies the config, metadata and source data into a temporary folder and
points every module at it, so integration tests never touch your real data/ or state/.
"""
import shutil

import pytest

import src.benchmark
import src.config
import src.ingest
import src.publish
import src.transform
import src.validate

REAL_ROOT = src.config.PROJECT_ROOT
MODULES_WITH_ROOT = [src.config, src.ingest, src.transform, src.validate, src.publish, src.benchmark]


@pytest.fixture(scope="session")
def sandbox_project(tmp_path_factory):
    source = REAL_ROOT / "data" / "source"
    needed = ["doh-epi-dengue-data-2016-2021.csv", *src.config.SETTINGS["source"]["weather_files"]]
    missing = [n for n in needed if not (source / n).exists()]
    if missing:
        pytest.skip(f"source files not in data/source/: {missing} (see README: data acquisition)")

    root = tmp_path_factory.mktemp("project")
    shutil.copytree(REAL_ROOT / "config", root / "config")
    shutil.copytree(REAL_ROOT / "metadata", root / "metadata")
    (root / "data" / "source").mkdir(parents=True)
    for name in needed:
        shutil.copyfile(source / name, root / "data" / "source" / name)

    mp = pytest.MonkeyPatch()
    for module in MODULES_WITH_ROOT:
        mp.setattr(module, "PROJECT_ROOT", root, raising=False)
    yield root
    mp.undo()


class ListLogger:
    """Stand-in for RunLogger that just remembers messages (no files written)."""

    def __init__(self):
        self.messages = []

    def info(self, message, **fields):
        self.messages.append(("INFO", message, fields))

    def warning(self, message, **fields):
        self.messages.append(("WARNING", message, fields))

    def error(self, message, **fields):
        self.messages.append(("ERROR", message, fields))


@pytest.fixture
def logger():
    return ListLogger()