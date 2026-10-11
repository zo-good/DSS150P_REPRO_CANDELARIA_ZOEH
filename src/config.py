"""Project settings: reads config/settings.yml and .env.

Every path is built from the project root folder, so the code works
on any computer and from any folder (no C:\\Users\\... paths).
"""
import os
from pathlib import Path

import yaml
from dotenv import load_dotenv

# src/config.py -> parent is src/, parent of that is the project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SETTINGS_FILE = PROJECT_ROOT / "config" / "settings.yml"

# read .env (database username/password) into environment variables
load_dotenv(PROJECT_ROOT / ".env")


def load_settings(path=SETTINGS_FILE):
    """Read settings.yml and return it as a Python dictionary."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


SETTINGS = load_settings()


def path_for(key):
    """Full path for a folder listed under 'paths' in settings.yml."""
    return PROJECT_ROOT / SETTINGS["paths"][key]


def source_file(name):
    """Full path to one of the original files in data/source/."""
    return path_for("source_dir") / name


def db_settings():
    """Database connection settings, taken from .env."""
    return {
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "dbname": os.getenv("POSTGRES_DB"),
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5433")),
    }