"""
Configure variables to be used.
"""

from pathlib import Path

from starlette.config import Config

ENV_FILE = Path(".env")
if not ENV_FILE.exists():
    ENV_FILE = None

config = Config(ENV_FILE)

# DB (Postgres)
DB_NAME: str = config("DB_NAME", default="postgres")
DB_USER: str = config("DB_USER", default="postgres")
DB_PASSWORD: str = config("DB_PASSWORD", default="pass")
DB: str = config("DB", default="db")
DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB}:5432/{DB_NAME}"

# Session DB (Redis)
SESSION_DB_NAME: int = config("SESSION_DB_NAME", default=0)
SESSION_DB_HOST: str = config("SESSION_DB_HOST", default="session-db")
SESSION_DB_PORT: int = config("SESSION_DB_PORT", default=6379)
SESSION_DB_TOKEN_KEY: str = config("SESSION_DB_TOKEN_KEY", default="secret")
