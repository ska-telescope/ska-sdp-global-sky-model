"""
Configure variables to be used.
"""

import os

# DB (Postgres)
DB_NAME = "postgres"
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@db:5432/{DB_NAME}"

# Session DB (Redis)
SESSION_DB_NAME = 0
SESSION_DB_HOST = "session-db"
SESSION_DB_PORT = 6379
SESSION_DB_TOKEN_KEY = "secret"
