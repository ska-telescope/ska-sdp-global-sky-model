"""
CRUD functionality goes here.
"""

from sqlalchemy import text
from sqlalchemy.orm import Session


def get_pg_sphere_version(db: Session):
    """
    Requests version information from pg_sphere.
    """
    return db.execute(text("SELECT pg_sphere_version();"))
