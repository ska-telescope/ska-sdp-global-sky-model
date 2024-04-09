"""
CRUD functionality goes here.
"""
from sqlalchemy.orm import Session
from sqlalchemy import text


def get_pg_sphere_version(db: Session):
    """
    Requests version information from pg_sphere.
    """
    return db.execute(text("SELECT pg_sphere_version();"))