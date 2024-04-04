from sqlalchemy import Column, Integer, String

from db import Base


class PointSource(Base):
    __tablename__ = "point_sources"

    id = Column(Integer, primary_key=True, index=True)
    RA = Column(String, default=True)
    DEC = Column(String, default=True)