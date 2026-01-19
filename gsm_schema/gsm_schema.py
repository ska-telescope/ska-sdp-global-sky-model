# flake8: noqa
import io

from sqlalchemy.schema import CreateSchema, CreateTable

from ska_sdp_global_sky_model.api.app.model import *
from ska_sdp_global_sky_model.configuration.config import engine


def write_out_schema():
    create_schema_stmt = str(
        CreateSchema(DB_SCHEMA).compile(engine)) + ";\n"

    metadata = Base().metadata

    for table in metadata.sorted_tables:
        create_schema_stmt += str(
            CreateTable(table).compile(engine)) + ";\n"

    print(create_schema_stmt)


if __name__ == "__main__":
    write_out_schema()
