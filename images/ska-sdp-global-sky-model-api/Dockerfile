FROM artefact.skao.int/ska-build-python-ubuntu26:1.0.1 AS builder

ENV POETRY_NO_INTERACTION=1
ENV POETRY_VIRTUALENVS_IN_PROJECT=1
ENV POETRY_VIRTUALENVS_CREATE=1

WORKDIR /app

COPY pyproject.toml poetry.lock README.rst LICENSE ./

RUN poetry install --no-root --only main

COPY src ./

RUN poetry install --only main

FROM artefact.skao.int/ska-python-ubuntu26:1.0.1 AS runner

EXPOSE 80

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:80/ping || exit 1

ENV VIRTUAL_ENV=/app/.venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
ENV PYTHONPATH="/src/"
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV TZ=Etc/UTC

USER 1000

WORKDIR /app

COPY --chown=1000:1000 --from=builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}

COPY src/ska_sdp_global_sky_model ./ska_sdp_global_sky_model

COPY --chmod=0755 scripts/db_migrate.sh /db_migrate.sh
COPY --chmod=0755 scripts/db_downgrade.sh /db_downgrade.sh
COPY --chmod=0755 scripts/db_init_data.py /db_init_data.py
COPY tests/data/*.csv /sample_data/
COPY tests/data/metadata_test.json /sample_data/metadata_test.json

CMD ["uvicorn", "ska_sdp_global_sky_model.api.app.main:app", "--host", "0.0.0.0", "--port", "80", "--app-dir", "/usr/src"]
