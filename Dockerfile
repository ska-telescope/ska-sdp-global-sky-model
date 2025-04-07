FROM artefact.skao.int/ska-build-python:0.1.1 AS builder

RUN pip install --user --upgrade poetry==2.0.1

ENV POETRY_NO_INTERACTION=1
ENV POETRY_VIRTUALENVS_IN_PROJECT=1
ENV POETRY_VIRTUALENVS_CREATE=1

WORKDIR /src
COPY pyproject.toml poetry.lock ./

# Install just the dependencies
RUN poetry install --only main --no-root

COPY README.rst LICENSE  ./
COPY src ./src

RUN poetry install --only main

FROM artefact.skao.int/ska-python:0.1.2 AS runner

ENV VIRTUAL_ENV=/src/.venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
ENV PYTHONPATH="$PYTHONPATH:/src/"
ENV PYTHONUNBUFFERED=1
ENV TZ=Etc/UTC
ENV PYTHONDONTWRITEBYTECODE=1

COPY --from=builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}

WORKDIR /src

COPY src/ska_sdp_global_sky_model ./ska_sdp_global_sky_model

FROM runner AS dev

CMD ["uvicorn", "ska_sdp_global_sky_model.api.main:app", "--reload", "--host", "0.0.0.0", "--port", "80", "--app-dir", "/usr/src"]

FROM runner AS prod

ENV WORKER_COUNT=1

# Using "bash -c" for the variable expansion, and without a single string uvicorn dies
CMD ["bash", "-c", "uvicorn ska_sdp_global_sky_model.api.main:app --workers ${WORKER_COUNT} --host 0.0.0.0 --port 80"]
