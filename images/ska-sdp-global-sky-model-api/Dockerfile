FROM artefact.skao.int/ska-build-python:0.3.1 AS build
WORKDIR /app
ENV POETRY_VIRTUALENVS_CREATE=false
COPY pyproject.toml poetry.lock ./
COPY README.rst LICENSE ./
RUN poetry install --no-root --only main

COPY src/ src/
RUN pip install --no-compile --no-dependencies .

FROM artefact.skao.int/ska-python:0.2.3 AS builder
# dont write pyc files
ENV PYTHONDONTWRITEBYTECODE=1
# dont buffer to stdout/stderr
ENV PYTHONUNBUFFERED=1


WORKDIR /

COPY --from=build /usr/local/ /usr/local/

CMD ["uvicorn", "ska_sdp_global_sky_model.api.app.main:app", "--host", "0.0.0.0", "--port", "80", "--app-dir", "/usr/src"]