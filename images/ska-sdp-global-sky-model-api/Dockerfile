# Using multi stage build to update the requirements.txt from the project.toml.
FROM ska-sdp-python:0.1.0 AS starter
WORKDIR /app
RUN pip install --no-cache-dir poetry
COPY pyproject.toml poetry.lock ./
COPY src/ska_sdp_global_sky_model/api/app /app/src/ska_sdp_global_sky_model/api/
RUN poetry export -o requirements.txt && poetry build


FROM ska-sdp-python:0.1.0 AS builder
# dont write pyc files
ENV PYTHONDONTWRITEBYTECODE=1
# dont buffer to stdout/stderr
ENV PYTHONUNBUFFERED=1


WORKDIR /

COPY --from=starter /app/requirements.txt /app/dist/*.whl ./
RUN apt-get update -y && apt-get install -y libpq-dev gcc
RUN pip install --upgrade pip
RUN pip install --no-cache-dir --no-compile -r requirements.txt && \
    pip install --no-cache-dir --no-compile ./*.whl && \
    rm -rf /app/*


FROM builder AS dev

WORKDIR /usr/src/ska_sdp_global_sky_model

COPY src/ska_sdp_global_sky_model/ .

EXPOSE 80

CMD ["uvicorn", "ska_sdp_global_sky_model.api.app.main:app", "--reload", "--host", "0.0.0.0", "--port", "80", "--app-dir", "/usr/src"]