# Using multi stage build to update the requirements.txt from the project.toml.
FROM python:3.10-slim as starter
WORKDIR /app
RUN pip install --no-cache-dir poetry
COPY pyproject.toml poetry.lock ./
COPY src/gsm-demo/kubernetes/api/app /app/src/gsm-demo/api/
RUN poetry export -o requirements.txt && poetry build


FROM python:3.10-slim as builder
# dont write pyc files
ENV PYTHONDONTWRITEBYTECODE 1
# dont buffer to stdout/stderr
ENV PYTHONUNBUFFERED 1


WORKDIR /

COPY --from=starter /app/requirements.txt /app/dist/*.whl ./
RUN apt-get update -y && apt-get install -y libpq-dev gcc
RUN pip install --upgrade pip
RUN pip install --no-cache-dir --no-compile -r requirements.txt && \
    pip install --no-cache-dir --no-compile ./*.whl && \
    rm -rf /app/*


FROM builder as dev

WORKDIR /usr/src/gsm-demo

COPY src/gsm-demo/kubernetes/api/app/ .

EXPOSE 80

CMD ["uvicorn", "main:app", "--reload", "--host", "0.0.0.0", "--port", "80"]
