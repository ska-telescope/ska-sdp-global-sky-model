FROM postgres:13.11

RUN echo "en_US.UTF-8 UTF-8"> /etc/locale.gen 
RUN locale-gen
RUN apt-get update -y && apt-get install -y wget cmake postgresql-server-dev-13 postgresql-13-pgsphere postgresql-13-q3c

COPY ./initdb-pgsphere.sh /docker-entrypoint-initdb.d/
