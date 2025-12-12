FROM postgres:14

# Install build dependencies
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        build-essential \
        wget \
        ca-certificates \
        libcfitsio-dev \
        libhealpix-cxx-dev \
        libxml2-dev \
        postgresql-server-dev-14; \
    rm -rf /var/lib/apt/lists/*

# Build H3C PostgreSQL extension
RUN set -eux; \
    cd /tmp; \
    wget --no-check-certificate https://cdsarc.u-strasbg.fr/ftp/sw/h3c.tar.gz; \
    tar xzf h3c.tar.gz; \
    cd cds_h3c_*; \
    ./configure \
        --with-healpix64=yes \
        --with-healpix-include=/usr/include/healpix_cxx \
        --with-healpix-lib=/usr/lib/x86_64-linux-gnu \
        --with-fitsio-include=/usr/include \
        --with-fitsio-lib=/usr/lib/x86_64-linux-gnu \
        --enable-shared; \
    # Fix the Makefile to use system shared libs instead of missing static local libs
    sed -i 's|lib/libcfitsio.a|-lcfitsio|g' Makefile; \
    sed -i 's|lib/libhealpix_cxx.a|-lhealpix_cxx|g' Makefile; \
    # libcxxsupport is internal to healpix-cxx on Ubuntu, so remove the explicit link to it
    sed -i 's|lib/libcxxsupport.a||g' Makefile; \
    make -j"$(nproc)"; \
    make install; \
    cd /; \
    rm -rf /tmp/cds_h3c_*


COPY db/*.sql /docker-entrypoint-initdb.d/

EXPOSE 5432

CMD ["postgres"]