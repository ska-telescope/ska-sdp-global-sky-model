SDP Global Sky Model's Overview
===============================


Automatic API Documentation
---------------------------
For detailed documentation of the API, see the FastAPI Swagger UI documentation. This interactive API documentation can be accessed at http://127.0.0.1:8000/docs when running the application locally or https://<domain>/<namespace>/global-sky-model/docs when deployed behind an ingress.

Basic Usage
-----------

Get Local Sky Model API Endpoint
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This API endpoint retrieves a local sky model from a global sky model for a specified celestial observation.

URI:
~~~~

.. code-block:: bash

    GET /local_sky_model


Request Parameters:
~~~~~~~~~~~~~~~~~~~

.. list-table::
    :widths: 20, 50, 20, 10
    :header-rows: 1

    * - Parameter
      - Description
      - Data Type
      - Required
    * - ``ra``
      - Right ascension of the observation point in degrees.
      - Float
      - Yes
    * - ``dec``
      - Declination of the observation point in degrees.
      - Float
      - Yes
    * - ``flux_wide``
      - Wide-field flux of the observation in Jy (Jansky).
      - Float
      - Yes
    * - ``telescope``
      - Name of the telescope being used for the observation.
      - String
      - Yes
    * - ``fov``
      - Field of view of the telescope in arcminutes.
      - Float
      - Yes

Response:
~~~~~~~~~

The endpoint returns a JSON object representing the local sky model.


.. code-block:: javascript

    {
    "ra": (float),  // Right ascension provided as input.
    "dec": (float),  // Declination provided as input.
    "flux_wide": (float),  // Wide-field flux provided as input.
    "telescope": (string),  // Telescope name provided as input.
    "fov": (float),  // Field of view provided as input.
    "local_data": (string),  // Placeholder for data specific to the local sky model. 
                                // This data will be populated by the backend.
    }


Example Usage:
~~~~~~~~~~~~~~

.. code-block:: bash

    curl -X GET http://localhost:8000/local_sky_model \
    -H 'Content-Type: application/json' \
    -d '{
        "ra": 123.456,
        "dec": -56.789,
        "flux_wide": 1.23,
        "telescope": "HST",
        "fov": 2.0
    }'

This example request retrieves a local sky model for an observation with the following parameters:

* Right Ascension (RA): 123.456 degrees
* Declination (DEC): -56.789 degrees
* Wide-field flux: 1.23 Jy
* Telescope: HST
* Field of view: 2.0 degrees

The response will be a JSON object containing the provided input parameters and a placeholder value for "local_data". 
The actual data for the local sky model will be populated by the backend implementation.


How It Works:
~~~~~~~~~~~~~

Under the hood, the Global Sky Model is using HEALPix Alchemy, an extension to SQL Alchemy that adds region and image arithmetic 
to PostgreSQL databases.

The whole sky has been divided into HEALPix pixels with a relatively coarse resolution of approximately one square degree. 
When a source is ingested into the postgres database, its position is mapped to one of these HEALPix pixels. This establishes 
a relationship between areas of the sky, and the sources they contain.

.. code-block:: python
    class WholeSky(Base):
      """
      Represents a collection of SkyTiles making up the whole sky.
      """

      __table_args__ = {"schema": DB_SCHEMA}

      id = Column(Integer, primary_key=True, autoincrement=False)
      tiles = relationship(
          lambda: SkyTile, order_by="SkyTile.id", cascade="all, delete, delete-orphan"
      )

    class SkyTile(Base):
        """
        A HEALPix tile that is a component of the whole sky.
        """

        __table_args__ = {"schema": DB_SCHEMA}

        id = Column(ForeignKey(WholeSky.id, ondelete="CASCADE"), primary_key=True)
        hpx = Column(Tile, index=True)
        pk = Column(Integer, primary_key=True, autoincrement=False, unique=True)
        sources = relationship("Source", back_populates="tile")

Each row in the Source table, is a source in our catalog, whose position is represented by a HEALPix point:

.. code-block:: python

    class Source(Base):

      id = mapped_column(Integer, primary_key=True, index=True, autoincrement=True)
      Heal_Pix_Position = Column(Point, index=True, nullable=False)
      tile_id = Column(Integer, ForeignKey(SkyTile.pk), nullable=False)
      tile = relationship(SkyTile, back_populates="sources")

Upon requesting a local sky model, a cone search is carried out with the given parameters. The cone is constructed from the 
HEALPix pixels that overlap (both fully and partially) with the defined area of interest. All the sources that satisfy the criteria
set out in the request for a local sky model are returned by the following query:

.. code-block:: python

    query = (
        db.query(SkyTile, Source, narrowband_data, wideband_data)
        .filter(SkyTile.pk.in_(tiles_int))
        .filter(wideband_data.Flux_Wide > flux_wide)
        .join(SkyTile.sources)
        .outerjoin(narrowband_data, Source.id == narrowband_data.source)
        .outerjoin(wideband_data, Source.id == wideband_data.source)
        .all()
    )