SDP Global Sky Model's Overview
===============================


Automatic API Documentation
---------------------------
For detailed documentation of the API, see the FastAPI Swagger UI documentation. This interactive API documentation can be accessed at http://127.0.0.1:8000/docs when running the application locally or https://<domain>/<namespace>/global-sky-model/docs when deployed behind an ingress.

Basic Usage
-----------

Ping
~~~~

This API endpoint is provided to ensure that the API is up and running.

URI:
~~~~

.. code-block:: bash

    GET /ping


Response:
~~~~~~~~~

The endpoint returns a JSON object reflecting the liveness of the API.


.. code-block:: javascript

    {
        "ping": "live"
    }


Get Sources API Endpoint
~~~~~~~~~~~~~~~~~~~~~~~~

This API endpoint retrieves a complete list of sources by reading the csv files in the datastore under `datasets/`. This could take a long time or be unsuccessful locally given the large amount of memory required to return all the source information.

URI:
~~~~

.. code-block:: bash

    GET /sources


Response:
~~~~~~~~~

The endpoint returns a JSON object representing the complete list of sources.


.. code-block:: javascript

    {
        "source 1": {"ra": 123, "dec": -12.3},
        "source 2": {"ra": 321, "dec": 32.1}
    }


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
    * - ``telescope``
      - Name of the telescope being used for the observation.
      - String
      - Yes
    * - ``fov``
      - Field of view of the telescope in degrees.
      - Float
      - Yes
    * - ``advanced_search_n``
      - This parameter can take a value such as "flux_wide", which represents the wide-field flux of the observation in Jansky (Jy). If specified, it will filter results to return only sources with a flux_wide value higher than the given threshold.
      - Float
      - Yes

Response:
~~~~~~~~~

The endpoint returns a JSON object representing the local sky model.


.. code-block:: javascript

    {
        "ra": (float),  // Right ascension provided as input.
        "dec": (float),  // Declination provided as input.
        "telescope": (string),  // Telescope name provided as input.
        "fov": (float),  // Field of view provided as input.
        "local_data": (string),  // Placeholder for data specific to the local sky model.
        "advanced_search_1": (float),  // Advanced search criteria 1.
        ...
        "advanced_search_n": (float), // Advanced search criteria n.
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

Under the hood, the Global Sky Model is using HEALPix coordinates and the data is managed by Polars which implements efficient DataFrames.

The whole sky has been divided into HEALPix pixels with a relatively coarse resolution of approximately one square degree.
The resolution can be set in the conf.py. #TODO: this resolution should be set in the catalogue config.
When a source is ingested into the postgres database, its position is mapped to one of these HEALPix pixels. This establishes
a relationship between areas of the sky, and the sources they contain.

.. code-block:: python

    class SourcePixel:
    """The manager for a pixel in source"""

        def __init__(self, telescope, pixel, dataset_root):
            """Source Pixel init"""
            self.pixel = pixel
            self.telescope = telescope
            self.dataset_root = dataset_root
            self.dataset_data = None

    class PixelHandler:
        """Pixel handler class used to manage pixels."""

        def __init__(self, dataset_root, telescope):
            """Pixel Handler init"""
            self.index = 0
            self.pixels = []
            self.telescope = telescope
            self.dataset_root = dataset_root
            self.metadata = self.get_metadata()

Each low resolution pixel is handled by a SourcePixel, these are aggregated by a PixelHandler, which aggregates the pixels within a catalogue configuration.
The dataset_data will point to the DataFrame containing all the sources.

.. code-block:: python

       def add(self, source_new):
            """Add new sources to the current pixel."""
            if self.dataset.is_empty():
                self.dataset = source_new
            else:
                for col_name, _ in source_new.schema.items():
                    if col_name not in self.dataset.schema.names():
                        self.dataset = self.dataset.with_columns(pl.lit(None).alias(col_name))
                self.dataset = self.dataset.update(source_new, on="name", how="full")

When a new Source is added to the low resolution pixel, then it is joined to the DataFrame.

Local Sky Model:
~~~~~~~~~~~~~~~~

When performing a local sky model search, the following steps are taken:
Initial Selection: Rough pixels within the cone search area are identified.
Refinement: These rough pixels are then filtered further based on their precise pixel locations.
