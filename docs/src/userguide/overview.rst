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

The response will be a JSON object containing the provided input parameters and a placeholder value for "local_data". The actual data for the local sky model will be populated by the backend implementation.