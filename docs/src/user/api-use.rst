API Usage Guide
===============

To deploy the API, see the :doc:`deployment docs <../developer/development>` under the developer guide.

Once up and running there are several API endpoints you can query to retrieve information:

    - ``/ping``
    - ``/sources``
    - ``/local_sky_model``
    - ``/docs``

Without visiting an endpoint, the base api host/port address will just display:

.. code-block:: bash

    {"detail":"Not Found"}

Ping
----

This endpoint returns a JSON object reflecting the liveness of the API.

Example use:

.. code-block:: bash

    GET /ping
    curl http://127.0.0.1:8000/ping


If the API is up and running, this should yield the result:

.. code-block:: javascript

    {
        "ping": "live"
    }


Sources
-------

This API endpoint retrieves a complete list of sources by reading the csv files in the datastore under ``datasets/``.

.. warning::

    This could take a long time or be unsuccessful (if done locally) given the large amount
    of memory required to return all the source information. Note recommended for local work.

Example use:

.. code-block:: bash

    GET /sources
    curl http://127.0.0.1:8000/sources


This returns a JSON object representing the complete list of sources, e.g:


.. code-block:: javascript

    {
        "source 1": {"ra": 123, "dec": -12.3},
        "source 2": {"ra": 321, "dec": 32.1}
    }


Local Sky Model
---------------

This API endpoint retrieves a subset of the global sky model, filtered for a specified celestial observation.
Filtered by right ascension (ra), declination (dec), telescope name and field of view (fov), the api returns sources within that region.
See the table below for more information on the parameters.

Example uses:

Get query:

.. code-block:: bash

    GET /local_sky_model?ra=120;130&dec=-50;-40&telescope=Murchison%20Widefield%20Array&fov=2

Directly in the url:

.. code-block:: bash

    curl http://127.0.0.1:8000/local_sky_model?ra=120;130&dec=-50;-40&telescope=Murchison%20Widefield%20Array&fov=2

Via curl:

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

This last example request retrieves a local sky model for an observation with the following parameters:

* Right Ascension (RA): 123.456 degrees
* Declination (DEC): -56.789 degrees
* Wide-field flux: 1.23 Jy
* Telescope: HST
* Field of view: 2.0 degrees


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


This endpoint returns a list of dictionaries of HEALPix pixels, plus what is configured in the ``catalogue.yaml``.
The HEALPix pixel id (shown as Heal_Pix_Position here) corresponds to specific regions of the sky and are explained more in the :doc:`overview <../user/overview>`.

.. code-block:: javascript

    [{"Heal_Pix_Position":156685},{"Heal_Pix_Position":156717}]


Interactive Documentation
-------------------------
For detailed documentation of the API, see the FastAPI Swagger UI documentation.
This interactive API documentation can be accessed at http://127.0.0.1:8000/docs when running the application locally,
or https://<domain>/<namespace>/global-sky-model/docs when deployed behind an ingress.
