API Usage Guide
===============

To deploy the API, see the :ref:`deployment`.

Once up and running there are several API endpoints you can query to retrieve information:

    - ``/ping``
    - ``/local_sky_model``
    - ``/docs``

Without visiting an endpoint, the base api host/port address will just display:

.. code-block:: bash

    {"detail":"Not Found"}

ping
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


local_sky_model
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

This will return a list of sources with various information, for example:

.. code-block::

    [{
        "Heal_Pix_Position":156722,"Fint122":0.069661,"Fintwide":0.074672,"Fint212":0.113818,"Fint115":-0.014681,"Fint092":0.062107,"Fint227":0.095824,"Fint174":0.030177,"Fint189":0.08885,"DEJ2000":-50.274509,"Fint204":0.09043,"Fint084":-0.017805,"name":"J080350-501628","Fint197":0.03983,"Fint158":0.064523,"Fint166":0.067736,"RAJ2000":120.961189,"Fint220":0.061598,"Fint143":0.05896,"Fint130":0.138036,"Fint099":0.058149,"Fint181":0.029213,"Fint107":0.046523,"Fint076":0.004217,"Fint151":0.093087
     }, ...]


Obtain data for a single source, via curl:

.. code-block:: bash

    curl -X GET http://localhost:8000/local_sky_model \
    -H 'Content-Type: application/json' \
    -d '{
        "ra": 120.0,
        "dec": -50.0,
        "flux_wide": 1.23,
        "telescope": "Murchison Widefield Array",
        "fov": 2.0
    }'

This last example request retrieves a local sky model for an observation with the following parameters:

* Right Ascension (RA): 120 degrees
* Declination (DEC): -50 degrees
* Wide-field flux: 1.23 Jy
* Telescope: Murchison Widefield Array
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
The HEALPix pixel id (shown as Heal_Pix_Position here) corresponds to specific regions of the sky and are explained more in the :doc:`overview <../design/overview>`.

.. code-block:: javascript

    [{"Heal_Pix_Position":156685},{"Heal_Pix_Position":156717}]


docs
----

Interactive documentation

For detailed documentation of the API, see the FastAPI Swagger UI documentation.
This interactive API documentation can be accessed at ``http://127.0.0.1:8000/docs`` when running the application locally,
or ``https://<domain>/<namespace>/global-sky-model/docs`` when deployed behind an ingress.
Remember to replace ``<domain>`` and ``<namespace>`` with the appropriate values.