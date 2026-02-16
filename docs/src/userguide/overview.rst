
SDP Global Sky Model Overview
=============================

Deployed via SDP -> link to SDP documentation
https://developer.skao.int/projects/ska-sdp-integration/en/latest/installation/standalone.html


How to upload data to the GSM
-----------------------------

-> link to batch_upload.rst

Example Usage:




How to request a LSM file
-------------------------
The process by which pipelines should request a Local Sky Model (LSM) is
described on the page :doc:`requesting_a_lsm`


Example Usage
^^^^^^^^^^^^^
To be written (summarise contents of :doc:`requesting_a_lsm`?)


LSM file structure
^^^^^^^^^^^^^^^^^^
The Local Sky Model (LSM) returned by the query is a subset of the data in the
Global Sky Model, which is filtered to return sky model components
(i.e. sources) that fall within a given distance of the supplied target
direction on the sky.

Data for these components in the LSM are saved to a table, formatted as a
CSV text file.
The file contains a set of columns that describe sky model component
parameters, and each row contains all the parameters for a single source
component.
The CSV data table is preceded by a short header section containing the file
metadata, where each header line starts with a hash (``#``) comment character.

The first line of the header is the most important, as it describes the column
types which are actually present in the file.
Based on the syntax of the
`LOFAR sourcedb format <https://www.astron.nl/lofarwiki/doku.php?id=public:user_software:documentation:makesourcedb#format_string>`_,
this first line is structured as ``# (...) = format``, where the text in
parentheses contains a comma-separated list of the column names in the file.
The order of the column names here corresponds to the order in which data
values appear in each row of the subsequent CSV table.
Although the file contains similar data to the files used by LOFAR, note that
the column names are not the same as the LOFAR ones.
The allowed column names, and associated types, are:

.. |br| raw:: html

   <br />

.. csv-table::
   :header: "Column name", "Type", "Description"
   :widths: 22, 12, 66

   **component_id**, string, "Name of component."
   **ra**, float, "Right Ascension of component."
   **dec**, float, "Declination of component."
   **i_pol**, float, "Stokes I flux of component, in Jy."
   **q_pol**, float, "Stokes Q flux of component, in Jy."
   **u_pol**, float, "Stokes U flux of component, in Jy."
   **v_pol**, float, "Stokes V flux of component, in Jy."
   **major_ax**, float, "Gaussian source FWHM major axis, in arcsec."
   **minor_ax**, float, "Gaussian source FWHM minor axis, in arcsec"
   **pos_ang**, float, "Position angle of Gaussian major axis, in degrees."
   **ref_freq**, float, "Reference frequency for source fluxes, in Hz."
   **spec_idx**, float[5], "Spectral index polynomial coefficients; may be a
   vector, with a CSV list of values enclosed in brackets and quotes;
   up to 5 terms may be present."
   **log_spec_idx**, boolean, "Boolean flag: If true, spectral
   indices are logarithmic, otherwise linear; see the
   `LOFAR Wiki page on LogarithmicSI <https://www.astron.nl/lofarwiki/doku.php?id=public:user_software:documentation:makesourcedb#logarithmic_spectral_index>`_.
   |br| Default true if omitted."

Other lines in the header section will contain the query parameters used, and
the total number of source components in the file.
The remainder of the file contains the CSV data table.
Note that the **spec_idx** column may contain multiple values enclosed inside a
vector, themselves also separated by commas: in this case, quotes will be
present around the vector in order to aid CSV parsers and ensure that values
inside the vector are not split prematurely (when the main columns are loaded).


Example LSM file
^^^^^^^^^^^^^^^^
The following shows the contents of a small LSM file as an example:

.. code-block:: text

   # (component_id,ra,dec,i_pol,major_ax,minor_ax,pos_ang,ref_freq,spec_idx,log_spec_idx) = format
   # NUMBER_OF_COMPONENTS: 3
   # QUERY_CENTRE_RAJ2000=123.456
   # QUERY_CENTRE_DEJ2000=45.678
   # QUERY_RADIUS_DEG=4.567
   J000011-000001,11.1,-1.234,10.0,100,10,1,1.01e+08,"[-0.7,0.01,0.123]",true
   J000022-000002,22.2,-2.345,20.0,200,20,2,1.02e+08,"[-0.7,0.02,0.123]",false
   J000033-000003,33.3,-3.456,30.0,300,30,3,1.03e+08,"[-0.7,0.03,0.123]",true


How to view data (in a browser)
-------------------------------

Example Usage:




(Move elsewhere)

Service layout
==============

Structure of the data and versions

Overview of deployments




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


How It Works:
~~~~~~~~~~~~~

Under the hood, the Global Sky Model is using Q3C (Quad Tree Cube), an extension to PostgreSQL, that adds a sky-indexing scheme along with a SQL interface for performing cone searches.

The schema stores all component information in a single SkyComponent table. Each row represents a celestial component with its associated properties and measurements, including an associated HEALPix position for efficient spatial indexing.

The SkyComponent model where most fields are dynamically generated from the ``SkyComponent`` dataclass in the ``ska-sdp-datamodels`` package, ensuring automatic synchronization with upstream data model changes:

This approach allows the schema to automatically adapt when new fields are added to the upstream
dataclass, while maintaining database-specific concerns like spatial indexing.

Upon requesting a local sky model, a cone search is carried out with the given parameters, using the `q3c_radial_query` provided by the Q3C extension. Sky components meeting the criteria of the given parameters are returned as the Local Sky Model.

.. code-block:: javascript

    {
      "components": {
        "<component_id>": {
          "ra": <number>,
          "dec": <number>,
          "i_pol": <number>,
          "major_ax": <number>,
          "minor_ax": <number>,
          "pos_ang": <number>,
          "spec_idx": [<number>, ...],
          "q_pol": <number>,
          "u_pol": <number>,
          "v_pol": <number>
        }
      }
    }
