.. _lsm_file:

LSM file structure
------------------

The LSM returned by the GSM query is saved to a table, formatted as a
CSV text file.
The file contains a set of columns that describe sky model component
parameters, and each row contains all the parameters for a single source
component.
The CSV data table is preceded by a short header section containing the file
metadata, where each header line starts with a hash (``#``) comment character.

The first line of the header describes the column
types which are actually present in the file.
Based on the syntax of the
`LOFAR sourcedb format <https://www.astron.nl/lofarwiki/doku.php?id=public:user_software:documentation:makesourcedb#format_string>`_,
this first line is structured as ``# (...) = format``, where the text in
parentheses contains a comma-separated list of the column names in the file.
The order of the column names here corresponds to the order in which data
values appear in each row of the subsequent CSV table.

.. note::

   The following columns follow the data models implemented in the
   `ska_sdp_datamodels package <https://gitlab.com/ska-telescope/sdp/ska-sdp-datamodels/-/blob/main/src/ska_sdp_datamodels/global_sky_model/global_sky_model.py>`_.
   The source of truth of what kind of data we store in the LSM is there.

The allowed column names, and associated types, are:

.. csv-table::
   :header: "Column name", "Type", "Description"
   :widths: 22, 12, 66

   ``component_id``, string, "Name of component."
   ``ra_deg``, float, "Right Ascension of component, in degrees."
   ``dec_deg``, float, "Declination of component, in degrees."
   ``i_pol_jy``, float, "Stokes I flux of component, in Jy."
   ``a_arcsec``, float, "Gaussian source FWHM semi-major axis, in arcsec."
   ``b_arcsec``, float, "Gaussian source FWHM semi-minor axis, in arcsec."
   ``pa_deg``, float, "Position angle of Gaussian major axis, in degrees."
   ``ref_freq_hz``, float, "Reference frequency for source fluxes, in Hz."
   ``spec_idx``, float[5], "Spectral index polynomial coefficients; a vector
   of length 5, with a CSV list of values enclosed in brackets and quotes."
   ``log_spec_idx``, boolean, "Boolean flag: If true, spectral
   indices are logarithmic, otherwise linear; see the
   `LOFAR Wiki page on LogarithmicSI <https://www.astron.nl/lofarwiki/doku.php?id=public:user_software:documentation:makesourcedb#logarithmic_spectral_index>`_."

Other lines in the header section will contain the query parameters used, and
the total number of source components in the file.
The remainder of the file contains the CSV data table.

Note that the ``spec_idx`` column contains multiple values enclosed inside a
vector, themselves also separated by commas.
Quotes will be present around the vector in order to aid CSV parsers and
ensure that values inside the vector are not split prematurely (when the main
columns are loaded).

Example LSM file
^^^^^^^^^^^^^^^^

The following shows the contents of a small LSM file as an example:

.. code-block:: text

   # (component_id,ra_deg,dec_deg,i_pol_jy,a_arcsec,b_arcsec,pa_deg,ref_freq_hz,spec_idx,log_spec_idx) = format
   # NUMBER_OF_COMPONENTS=3
   # QUERY_CENTRE_RAJ2000_DEG=123.456
   # QUERY_CENTRE_DEJ2000_DEG=45.678
   # QUERY_RADIUS_DEG=4.567
   J000011-000001,11.1,-1.234,10.0,100,10,1,1.01e+08,"[-0.7,0.01,0.123]",true
   J000022-000002,22.2,-2.345,20.0,200,20,2,1.02e+08,"[-0.7,0.02,0.123]",false
   J000033-000003,33.3,-3.456,30.0,300,30,3,1.03e+08,"[-0.7,0.03,0.123]",true
