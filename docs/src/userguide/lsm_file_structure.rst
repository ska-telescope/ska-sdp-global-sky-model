LSM file structure
==================

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
