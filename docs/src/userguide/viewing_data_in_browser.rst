Viewing LSM data in a browser
=============================

For users who would like to inspect the data visually two views have been provided.
The two table views are available through a browser interface.
These are at the `/components` and the `/local_sky_model` endpoints.

Components
----------

The components endpoint gives a table of the components that have been added to the GSM.
To access these on a local instance of the GSM navigate to ``GET /components``

The following table will be displayed:

+----+----------------+---------+----------------+-----------+-----------+-------+----------+----------+---------+----------+--------------+-------+-------+-------+
| Id | Healpix_index  | Version | Component_id   | Ra        | Dec       | I_pol | Major_ax | Minor_ax | Pos_ang | Spec_idx | Log_spec_idx | Q_pol | U_pol | V_pol |
+====+================+=========+================+===========+===========+=======+==========+==========+=========+==========+==============+=======+=======+=======+
| 1  | 72434864       | 0.0.0   | J023255-053134 | 38.230309 | -5.526247 | None  | None     | None     | None    | None     | None         | None  | None  | None  |
+----+----------------+---------+----------------+-----------+-----------+-------+----------+----------+---------+----------+--------------+-------+-------+-------+

Components
----------

To access the LSM (filtered list of components) navigate to
``GET /local_sky_model?ra={ra}&dec={dec}&fov={fov}&version={version}``

where:
.. list-table::
    :widths: 20, 50, 20, 10
    :header-rows: 1

    * - Parameter
      - Description
      - Data Type
      - Required
    * - ``ra``
      - The ra of the centre of the cone search (in degrees)
      - float
      - Yes
    * - ``dec``
      - The dec of the centre of the cone search (in degrees)
      - float
      - Yes
    * - ``fov``
      - The field of view of the cone search (in arcminutes)
      - float
      - Yes
    * - ``version``
      - The version string of the GSM to select from (not implemented)
      - string
      - Yes

This will return a filtered view of the sky components.
