CHANGELOG
=========

Development
-----------

- [Updated] Complete refactoring for simplified ska-sdp-datamodels v1.0.2+ schema.
  **Breaking Changes:**
  - Removed separate ``Telescope``, ``Band``, ``WideBandData``, and ``NarrowBandData`` tables
  - Database schema now consists of ``Source`` and ``GlobalSkyModelMetadata`` tables only
  - All measurements and properties stored directly on source records
  - List-type fields (e.g., ``spec_idx``) stored as JSON columns
  - Field names now match data model exactly: ``ra``, ``dec``, ``i_pol``, ``healpix_index`` (no longer using ``RAJ2000``, ``DECJ2000``, etc.)
  - Schema generation refactored with separated configuration in ``scripts/db_config.py``
  [Added] Use ``make generate-schema`` to regenerate the schema after datamodel updates.
  (`MR[TBD] <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/[TBD]>`__)
- Added etcd service to docker-compose setup and updated documentation
  (`MR60 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/60>`__)
- [Removed]Remove the dependencie on a running PostgreSQL instance for tests
  (`MR59 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/59>`__)
- [Added] Re-instated postgres backend storage starting from version 0.1.4.
  (`MR49 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/49>`__)
- [Added] Watcher for creating local sky models from Flow entries
  (`MR51 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/51>`__)
- [Removed] Helm chart
  (`MR58 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/58>`__)
- [Modified] Logger ENV variable has changed behaviour, and remove links to Redis
  (`MR58 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/58>`__)

Note
----
The 0.2.0 release has been shelved and should not be used as a reference to main. The 'new' main
is based on version 0.1.4. The development changes post 0.2.0 (under 'Unreleased') were not
released and can also be disregarded.


Unreleased
----------

- Reorganized and updated the documentation
  (`MR44 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/44>`__)
- Fix RTD by upgrading to poetry 2
  (`MR42 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/42>`__)
- Update documentation and Make commands for datasets
  (`MR41 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/41>`__)


0.2.0 (former)
--------------

- Prepare deployment and release new version
  (`MR40 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/40>`__)
- Update uvicorn command line
  (`MR39 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/39>`__)
- Fix Deployment, and small fixes
  (`MR38 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/38>`__)
- Update the Chart and documentation
  (`MR37 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/37>`__)
- Initial Updates for restructure
  (`MR36 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/36>`__)
- Update readme
  (`MR35 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/35>`__)
- Only return default columns by default
  (`MR34 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/34>`__)
- Added free form search to local sky model
  (`MR32 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/32>`__)
- Added file base (polars) backend, sped up MWA (vizier) ingest and RACS (file based) ingest.
  (`MR31 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/31>`__)
- Allow deployments from ci pipelines to techops cluster
  (`MR30 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/30>`__)
- Reimplement precise cone search
  (`MR29 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/29>`__)


0.1.4
-----

- Bumped version to 0.1.4
  (`MR28 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/28>`__)
- Update docs to reflect changes to GSM
  (`MR27 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/27>`__)
- Increase code coverage
  (`MR26 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/26>`__)
- Use the ska-sdp-python image
  (`MR25 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/25>`__)
- Modify schema to include spectral index, variability, curvature, polarisation
  (`MR24 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/24>`__)
- Added flux minimum to query
  (`MR23 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/23>`__)


0.1.3
-----

- Changes to allow ingest of an arbitrary catalog for testing.
  (`MR22 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/22>`__)
- Improve Local Sky Model creation speed
  (`MR21 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/21>`__)
- Make dev db available
  (`MR20 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/20>`__)
- Quick fix for key error
  (`MR19 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/19>`__)
- Speed up cone search
  (`MR18 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/18>`__)
- Fixed up dockerfile, ensure DB connection is up and running before finishing app startup
  (`MR17 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/17>`__)
- Make the ingest generic to be able to process both vizier and file based catalog
  (`MR16 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/16>`__)
- Implement Cone Search
  (`MR15 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/15>`__)
- PG-Sphere extension removal
  (`MR14 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/14>`__)
- Addition of some logs
  (`MR13 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/13>`__)
- Added AOI model for DB based selects
  (`MR12 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/12>`__)
- Add probes, update volumes, update docker compose
  (`MR11 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/11>`__)
- Ingest GLEAM catalog, updated models, added test endpoints
  (`MR10 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/10>`__)
- Use sqlalchemy for Data models
  (`MR7 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/7>`__)
- Add basic documentation
  (`MR5 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/5>`__)
- Documentation structure updates
  (`MR4 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/4>`__)
- Update dependencies to fix failing docs build
  (`MR3 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/3>`__)

0.1.1
-----
- Bump release
  (`MR2 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/2>`__)

0.1.0
-----

- Helm chart for Postgresql backend with pgsphere extension and a fastAPI
  (`MR1 <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/merge_requests/1>`__)
