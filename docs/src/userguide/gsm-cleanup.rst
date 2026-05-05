Cleanup Catalogues
==================

Two methods of cleaning catalogues are currently available: deleting a catalogue based on its ID, or deleting all catalogues older than a specified age.

.. danger::
    Currently, these commands are provided as CLI commands only, and their use
    may cause unwanted data loss. While there are some safeguards, there is no second confirmation.

Delete a Catalogue
------------------

This command allows you to remove a catalogue from either the published catalogues
or staging catalogues.

.. code-block:: bash
    :caption: Help for deleting a catalogue

    $ gsm-delete --help

    usage: gsm-delete [-h] [--delete] catalogue_id

    Delete a catalogue, this can delete a staging or released catalogue.

    positional arguments:
      catalogue_id  The numerical ID of a catalogue

    options:
      -h, --help    show this help message and exit
      --delete      Do the actual delete (default: False)

    Version: 0.3.0

.. code-block:: bash
   :caption: Dry-run for a deletion

   $ gsm-delete 1

   1|2026-05-05T08:39:46.611Z|INFO|MainThread|main|catalogue_delete.py#53||Catalogue: 'TEST_CATALOGUE_1' (uploaded @ '2026-05-05 08:39:29.751168') (Staging:no)
   1|2026-05-05T08:39:46.616Z|INFO|MainThread|main|catalogue_delete.py#63||Found 200 components
   1|2026-05-05T08:39:46.619Z|INFO|MainThread|main|catalogue_delete.py#69||Found 0 components in staging table

.. code-block:: bash
   :caption: Delete a catalogue

   $ gsm-delete --delete 1

   1|2026-05-05T08:40:38.866Z|INFO|MainThread|main|catalogue_delete.py#53||Catalogue: 'TEST_CATALOGUE_1' (uploaded @ '2026-05-05 08:39:29.751168') (Staging:no)
   1|2026-05-05T08:40:38.872Z|INFO|MainThread|main|catalogue_delete.py#63||Found 200 components
   1|2026-05-05T08:40:38.874Z|INFO|MainThread|main|catalogue_delete.py#69||Found 0 components in staging table
   1|2026-05-05T08:40:38.874Z|WARNING|MainThread|main|catalogue_delete.py#72||Deleting catalogue...


Clean old Staging Catalogues
----------------------------

This command allows you to cleanup old catalogue data from the staging table.

.. danger::
    Note that the default is to perform the deletion.

.. code-block:: bash
    :caption: Help for the catalogue and components cleanup

    $ gsm-cleanup --help

    usage: usage: gsm-cleanup [-h] [--max-age MAX_AGE] [--delete]

    Run cleanups of the catalogues

    options:
      -h, --help         show this help message and exit
      --max-age MAX_AGE  Override the default maximum age of an upload (default: 168)
      --delete           Commit the deletion (default: False)

    Version: 0.3.0

.. code-block:: bash
    :caption: If there is nothing to delete (and ``--delete`` wasn't specified)

    $ gsm-cleanup

    1|2026-05-05T11:30:05.329Z|INFO|MainThread|_cleanup_old_uploads|upload_manager.py#318||Found 0 catalogues to clean
    1|2026-05-05T11:30:05.331Z|INFO|MainThread|_cleanup_partial_migrations_and_orphaned_staging_components|upload_manager.py#339||Found 0 unique catalogue(s) in staging
    1|2026-05-05T11:30:05.331Z|INFO|MainThread|run_db_cleanup|upload_manager.py#298||Rolling back any changes


.. code-block:: bash
    :caption: When there are staging catalogues, but nothing to remove

    $ gsm-cleanup --delete
    1|2026-05-05T11:27:58.205Z|INFO|MainThread|_cleanup_old_uploads|upload_manager.py#313||Found 0 catalogues to clean
    1|2026-05-05T11:27:58.206Z|INFO|MainThread|_cleanup_partial_migrations_and_orphaned_staging_components|upload_manager.py#334||Found 1 unique catalogue(s) in staging
    1|2026-05-05T11:27:58.206Z|INFO|MainThread|_cleanup_partial_migrations_and_orphaned_staging_components|upload_manager.py#337||Checking upload ID: '8ccbdcbb-5e24-44f3-974f-63ece8f35425'
    1|2026-05-05T11:27:58.207Z|INFO|MainThread|_cleanup_partial_migrations_and_orphaned_staging_components|upload_manager.py#351|| -> Has an existing catalogue
    1|2026-05-05T11:27:58.207Z|INFO|MainThread|_cleanup_partial_migrations_and_orphaned_staging_components|upload_manager.py#353|| -> Catalogue: 4:'TEST_CATALOGUE_1' (uploaded @ '2026-05-05 11:27:31.001711') (Staging:yes)
    1|2026-05-05T11:27:58.211Z|INFO|MainThread|_cleanup_partial_migrations_and_orphaned_staging_components|upload_manager.py#369|| -> there are no partially transferred components (can be ignored for now)



.. code-block:: bash
    :caption: Cleaning 1 catalogue

    $ gsm-cleanup --delete
    1|2026-05-05T11:30:01.288Z|INFO|MainThread|_cleanup_old_uploads|upload_manager.py#318||Found 1 catalogues to clean
    1|2026-05-05T11:30:01.288Z|INFO|MainThread|_cleanup_old_uploads|upload_manager.py#320||Remove old catalogue: '8ccbdcbb-5e24-44f3-974f-63ece8f35425/TEST_CATALOGUE_1' (uploaded @ '2026-05-05 11:27:31.001711')
    1|2026-05-05T11:30:01.290Z|INFO|MainThread|_cleanup_partial_migrations_and_orphaned_staging_components|upload_manager.py#339||Found 0 unique catalogue(s) in staging
    1|2026-05-05T11:30:01.291Z|INFO|MainThread|run_db_cleanup|upload_manager.py#295||Commiting any changes

