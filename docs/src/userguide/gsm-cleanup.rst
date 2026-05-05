Cleanup Catalogues
==================

2 methods of cleaning catalogues are currently available.

.. danger::
    These commands are currently only as CLI commands, and is dangerous. While
    there are some safe guards, there is no second confirmation.

Delete a Catalogue
------------------

This command allows you to remove a catalogue from either the published catalogues
or staging catalogues.

.. code-block:: bash
    :caption: Help for deleting a catalogue

    $ gsm-delete --help

    usage: gsm-delete [-h] [--delete] catalogue_id

    Delete a catalogue

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

This command allows you to cleanup old catalogues.

.. danger::
    Note that the default is to perform the deletion.

.. code-block:: bash
    :caption: Help for the catalogue and components cleanup

    $ gsm-cleanup --help

    usage: gsm-cleanup [-h] [--max-age MAX_AGE] [--dry-run]

    Run cleanups of the catalogues

    options:
      -h, --help         show this help message and exit
      --max-age MAX_AGE  Override the default maximum age of an upload (default: None)
      --dry-run          Perform dry run only (default: False)

    Version: 0.3.0

.. code-block:: bash
    :caption: If there is nothing to delete

    $ gsm-cleanup

    1|2026-05-05T08:48:02.606Z|INFO|MainThread|_cleanup_old_uploads|upload_manager.py#312||Found 0 catalogues to clean
    1|2026-05-05T08:48:02.607Z|INFO|MainThread|_cleanup_partial_migrations_and_orphaned_staging_components|upload_manager.py#333||Found 0 unique catalogue(s) in staging

.. code-block:: bash
    :caption: When there are staging catalogues, but nothing to remove

    $ gsm-cleanup
    1|2026-05-05T08:50:16.605Z|INFO|MainThread|_cleanup_old_uploads|upload_manager.py#312||Found 0 catalogues to clean
    1|2026-05-05T08:50:16.606Z|INFO|MainThread|_cleanup_partial_migrations_and_orphaned_staging_components|upload_manager.py#333||Found 1 unique catalogue(s) in staging
    1|2026-05-05T08:50:16.606Z|INFO|MainThread|_cleanup_partial_migrations_and_orphaned_staging_components|upload_manager.py#336||Checking upload ID: 'f5401f4e-10fb-4e7e-869e-d68f78768114'
    1|2026-05-05T08:50:16.607Z|INFO|MainThread|_cleanup_partial_migrations_and_orphaned_staging_components|upload_manager.py#350|| -> Has an existing catalogue
    1|2026-05-05T08:50:16.607Z|INFO|MainThread|_cleanup_partial_migrations_and_orphaned_staging_components|upload_manager.py#352|| -> Catalogue: 2:'TEST_CATALOGUE_1' (uploaded @ '2026-05-05 08:50:10.416411') (Staging:yes)
    1|2026-05-05T08:50:16.611Z|INFO|MainThread|_cleanup_partial_migrations_and_orphaned_staging_components|upload_manager.py#368|| -> there are no partially transferred components (can be ignored for now)


.. code-block:: bash
    :caption: Cleaning 1 catalogue

    1|2026-05-05T08:51:51.971Z|INFO|MainThread|_cleanup_old_uploads|upload_manager.py#312||Found 1 catalogues to clean
    1|2026-05-05T08:51:51.971Z|INFO|MainThread|_cleanup_old_uploads|upload_manager.py#314||Remove old catalogue: 'f5401f4e-10fb-4e7e-869e-d68f78768114/TEST_CATALOGUE_1' (uploaded @ '2026-05-05 08:50:10.416411')
    1|2026-05-05T08:51:51.973Z|INFO|MainThread|_cleanup_partial_migrations_and_orphaned_staging_components|upload_manager.py#333||Found 0 unique catalogue(s) in staging
