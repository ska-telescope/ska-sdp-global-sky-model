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

.. command-output:: gsm-delete --help
    :caption: Help for deleting a catalogue

.. code-block:: bash
   :caption: Dry-run for a deletion

   $ gsm-delete 1

   1|2026-05-05T08:39:46.611Z|INFO|...|Catalogue: 'TEST_CATALOGUE_1' (uploaded @ '2026-05-05 08:39:29.751168') (Staging:no)
   1|2026-05-05T08:39:46.616Z|INFO|...|Found 200 components
   1|2026-05-05T08:39:46.619Z|INFO|...|Found 0 components in staging table

.. code-block:: bash
   :caption: Delete a catalogue

   $ gsm-delete --delete 1

   1|2026-05-05T08:40:38.866Z|INFO|...|Catalogue: 'TEST_CATALOGUE_1' (uploaded @ '2026-05-05 08:39:29.751168') (Staging:no)
   1|2026-05-05T08:40:38.872Z|INFO|...|Found 200 components
   1|2026-05-05T08:40:38.874Z|INFO|...|Found 0 components in staging table
   1|2026-05-05T08:40:38.874Z|WARNING|...|Deleting catalogue...


Clean old Staging Catalogues
----------------------------

This command allows you to cleanup old catalogue data from the staging table.

.. danger::
    Note that the default is to perform the deletion.

.. command-output:: gsm-cleanup --help
    :caption: Help for the catalogue and components cleanup

.. code-block:: bash
    :caption: If there is nothing to delete (and ``--delete`` wasn't specified)

    $ gsm-cleanup

    1|2026-05-05T11:30:05.329Z|INFO|...|Found 0 catalogues to clean
    1|2026-05-05T11:30:05.331Z|INFO|...|Found 0 unique catalogue(s) in staging
    1|2026-05-05T11:30:05.331Z|INFO|...|Rolling back any changes


.. code-block:: bash
    :caption: When there are staging catalogues, but nothing to remove

    $ gsm-cleanup --delete
    1|2026-05-05T11:27:58.205Z|INFO|...|Found 0 catalogues to clean
    1|2026-05-05T11:27:58.206Z|INFO|...|Found 1 unique catalogue(s) in staging
    1|2026-05-05T11:27:58.206Z|INFO|...|Checking upload ID: '8ccbdcbb-5e24-44f3-974f-63ece8f35425'
    1|2026-05-05T11:27:58.207Z|INFO|...| -> Has an existing catalogue
    1|2026-05-05T11:27:58.207Z|INFO|...| -> Catalogue: 4:'TEST_CATALOGUE_1' (uploaded @ '2026-05-05 11:27:31.001711') (Staging:yes)
    1|2026-05-05T11:27:58.211Z|INFO|...| -> there are no partially transferred components (can be ignored for now)



.. code-block:: bash
    :caption: Cleaning 1 catalogue

    $ gsm-cleanup --delete
    1|2026-05-05T11:30:01.288Z|INFO|...|Found 1 catalogues to clean
    1|2026-05-05T11:30:01.288Z|INFO|...|Remove old catalogue: '8ccbdcbb-5e24-44f3-974f-63ece8f35425/TEST_CATALOGUE_1' (uploaded @ '2026-05-05 11:27:31.001711')
    1|2026-05-05T11:30:01.290Z|INFO|...|Found 0 unique catalogue(s) in staging
    1|2026-05-05T11:30:01.291Z|INFO|...|Commiting any changes

