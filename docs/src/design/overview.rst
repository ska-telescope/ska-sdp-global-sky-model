
Overview
========

Under the hood, the Global Sky Model is using `HEALPix <https://healpix.sourceforge.io>`_ coordinates and the
data is managed by `Polars <https://pola.rs/>`_ which implements efficient DataFrames.

The whole sky has been divided into HEALPix pixels with a relatively coarse resolution of approximately one square degree.
The resolution can currently be set in `config.py <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/blob/main/src/ska_sdp_global_sky_model/configuration/config.py>`_
by editing ``NSIDE`` (coarse resolution) and ``NSIDE_PIXEL`` (fine resolution). The hope is that eventually
the resolution values will be defined by the catalogue-specific metadata file, :ref:`catalogue.yaml <metadata>`.
When a source is ingested into the file-based database, its position is mapped to one of these HEALPix pixels. This establishes
a relationship between areas of the sky, and the sources they contain.

.. code-block:: python

    class SourcePixel:
    """The manager for a pixel in source"""

        def __init__(self, telescope, pixel, dataset_root):
            """Source Pixel init"""
            self.pixel = pixel
            self.telescope = telescope
            self.dataset_root = dataset_root
            self.dataset_data = None

    class PixelHandler:
        """Pixel handler class used to manage pixels."""

        def __init__(self, dataset_root, telescope):
            """Pixel Handler init"""
            self.index = 0
            self.pixels = []
            self.telescope = telescope
            self.dataset_root = dataset_root
            self.metadata = self.get_metadata()

Each low resolution pixel is handled by a ``SourcePixel``, these are aggregated by a ``PixelHandler``, which aggregates the pixels within a catalogue configuration.
The ``dataset_data`` will point to the DataFrame containing all the sources.

.. code-block:: python

       def add(self, source_new):
            """Add new sources to the current pixel."""
            if self.dataset.is_empty():
                self.dataset = source_new
            else:
                for col_name, _ in source_new.schema.items():
                    if col_name not in self.dataset.schema.names():
                        self.dataset = self.dataset.with_columns(pl.lit(None).alias(col_name))
                self.dataset = self.dataset.update(source_new, on="name", how="full")

When a new Source is added to ``SourcePixel``, then it is joined to the DataFrame.

Local Sky Model
---------------

When performing a local sky model search, the following steps are taken:

1. Initial Selection: Rough pixels within the cone search area are identified.
#. Refinement: These rough pixels are then filtered further based on their precise pixel locations.
