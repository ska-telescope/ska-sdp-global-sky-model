# pylint: disable=too-few-public-methods
# pylint: disable=inconsistent-return-statements
"""Manage the file based datasets for catalogs.
"""

import logging
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)


class SourcePixel:
    """The manager for a pixel in source"""

    def __init__(self, telescope, pixel, dataset_root):
        """Source Pixel init"""
        self.pixel = pixel
        self.telescope = telescope
        self.dataset_root = Path(dataset_root)
        self.source_path = Path(self.dataset_root, self.telescope, str(self.pixel))
        self.dataset = self.read()

    def read(self):
        """Read the content of the source file."""
        if not self.source_path.is_file():
            return pl.DataFrame(
                [], schema={"name": str, "Heal_Pix_Position": pl.Int64, "Fpwide": pl.Float64}
            )
        return pl.read_csv(self.source_path)

    def add(self, source_new):
        """Add new sources to the current pixel."""
        if self.dataset.is_empty():
            self.dataset = source_new
        else:
            for col_name, _ in source_new.schema.items():
                if col_name not in self.dataset.schema.names():
                    self.dataset = self.dataset.with_columns(pl.lit(None).alias(col_name))
            self.dataset = self.dataset.update(source_new, on="name", how="full")

    def save(self):
        """Commit current sources to file."""
        self.source_path.parent.mkdir(parents=True, exist_ok=True)
        with self.source_path.open("a", encoding="utf-8"):
            pass
        self.dataset.write_csv(self.source_root)

    def all(self):
        """Get all sources in this pixel."""
        return self.dataset

    def clear(self):
        """Clear the in-memory reperesentation to the dataset."""
        del self.dataset


class PixelHandler:
    """Pixel handler class used to manage pixels."""

    def __init__(self, data_root):
        """Pixel Handler init"""
        self.index = 0
        self.pixels = []
        self.data_root = data_root

    def append(self, source_pixel):
        """Add new source to the list of sources this handler is managing"""
        self.pixels.append(source_pixel)

    def get_or_create_pixel(self, telescope, pixel):
        """Get the pixel by reference if it exists else create it."""
        for source_pixel in self.pixels:
            if source_pixel.telescope == telescope:
                if source_pixel.pixel == pixel:
                    return source_pixel
        source_pixel = SourcePixel(telescope, pixel, self.data_root)
        self.pixels.append(source_pixel)
        return source_pixel

    def save(self):
        """Commit all data to disk"""
        for pixel in self.pixels:
            pixel.save()

    def __iter__(self):
        return self

    def __next__(self):
        try:
            pixel = self.pixels[self.index]
        except IndexError as e:
            raise StopIteration from e
        self.index += 1
        return pixel

    def __len__(self):
        return len(self.pixels)

    def __getitem__(self, index):
        return self.pixels[index]


class Search:
    """Search class"""

    def __init__(self, dataset_root, search_query):
        """Search init method"""
        self.pixel_handler = PixelHandler(dataset_root)
        self.search_query = search_query

    def stream(self):
        """Stream all data that matches the search criteria"""
        telescopes = self.search_query.get("telescopes")
        if not telescopes:
            return "No telescope was found"
        pixels = self.search_query.get("healpix_pixel_rough", None)
        flux_wide = self.search_query.get("flux_wide", None)
        if not pixels.any():
            return "Empty search"
        fine_pixels = self.search_query.get(
            "hp_pixel_fine",
        )
        yield "["
        first = True
        for telescope in telescopes:
            for pixel in pixels:
                source_pixel = self.pixel_handler.get_or_create_pixel(telescope, pixel)
                if fine_pixels.any():
                    all_sources = source_pixel.all().filter(
                        pl.col("Heal_Pix_Position").is_in(fine_pixels)
                    )
                else:
                    all_sources = source_pixel.all()
                if flux_wide:
                    all_sources = all_sources.filter(pl.col("Fpwide") > flux_wide)
                if all_sources.is_empty():
                    continue
                if first:
                    first = False
                    yield f"{all_sources.write_json()[1:-1]}"
                else:
                    yield f",{all_sources.write_json()[1:-1]}"
        yield "]"


class DataStore:
    """ "Data Store handle"""

    def __init__(self, dataset_root, telescopes="*"):
        """The datastore init method."""
        self.dataset_root = dataset_root
        self.pixel_handler = PixelHandler(self.dataset_root)
        self.telescopes = self._telescope_args(telescopes)
        self._load_datasets()

    def add_source(self, source, telescope, pixel):
        """Add a source or sources to the datastore"""
        source_pixel = self.pixel_handler.get_or_create_pixel(telescope, pixel)
        source_pixel.add(source)

    def add_dataset(self, sources, telescope, pixel):
        """Add a source or sources to the datastore."""
        source_pixel = self.pixel_handler.get_or_create_pixel(telescope, pixel)
        source_pixel.add(sources)

    def save(self):
        """Commit all data to file"""
        self.pixel_handler.save()

    def query_pxiels(self, search_query):
        """Instantiate a search"""
        search_query["telescopes"] = search_query.get("telescopes", self.telescopes)
        return Search(self.dataset_root, search_query)

    def _telescope_args(self, telescopes):
        available_names = []
        root_path = Path(self.dataset_root)
        if not root_path.is_dir():
            logger.warning("Datasets directory is missing.")
            return []
        tel_available = root_path.iterdir()
        for tel_name in tel_available:
            if not tel_name.is_dir() or tel_name.name[0] == ".":
                continue
            available_names.append(tel_name.name)
        if telescopes == "*":
            return available_names
        if isinstance(telescopes, str):
            telescopes = telescopes.split(",")
        telescopes = [telescope.strip() for telescope in telescopes]
        return list(set(telescopes) & set(available_names))

    def all(self, pixel_handler=None):
        """Get all sources."""
        if not pixel_handler:
            pixel_handler = self.pixel_handler
        sources = pixel_handler[0].all()
        for i in range(1, len(pixel_handler)):
            sources_pixel = pixel_handler[i].all()
            for col_name, _ in sources_pixel.schema.items():
                if col_name not in sources.schema.names():
                    sources = sources.with_columns(pl.lit(None).alias(col_name))
            sources = sources.update(sources_pixel, on="name", how="full")
        return sources

    def _load_datasets(self):
        for telescope in self.telescopes:
            tel_root = Path(self.dataset_root, telescope)
            for pixel in tel_root.iterdir():
                source_pixel = SourcePixel(telescope, pixel.name, self.dataset_root)
                self.pixel_handler.append(source_pixel)

    def has_telescope(self, telescope):
        """Check whether a catalogue is currently present in the datastore"""
        if telescope in self.telescopes:
            return True
        return False

    def add_telescope(self, telescope):
        """Add a telescope (catalog) to the datastore."""
        if not self.has_telescope(telescope):
            Path(self.dataset_root, telescope).mkdir(parents=True, exist_ok=True)
