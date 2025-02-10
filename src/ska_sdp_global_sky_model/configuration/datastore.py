"""Manage the file based datasets for catalogs."""

import logging
from pathlib import Path

import polars as pl
import yaml

logger = logging.getLogger(__name__)


class SourcePixel:
    """The manager for a pixel in source"""

    def __init__(self, telescope, pixel, dataset_root):
        """Source Pixel init"""
        self.pixel = pixel
        self.telescope = telescope
        self.dataset_root = dataset_root
        self.dataset_data = None

    @property
    def dataset(self):
        """Avoid loading the dataset until it is needed."""
        if self.dataset_data is None:
            self.dataset_data = self.read()
        return self.dataset_data

    @dataset.setter
    def dataset(self, value):
        self.dataset_data = value

    @property
    def source_root(self):
        """Get the path to the source file"""
        return Path(self.dataset_root, self.telescope, str(self.pixel))

    def read(self):
        """Read the content of the source file."""
        if not self.source_root.is_file():
            return pl.DataFrame([], schema={"name": str, "Heal_Pix_Position": pl.Int64})

        logger.info("Reading existing dataset: %s", self.source_root)
        return pl.read_csv(self.source_root)

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
        logger.info("Writing: %s", self.source_root)
        self.source_root.parent.mkdir(parents=True, exist_ok=True)
        with self.source_root.open("w", encoding="utf-8") as file:
            self.dataset.write_csv(file)

    def all(self, defaults: list[str] | None = None):
        """Get all sources in this pixel."""
        if defaults is None:
            return self.dataset
        defaults = list(set(defaults) & set(self.dataset.schema.keys()))
        return self.dataset.select(["Heal_Pix_Position"] + defaults)

    def clear(self):
        """Clear the in-memory dataset."""
        del self.dataset_data


class PixelHandler:
    """Pixel handler class used to manage pixels."""

    def __init__(self, dataset_root, telescope):
        """Pixel Handler init"""
        self.index = 0
        self.pixels = []
        self.telescope = telescope
        self.dataset_root = dataset_root
        self.metadata = self.get_metadata()

    def metadata_file(self):
        """get the path to the metadata file"""
        return Path(self.dataset_root, self.telescope, "catalogue.yaml")

    def defaults(self):
        """get the default catalgue attributes"""
        if "default-attributes" in self.metadata["config"]:
            return self.metadata["config"]["default-attributes"]
        return self.metadata["config"]["attributes"]

    def get_metadata(self):
        """get the catalogue's metadata, else create an empty metadata file"""
        if not self.metadata_file().is_file():
            return {"config": {"attributes": []}}
        with self.metadata_file().open("r", encoding="utf-8") as fd:
            return yaml.safe_load(fd.read())

    def has_attribute(self, key):
        """verify that a specific attribute exists within the metadata"""
        if key in self.metadata["config"]["attributes"]:
            return True
        return False

    def append(self, source_pixel):
        """Add new source to the list of sources this handler is managing"""
        self.pixels.append(source_pixel)

    def get_or_create_pixel(self, telescope, pixel):
        """Get the pixel by reference if it exists else create it."""
        for source_pixel in self.pixels:
            if source_pixel.telescope == telescope and source_pixel.pixel == pixel:
                return source_pixel
        source_pixel = SourcePixel(telescope, pixel, self.dataset_root)
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
        self.dataset_root = dataset_root
        self.search_query = search_query
        self.telescopes = self.get_telescopes()
        self.validate()

    def validate(self):
        """Validate that the search criteria, remove unknown search terms"""
        invalid_keys = []
        for key in self.search_query["advanced_search"].keys():
            key_available = False
            for pixel_handler in self.telescopes.values():
                if pixel_handler.has_attribute(key):
                    key_available = True
            if not key_available:
                invalid_keys.append(key)
        for key in invalid_keys:
            logger.info("Removing the following search criteria: %s", invalid_keys)
            self.search_query["advanced_search"].pop(key)

    def get_telescopes(self):
        """Validate the search query."""
        if not self.search_query.get("telescopes", None):
            self.search_query["telescopes"] = "*"
        elif isinstance(self.search_query.get("telescopes"), str):
            self.search_query["telescopes"] = [
                t.strip() for t in self.search_query["telescopes"].split(",")
            ]
        telescopes = self.search_query["telescopes"]
        available_telescopes = []
        root_path = Path(self.dataset_root)
        if not root_path.is_dir():
            logger.warning("Datasets directory is missing.")
            return []
        tel_available = root_path.iterdir()
        for tel_name in tel_available:
            if not tel_name.is_dir() or tel_name.name[0] == ".":
                continue
            available_telescopes.append(tel_name.name)
        if not available_telescopes:
            logger.warning("No matching catalog found")
            raise NameError
        if not telescopes == "*":
            available_telescopes = list(set(telescopes) & set(available_telescopes))
        return {
            telescope: PixelHandler(self.dataset_root, telescope)
            for telescope in available_telescopes
        }

    def filter(self, data_set):
        """Remove items that are less than a given criteria"""
        for search_criteria, minimum in self.search_query["advanced_search"].items():
            try:
                minimum = float(minimum)
            except ValueError:
                logger.info("Could not evaluate %s %s", search_criteria, minimum)
                continue
            if search_criteria in data_set.schema.names():
                data_set = data_set.filter(pl.col(search_criteria) > minimum)
        return data_set

    def stream(self):
        """Stream all data that matches the search criteria"""
        pixels = self.search_query.get("healpix_pixel_rough")
        if not pixels.any():
            yield "Empty search"
            return
        fine_pixels = self.search_query.get(
            "hp_pixel_fine",
        )
        yield "["
        first = True
        for telescope, pixel_handler in self.telescopes.items():
            defaults = pixel_handler.defaults()
            for pixel in pixels:
                source_pixel = pixel_handler.get_or_create_pixel(telescope, pixel)
                if fine_pixels.any():
                    all_sources = source_pixel.all(defaults=defaults).filter(
                        pl.col("Heal_Pix_Position").is_in(fine_pixels)
                    )
                else:
                    all_sources = source_pixel.all(defaults=defaults)
                all_sources = self.filter(all_sources)
                if all_sources.is_empty():
                    continue
                if first:
                    first = False
                    yield f"{all_sources.write_json()[1:-1]}"
                else:
                    yield f",{all_sources.write_json()[1:-1]}"
        yield "]"


class DataStore:
    """Data Store handle"""

    def __init__(self, dataset_root, telescopes="*"):
        """The datastore init method."""
        self.dataset_root = dataset_root
        # self.pixel_handler = PixelHandler(self.dataset_root)
        self._telescopes_search = telescopes
        self.telescopes = {}
        self.last_loaded_at = "0"

    def reload(self):
        """Reload the datasets"""

        last_update_file = self.dataset_root / ".last_updated"
        if last_update_file.exists():
            with last_update_file.open("r", encoding="utf8") as file:
                last_updated_at = file.read()
                if self.last_loaded_at == last_updated_at:
                    logger.info("Skip reload as content has not changed")
                    return
                self.last_loaded_at = last_updated_at

        logger.info("Reloading datasets...")
        self.telescopes = {
            telescope: PixelHandler(self.dataset_root, telescope)
            for telescope in self._telescope_args(self._telescopes_search)
        }
        self._load_datasets()

    def add_source(self, source, telescope, pixel):
        """Add a source or sources to the datastore"""
        if telescope not in self.telescopes.keys():
            self.telescopes[telescope] = PixelHandler(self.dataset_root, telescope)
        pixel_handler = self.telescopes[telescope]
        source_pixel = pixel_handler.get_or_create_pixel(telescope, pixel)
        source_pixel.add(source)

    def add_dataset(self, sources, telescope, pixel):
        """Add a source or sources to the datastore."""
        if telescope not in self.telescopes.keys():
            self.telescopes[telescope] = PixelHandler(self.dataset_root, telescope)
        pixel_handler = self.telescopes[telescope]
        source_pixel = pixel_handler.get_or_create_pixel(telescope, pixel)
        source_pixel.add(sources)

    def save(self):
        """Commit all data to file"""
        for pixel_handler in self.telescopes.values():
            pixel_handler.save()

    def query_pxiels(self, search_query):
        """Instantiate a search"""
        search_query["telescopes"] = search_query.get("telescopes", self.telescopes.keys())
        return Search(self.dataset_root, search_query)

    def _telescope_args(self, telescopes):
        """Get all telescopes that have been instantiated."""
        available_names = []
        root_path = Path(self.dataset_root)
        if not root_path.is_dir():
            logger.warning("Datasets directory is missing.")
            return []
        tel_available = root_path.iterdir()
        for tel_name in tel_available:
            if "ingest" in str(tel_name):
                continue
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
        if pixel_handler:
            pixel_handlers = [pixel_handler]
        else:
            pixel_handlers = self.telescopes.values()
        sources = None
        for ph in pixel_handlers:
            for sources_pixel in ph:
                sources_pixel = sources_pixel.all()
                if sources is None:
                    sources = sources_pixel
                    continue
                for col_name, _ in sources_pixel.schema.items():
                    if col_name not in sources.schema.names():
                        sources = sources.with_columns(pl.lit(None).alias(col_name))
                sources = sources.update(sources_pixel, on="name", how="full")
        return sources

    def _load_datasets(self):
        """Load catalogue datasets"""
        for telescope, pixel_handler in self.telescopes.items():
            tel_root = Path(self.dataset_root, telescope)
            logger.info("Loading ... %s", tel_root)
            for pixel in tel_root.iterdir():
                if pixel.name == "catalogue.yaml":
                    continue
                source_pixel = SourcePixel(telescope, pixel.name, self.dataset_root)
                pixel_handler.append(source_pixel)

    def has_telescope(self, telescope):
        """Check whether a catalogue is currently present in the datastore"""
        if telescope in self.telescopes.keys():
            return True
        return False

    def add_telescope(self, telescope):
        """Add a telescope (catalog) to the datastore."""
        if not self.has_telescope(telescope):
            Path(self.dataset_root, telescope).mkdir(parents=True, exist_ok=True)
