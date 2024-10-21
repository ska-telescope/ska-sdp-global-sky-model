import polars as pl
from os import listdir, walk, mkdir
from os.path import isfile, join
import time

from astropy.cosmology import available
# from tomlkit.source import Source
from pathlib import Path


class SourcePixel:
    def __init__(self, telescope, pixel, dataset_root):
        self.pixel = pixel
        self.telescope = telescope
        self.dataset_root = dataset_root
        self.dataset = self.read()
    @property
    def source_root(self):
        return join(self.dataset_root, self.telescope, str(self.pixel))
    def read(self):
        if not isfile(self.source_root):
            return pl.DataFrame([], schema={
                'name': str,
                'Heal_Pix_Position': pl.Int64,
                'Flux_Wide': pl.Float64
            })
        return pl.read_csv(self.source_root)
    def add(self, source_new):
        if self.dataset.is_empty():
            self.dataset = source_new
        else:
            for col_name, col_type in source_new.schema.items():
                if col_name not in self.dataset.schema.names():
                    self.dataset = self.dataset.with_columns(pl.lit(None).alias(col_name))
            self.dataset =self.dataset.update(source_new, on='name', how='full')
    def save(self):
        Path(join(self.dataset_root, self.telescope)).mkdir(parents=True, exist_ok=True)
        open(self.source_root, 'a').close()
        self.dataset.write_csv(self.source_root)
    def all(self):
        return self.dataset


class PixelHandler:
    def __init__(self, data_root):
        self.index = 0
        self.pixels = []
        self.data_root = data_root
    def append(self, source_pixel):
        self.pixels.append(source_pixel)
    def get_or_create_pixel(self, telescope, pixel):
        for source_pixel in self.pixels:
            if source_pixel.telescope == telescope:
                if source_pixel.pixel == pixel:
                    return source_pixel
        source_pixel = SourcePixel(telescope, pixel, self.data_root)
        self.pixels.append(source_pixel)
        return source_pixel
    def save(self):
        for pixel in self.pixels:
            pixel.save()
    def __iter__(self):
        return self
    def __next__(self):
        try:
            pixel = self.pixels[self.index]
        except IndexError:
            raise StopIteration
        self.index += 1
        return pixel
    def __len__(self):
        return len(self.pixels)
    def __getitem__(self, index):
        return self.pixels[index]

class Search:

    def __init__(self, dataset_root, search_query):
        self.pixel_handler = PixelHandler(dataset_root)
        self.search_query = search_query
    def stream(self):
        telescopes = self.search_query.get('telescopes')
        if not telescopes:
            return "No telescope was found"
        pixels = self.search_query.get("healpix_pixel_rough", None)
        flux_wide = self.search_query.get("flux_wide", None)
        if not pixels.any():
            return "Empty search"
        fine_pixels = self.search_query.get("hp_pixel_fine",)
        for telescope in telescopes:
            for pixel in pixels:
                source_pixel =self.pixel_handler.get_or_create_pixel(telescope, pixel)
                if fine_pixels.any():
                    all_sources = source_pixel.all().filter(pl.col("Heal_Pix_Position").is_in(fine_pixels))
                else:
                    all_sources = source_pixel.all()
                if flux_wide:
                     all_sources = all_sources.filter(pl.col("Flux_Wide") > flux_wide)
                yield all_sources.write_json()

class DataStore:
    def __init__(self, dataset_root, telescopes='*'):
        self.dataset_root = dataset_root
        self.pixel_handler = PixelHandler(self.dataset_root)
        self.telescopes = self._telescope_args(telescopes)
        self._load_datasets()
    def add_source(self, source, telescope, pixel):
        source_pixel = self.pixel_handler.get_or_create_pixel(telescope, pixel)
        source_pixel.add(source)
    def add_dataset(self, sources, telescope, pixel):
        source_pixel = self.pixel_handler.get_or_create_pixel(telescope, pixel)
        source_pixel.add(sources)
    def save(self):
        self.pixel_handler.save()
    def query_pxiels(self, search_query):
        search_query["telescopes"] = search_query.get("telescopes", self.telescopes)
        return Search(self.dataset_root, search_query)
    def _telescope_args(self, telescopes):
        available_names = []
        tel_available = next(walk(self.dataset_root))[1]
        for tel_name in tel_available:
            if not tel_name or tel_name[0] == '.': continue
            available_names.append(tel_name)
        if telescopes == '*':
            return available_names
        if type(telescopes) == str:
            telescopes = telescopes.split(',')
        telescopes = [telescope.strip() for telescope in telescopes]
        return list(set(telescopes) & set(available_names))
    def all(self, pixel_handler = None):
        if not pixel_handler:
            pixel_handler = self.pixel_handler
        sources = pixel_handler[0].all()
        for i in range(1,len(pixel_handler)-1):
            sources_pixel = pixel_handler[i].all()
            for col_name, col_type in sources_pixel.schema.items():
                if col_name not in sources.schema.names():
                    sources =   sources.with_columns(pl.lit(None).alias(col_name))
            sources = sources.update(sources_pixel, on="name", how="full")
        return sources
    def _load_datasets(self):
        for telescope in self.telescopes:
            tel_root = join(self.dataset_root, telescope)
            for pixel in listdir(tel_root):
                source_pixel = SourcePixel(telescope, pixel, self.dataset_root)
                self.pixel_handler.append(source_pixel)
    def has_telescope(self, telescope):
        if telescope in self.telescopes:
            return True
        return False
    def add_telescope(self, telescope):
        if not self.has_telescope(telescope):
            mkdir(join(self.dataset_root, telescope))
