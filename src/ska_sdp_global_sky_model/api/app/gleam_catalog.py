from astropy import units as u
from astropy.coordinates import SkyCoord
from astroquery.vizier import Vizier

from ska_sdp_global_sky_model.api.app.model import (
    Band,
    NarrowBandData,
    Source,
    Telescope,
    WideBandData,
)


def create_point(ra, dec):
    return SkyCoord(ra * u.deg, dec * u.deg)


def get_full_catalog(db):
    telescope = (
        db.query(Telescope).filter_by(name="Murchison Widefield Array").first()
    )
    if not telescope:
        telescope = Telescope(
            name="Murchison Widefield Array",
            frequency_min=80,
            frequency_max=300,
            ingested=False,
        )
        db.add(telescope)
        db.commit()
    if telescope:
        if telescope.ingested:
            return 0
    Vizier.ROW_LIMIT = -1
    Vizier.columns = ["**"]
    catalog = Vizier.get_catalogs("VIII/100")
    source_data = catalog[1]

    bands = {}
    for band_cf in [
        76,
        84,
        92,
        99,
        107,
        115,
        122,
        130,
        143,
        151,
        158,
        166,
        174,
        181,
        189,
        197,
        204,
        212,
        220,
        227,
    ]:
        band = (
            db.query(Band)
            .filter_by(centre=band_cf, telescope=telescope.id)
            .first()
        )
        if not band:
            band = Band(centre=band_cf, telescope=telescope.id)
            db.add(band)
            db.commit()
        bands[band_cf] = band
    for source in source_data:
        name = source["GLEAM"]
        if db.query(Source).filter_by(name=name).count():
            # If we have already ingested this, skip.
            continue
        point = create_point(source["RAJ2000"], source["DEJ2000"])
        source_catalog = Source(
            name=name,
            Heal_Pix_Position=point,
            RAJ2000=source["RAJ2000"],
            RAJ2000_Error=source["e_RAJ2000"],
            DECJ2000=source["DEJ2000"],
            DECJ2000_Error=source["e_DEJ2000"],
        )
        db.add(source_catalog)
        db.commit()
        source_float = {}
        for k in source.keys():
            if k == "GLEAM":
                pass
            else:
                source_float[k] = float(source[k])
        source = source_float
        wide_band_data = WideBandData(
            Bck_Wide=source["bckwide"],
            Local_RMS_Wide=source["lrmswide"],
            Int_Flux_Wide=source["Fintwide"],
            Int_Flux_Wide_Error=source["e_Fintwide"],
            Resid_Mean_Wide=source["resmwide"],
            Resid_Sd_Wide=source["resstdwide"],
            Abs_Flux_Pct_Error=source["e_Fpwide"],
            Fit_Flux_Pct_Error=source["efitFpct"],
            A_PSF_Wide=source["psfawide"],
            B_PSF_Wide=source["psfbwide"],
            PA_PSF_Wide=source["psfPAwide"],
            Spectral_Index=source["alpha"],
            Spectral_Index_Error=source["e_alpha"],
            A_Wide=source["awide"],
            A_Wide_Error=source["e_awide"],
            B_Wide=source["bwide"],
            B_Wide_Error=source["e_bwide"],
            PA_Wide=source["pawide"],
            PA_Wide_Error=source["e_pawide"],
            Flux_Wide=source["Fpwide"],
            Flux_Wide_Error=source["eabsFpct"],
            telescope=telescope.id,
            source=source_catalog.id,
        )
        db.add(wide_band_data)
        db.commit()
        for band_cf in bands.keys():
            band_id = bands[band_cf].id
            band_cf = ("0" + str(band_cf))[-3:]
            narrow_band_data = NarrowBandData(
                Bck_Narrow=source[f"bck{band_cf}"],
                Local_RMS_Narrow=source[f"lrms{band_cf}"],
                Int_Flux_Narrow=source[f"Fint{band_cf}"],
                Int_Flux_Narrow_Error=source[f"e_Fint{band_cf}"],
                Resid_Mean_Narrow=source[f"resm{band_cf}"],
                Resid_Sd_Narrow=source[f"resstd{band_cf}"],
                A_PSF_Narrow=source[f"psfa{band_cf}"],
                B_PSF_Narrow=source[f"psfb{band_cf}"],
                PA_PSF_Narrow=source[f"psfPA{band_cf}"],
                A_Narrow=source[f"a{band_cf}"],
                B_Narrow=source[f"b{band_cf}"],
                PA_Narrow=source[f"pa{band_cf}"],
                Flux_Narrow=source[f"Fp{band_cf}"],
                Flux_Narrow_Error=source[f"e_Fp{band_cf}"],
                source=source_catalog.id,
                band=band_id,
            )
            db.add(narrow_band_data)
            db.commit()
    telescope.ingested = True
    db.add(telescope)
    db.commit()
    return True
