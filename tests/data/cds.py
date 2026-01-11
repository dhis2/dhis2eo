import logging
from pathlib import Path

import geopandas as gpd

from dhis2eo.data.cds import era5_land

DATA_DIR = Path(__file__).parent.parent / "test_data"

# set verbose logging (hacky for now)
logging.basicConfig(
    level=logging.INFO,  # or DEBUG for more details
)


def test_download_hourly_era5_data():
    # download args
    dirname = DATA_DIR / '../test_outputs/cds'
    prefix = 'era5_hourly_sierra_leone'
    # get bbox
    geojson_file = DATA_DIR / "sierra-leone-districts.geojson"
    org_units = gpd.read_file(geojson_file)
    bbox = org_units.total_bounds
    # start/end dates
    start = '2025-01'
    end = '2025-12'
    # download
    paths = era5_land.hourly.retrieve(start, end, bbox, dirname=dirname, prefix=prefix)
    logging.info(paths)
    assert len(paths) == 12

    # test opening multifile xarray
    import xarray as xr
    ds = xr.open_mfdataset(paths)
    logging.info(ds)

    # test aggregating temperature to daily
    daily_temp = ds['t2m'].resample(valid_time='1D').mean()
    logging.info(daily_temp)
