import logging
from pathlib import Path

import geopandas as gpd
import xarray as xr
from earthkit.plots import quickplot

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
    end = '2025-03'

    # download
    paths = era5_land.hourly.download(start, end, bbox, dirname=dirname, prefix=prefix, 
                                      skip_existing=True)
    logging.info(paths)
    assert len(paths) == 3

    # test opening multifile xarray
    ds = xr.open_mfdataset(paths)
    logging.info(ds)

    # test aggregating temperature to daily
    daily_temp = ds['t2m'].resample(valid_time='1D').mean().compute()
    logging.info(daily_temp)

    # test visualize
    #fig = quickplot(ds.sel(valid_time=start))
    #fig.save(dirname / 'quickplot.png')
