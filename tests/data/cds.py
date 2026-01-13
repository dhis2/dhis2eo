import logging
from pathlib import Path
from datetime import date, timedelta

import geopandas as gpd
import xarray as xr
from earthkit.plots import quickplot

from dhis2eo.data.cds import era5_land
from dhis2eo.utils.time import months_ago

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
    variables = ['2m_temperature', 'total_precipitation']
    paths = era5_land.hourly.download(start, end, bbox, dirname=dirname, prefix=prefix, 
                                      variables=variables, skip_existing=True)
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

def test_download_hourly_era5_skip_incomplete_month():
    # download args
    dirname = DATA_DIR / '../test_outputs/cds'
    prefix = 'era5_hourly_sierra_leone'

    # get bbox
    geojson_file = DATA_DIR / "sierra-leone-districts.geojson"
    org_units = gpd.read_file(geojson_file)
    bbox = org_units.total_bounds

    # start/end dates (current and previous month only)
    current_date = date.today()
    last_month = months_ago(current_date, 1)
    start = last_month.isoformat()[:7]
    end = current_date.isoformat()[:7]
    logging.info(f'Today is {current_date.isoformat()}')
    logging.info(f'Testing months {start} to {end}')

    # download
    variables = ['2m_temperature', 'total_precipitation']
    paths = era5_land.hourly.download(start, end, bbox, dirname=dirname, prefix=prefix, 
                                      variables=variables, skip_existing=True)
    logging.info(paths)

    # at least the current month should not be downloaded
    # if it's earlier than the 7th day, then the previous month will also be skipped
    # this means either 0 or 1 months should be downloaded and returned
    assert len(paths) < 2

def test_download_monthly_era5_data():
    # download args
    dirname = DATA_DIR / '../test_outputs/cds'
    prefix = 'era5_monthly_sierra_leone'

    # get bbox
    geojson_file = DATA_DIR / "sierra-leone-districts.geojson"
    org_units = gpd.read_file(geojson_file)
    bbox = org_units.total_bounds

    # start/end dates
    start = '1990'
    end = '2025'

    # download
    variables = ['2m_temperature', 'total_precipitation']
    paths = era5_land.monthly.download(start, end, bbox, dirname=dirname, prefix=prefix, 
                                      variables=variables, skip_existing=True)
    logging.info(paths)
    assert len(paths) == 1

    # test opening the data
    ds = xr.open_dataset(paths[0])
    logging.info(ds)
