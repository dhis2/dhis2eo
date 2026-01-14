import logging
from pathlib import Path
from datetime import date, timedelta

import geopandas as gpd
import xarray as xr

from dhis2eo.data.chc import chirps3
from dhis2eo.utils.time import months_ago

DATA_DIR = Path(__file__).parent.parent / "test_data"

# set verbose logging (hacky for now)
logging.basicConfig(
    level=logging.INFO,  # or DEBUG for more details
)


######################


def test_download_daily_chirps3_data():
    # download args
    dirname = DATA_DIR / '../test_outputs/chc'
    prefix = 'chirps3_daily_sierra_leone'

    # get bbox
    geojson_file = DATA_DIR / "sierra-leone-districts.geojson"
    org_units = gpd.read_file(geojson_file)
    bbox = org_units.total_bounds

    # start/end dates
    start = "2025-01"
    end = "2025-03"

    # download
    paths = chirps3.daily.download(start=start, end=end, bbox=bbox,
                                   dirname=dirname, prefix=prefix)
    logging.info(paths)
    assert len(paths) == 3

    # test opening multifile xarray
    ds = xr.open_mfdataset(paths)
    logging.info(ds)

    # test aggregating to monthly
    monthly_temp = ds['precip'].resample(time='1M').mean().compute()
    logging.info(monthly_temp)

    # test visualize
    #from earthkit.plots import quickplot
    #fig = quickplot(ds.sel(time=start))
    #fig.save(dirname / 'quickplot.png')


def test_download_daily_chirps3_skip_incomplete_month():
    # download args
    dirname = DATA_DIR / '../test_outputs/chc'
    prefix = 'chirps3_daily_sierra_leone'

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
    paths = chirps3.daily.download(start, end, bbox, dirname=dirname, prefix=prefix)
    logging.info(paths)

    # at least the current month should not be downloaded
    # if it's earlier than the 20th day, then the previous month will also be skipped
    # this means either 0 or 1 months should be downloaded and returned
    assert len(paths) < 2
