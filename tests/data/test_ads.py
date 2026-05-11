import logging
from pathlib import Path
from datetime import date, timedelta
import pytest

import geopandas as gpd
import xarray as xr

from dhis2eo.data.ads import cams
from dhis2eo.utils.time import months_ago

DATA_DIR = Path(__file__).parent.parent / "test_data"

# set verbose logging (hacky for now)
logging.basicConfig(
    level=logging.INFO,  # or DEBUG for more details
)


@pytest.mark.integration
def test_download_hourly_cams_data():
    # download args
    dirname = DATA_DIR / '../test_outputs/ads'
    prefix = 'cams_hourly_sierra_leone'

    # get bbox
    geojson_file = DATA_DIR / "sierra-leone-districts.geojson"
    org_units = gpd.read_file(geojson_file)
    bbox = org_units.total_bounds

    # start/end dates
    start = '2025-01'
    end = '2025-02'

    # download
    variables = [
        "particulate_matter_1um",
        "particulate_matter_2.5um",
        "particulate_matter_10um"
    ]
    paths = cams.hourly.download(start, end, bbox, dirname=dirname, prefix=prefix,
                                 variables=variables, overwrite=True)
    logging.info(paths)
    assert len(paths) == 2

    # test opening multifile xarray
    ds = xr.open_mfdataset(paths)
    logging.info(ds)

    # test aggregating temperature to daily
    daily_temp = ds['pm10'].resample(valid_time='1D').mean().compute()
    logging.info(daily_temp)

    # test visualize
    from earthkit.plots import quickplot
    fig = quickplot(ds['pm10'].sel(valid_time=start))
    fig.save(dirname / 'cams.png')


@pytest.mark.integration
def test_download_hourly_cams_skip_incomplete_month():
    # download args
    dirname = DATA_DIR / '../test_outputs/ads'
    prefix = 'cams_hourly_sierra_leone'

    # get bbox
    geojson_file = DATA_DIR / "sierra-leone-districts.geojson"
    org_units = gpd.read_file(geojson_file)
    bbox = org_units.total_bounds

    # start/end dates (last 12 months)
    current_date = date.today()
    last_month = months_ago(current_date, 12)
    start = last_month.isoformat()[:7]
    end = current_date.isoformat()[:7]
    logging.info(f'Today is {current_date.isoformat()}')
    logging.info(f'Testing months {start} to {end}')

    # download
    variables = [
        "particulate_matter_1um",
        "particulate_matter_2.5um",
        "particulate_matter_10um"
    ]
    paths = cams.hourly.download(start, end, bbox, dirname=dirname, prefix=prefix, 
                                 variables=variables, overwrite=False)
    logging.info(paths)

    # no more than 6 months should be downloaded
    # anything newer than 9 months should be skipped
    assert len(paths) < 6
