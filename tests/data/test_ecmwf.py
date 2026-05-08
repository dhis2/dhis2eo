import logging
from pathlib import Path
from datetime import date, timedelta
import pytest

import geopandas as gpd
import xarray as xr

from dhis2eo.data.ecmwf import ifs, aifs
from dhis2eo.utils.time import months_ago

DATA_DIR = Path(__file__).parent.parent / "test_data"

# set verbose logging (hacky for now)
logging.basicConfig(
    level=logging.INFO,  # or DEBUG for more details
)


@pytest.mark.integration
def test_download_hourly_ifs_data():
    # download args
    dirname = DATA_DIR / '../test_outputs/ecmwf'
    prefix = 'ifs_hourly_sierra_leone'

    # get bbox
    geojson_file = DATA_DIR / "sierra-leone-districts.geojson"
    org_units = gpd.read_file(geojson_file)
    bbox = org_units.total_bounds

    # start/end dates
    start = ( date.today() - timedelta(days=1) ).isoformat()
    end = start

    # download
    variables = ['2t'] # tp
    paths = ifs.hourly.download(start, end, bbox, dirname=dirname, prefix=prefix, 
                                variables=variables, overwrite=True)
    logging.info(paths)
    assert len(paths) == 1

    # test opening multifile xarray
    ds = xr.open_mfdataset(paths).load()
    logging.info(ds)

    # test visualize
    from earthkit.plots import quickplot
    fig = quickplot(ds.sel(time=start).isel(step=-1))
    fig.save(dirname / 'ifs.png')


@pytest.mark.integration
def test_download_hourly_aifs_data():
    # download args
    dirname = DATA_DIR / '../test_outputs/ecmwf'
    prefix = 'aifs_hourly_sierra_leone'

    # get bbox
    geojson_file = DATA_DIR / "sierra-leone-districts.geojson"
    org_units = gpd.read_file(geojson_file)
    bbox = org_units.total_bounds

    # start/end dates
    start = ( date.today() - timedelta(days=1) ).isoformat()
    end = start

    # download
    variables = ['2t'] # or tp
    paths = aifs.hourly.download(start, end, bbox, dirname=dirname, prefix=prefix, 
                                variables=variables, overwrite=True)
    logging.info(paths)
    assert len(paths) == 1

    # test opening multifile xarray
    ds = xr.open_mfdataset(paths).load()
    logging.info(ds)

    # test visualize
    from earthkit.plots import quickplot
    fig = quickplot(ds.sel(time=start).isel(step=-1))
    fig.save(dirname / 'aifs.png')
