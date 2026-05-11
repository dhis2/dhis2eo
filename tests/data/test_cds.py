import logging
from pathlib import Path
from datetime import date, timedelta
import pytest

import geopandas as gpd
import xarray as xr

from dhis2eo.data.cds import era5_land, era5_heat, era5_drought, esa_landcover
from dhis2eo.utils.time import months_ago

DATA_DIR = Path(__file__).parent.parent / "test_data"

# set verbose logging (hacky for now)
logging.basicConfig(
    level=logging.INFO,  # or DEBUG for more details
)


@pytest.mark.integration
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
    end = '2025-02'

    # download
    variables = ['2m_temperature', 'total_precipitation']
    paths = era5_land.hourly.download(start, end, bbox, dirname=dirname, prefix=prefix, 
                                      variables=variables, overwrite=True)
    logging.info(paths)
    assert len(paths) == 2

    # test opening multifile xarray
    ds = xr.open_mfdataset(paths)
    logging.info(ds)

    # test aggregating temperature to daily
    daily_temp = ds['t2m'].resample(valid_time='1D').mean().compute()
    logging.info(daily_temp)

    # test visualize
    #from earthkit.plots import quickplot
    #fig = quickplot(ds.sel(valid_time=start))
    #fig.save(dirname / 'quickplot.png')


@pytest.mark.integration
def test_download_hourly_era5_data_no_server_cache():
    # download args
    dirname = DATA_DIR / '../test_outputs/cds'
    prefix = 'era5_hourly_sierra_leone'

    # get bbox
    geojson_file = DATA_DIR / "sierra-leone-districts.geojson"
    org_units = gpd.read_file(geojson_file)
    bbox = org_units.total_bounds

    # start/end dates
    start = '2025-01'
    end = '2025-01'

    # download
    variables = ['2m_temperature']
    paths = era5_land.hourly.download(start, end, bbox, dirname=dirname, prefix=prefix, 
                                      variables=variables, overwrite=True,
                                      use_server_cache=False)
    logging.info(paths)
    assert len(paths) == 1


@pytest.mark.integration
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
                                      variables=variables, overwrite=False)
    logging.info(paths)

    # at least the current month should not be downloaded
    # if it's earlier than the 7th day, then the previous month will also be skipped
    # this means either 0 or 1 months should be downloaded and returned
    assert len(paths) < 2


@pytest.mark.integration
def test_download_monthly_era5_data():
    # download args
    dirname = DATA_DIR / '../test_outputs/cds'
    prefix = 'era5_monthly_sierra_leone'

    # get bbox
    geojson_file = DATA_DIR / "sierra-leone-districts.geojson"
    org_units = gpd.read_file(geojson_file)
    bbox = org_units.total_bounds

    # start/end dates
    start = '2020'
    end = '2025'

    # download
    variables = ['2m_temperature', 'total_precipitation']
    paths = era5_land.monthly.download(start, end, bbox, dirname=dirname, prefix=prefix, 
                                      variables=variables, overwrite=True)
    logging.info(paths)
    assert len(paths) == 1

    # test opening the data
    ds = xr.open_dataset(paths[0])
    logging.info(ds)

    
@pytest.mark.integration
def test_download_hourly_era5heat_data():
    # download args
    dirname = DATA_DIR / '../test_outputs/cds'
    prefix = 'era5heat_hourly_sierra_leone'

    # get bbox
    geojson_file = DATA_DIR / "sierra-leone-districts.geojson"
    org_units = gpd.read_file(geojson_file)
    bbox = org_units.total_bounds

    # start/end dates
    start = '2026-03'
    end = '2026-04'

    # download
    variables = ['universal_thermal_climate_index']
    paths = era5_heat.hourly.download(start, end, bbox, dirname=dirname, prefix=prefix, 
                                      variables=variables, overwrite=False)
    logging.info(paths)
    assert len(paths) == 2

    # test opening the data
    ds = xr.open_mfdataset(paths)
    logging.info(ds)
  
  
@pytest.mark.integration
def test_download_monthly_era5drought_data():
    # download args
    dirname = DATA_DIR / '../test_outputs/cds'
    prefix = 'era5drought_monthly_sierra_leone'

    # start/end dates
    start = '2025'
    end = '2026'

    # download
    variables = ['standardised_precipitation_index', 'standardised_precipitation_evapotranspiration_index']
    paths = era5_drought.monthly.download(start, end, bbox, dirname=dirname, prefix=prefix, 
                                      variables=variables, overwrite=True)
    logging.info(paths)
    assert len(paths) == 1

    # test opening the data
    ds = xr.open_dataset(paths[0])
    logging.info(ds)
    

@pytest.mark.integration
def test_download_yearly_esa_landcover():
    # download args
    dirname = DATA_DIR / '../test_outputs/cds'
    prefix = 'esa_landcover_yearly_sierra_leone'

    # get bbox
    geojson_file = DATA_DIR / "sierra-leone-districts.geojson"
    org_units = gpd.read_file(geojson_file)
    bbox = org_units.total_bounds

    # start/end dates
    start = '2013'
    end = '2022'

    # download
    paths = esa_landcover.yearly.download(start, end, bbox, dirname=dirname, prefix=prefix, 
                                          overwrite=False)
    logging.info(paths)
    assert len(paths) == 10

    # test opening the data
    ds = xr.open_mfdataset(paths)
    logging.info(ds)

    # test visualize
    import matplotlib.pyplot as plt
    import numpy as np
    fig, ax = plt.subplots()
    lc_classes = np.unique(ds['lccs_class'].values)
    ds['lccs_class'].isel(time=-1).plot(ax=ax, levels=len(lc_classes), cmap='tab20')
    fig.savefig(dirname / 'esa_landcover.png')
