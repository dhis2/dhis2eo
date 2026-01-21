import logging
from pathlib import Path
import pytest

import geopandas as gpd
import xarray as xr

from dhis2eo.data.worldpop import pop_total

DATA_DIR = Path(__file__).parent.parent / "test_data"

# set verbose logging (hacky for now)
logging.basicConfig(
    level=logging.INFO,  # or DEBUG for more details
)


@pytest.mark.integration
def test_download_yearly_population_data():
    # download args
    dirname = DATA_DIR / '../test_outputs/worldpop'
    prefix = 'population_yearly_sierra_leone'

    # get country
    country_code = 'SLE'

    # start/end dates
    start = "2015"
    end = "2020"

    # download
    paths = pop_total.yearly.download(start, end, country_code=country_code, 
                                      dirname=dirname, prefix=prefix)
    logging.info(paths)
    assert len(paths) == 6

    # test opening multifile xarray
    ds = xr.open_mfdataset(paths)
    logging.info(ds)

    # test total aggregate
    for yr in range(int(start), int(end)+1):
        ds_year = ds.sel(time=str(yr))
        total_pop = ds_year['pop_total'].sum().compute().item() # compute+item are needed to execute the dask task
        logging.info(f'Total population {yr}: {total_pop}')

    # test visualize
    #from earthkit.plots import quickplot
    #fig = quickplot(ds.sel(time=end))
    #fig.save(dirname / 'quickplot.png')


@pytest.mark.integration
def test_download_yearly_population_versions():
    # NOTE: in addition to testing the ability to download both global1 and global2 versions
    # ...this also tests that we can stitch them together to get a single timeseries across the 
    # ...version break (2015). mostly just for exploration, this may or may not be a good idea
    # ...due to different methodologies etc.

    # download args
    dirname = DATA_DIR / '../test_outputs/worldpop'
    prefix = 'population_yearly_sierra_leone'

    # get country
    country_code = 'SLE'

    # download global1 data for 2010-2014
    start = "2010"
    end = "2014"
    global1_paths = pop_total.yearly.download(start, end, country_code=country_code, 
                                            dirname=dirname, prefix=prefix, version='global1')
    global1_ds = xr.open_mfdataset(global1_paths)
    logging.info(global1_ds)

    # download global2 data for 2015-2020 (this is the default version)
    start = "2015"
    end = "2020"
    global2_paths = pop_total.yearly.download(start, end, country_code=country_code, 
                                            dirname=dirname, prefix=prefix) # version='global2'
    global2_ds = xr.open_mfdataset(global2_paths)
    logging.info(global2_ds)

    # ensure both versions use the same ordering of x coordinates
    if global1_ds.x[0] > global1_ds.x[-1]:
        global1_ds = global1_ds.sortby("x")
    if global2_ds.x[0] > global2_ds.x[-1]:
        global2_ds = global2_ds.sortby("x")

    # join the two into one dataset
    ds = xr.concat([global1_ds, global2_ds], dim="time")
    logging.info(ds)

    # test total population changes across the version break
    for yr in range(int(2010), int(2020)+1):
        ds_year = ds.sel(time=str(yr))
        total_pop = ds_year['pop_total'].sum().compute().item() # compute+item are needed to execute the dask task
        logging.info(f'Total population {yr}: {total_pop}')

