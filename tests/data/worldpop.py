import logging
from pathlib import Path

import geopandas as gpd
import xarray as xr
from earthkit.plots import quickplot

from dhis2eo.data.worldpop import pop_total

DATA_DIR = Path(__file__).parent.parent / "test_data"

# set verbose logging (hacky for now)
logging.basicConfig(
    level=logging.INFO,  # or DEBUG for more details
)


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
                                      dirname=dirname, prefix=prefix, skip_existing=True)
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
    #fig = quickplot(ds.sel(time=end))
    #fig.save(dirname / 'quickplot.png')
