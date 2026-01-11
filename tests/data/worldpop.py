import logging
from pathlib import Path

import geopandas as gpd
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
    # get bbox
    #geojson_file = DATA_DIR / "sierra-leone-districts.geojson"
    #org_units = gpd.read_file(geojson_file)
    #bbox = org_units.total_bounds
    country_code = 'SLE'
    # start/end dates
    start = "2015"
    end = "2018"
    # download
    paths = pop_total.yearly.retrieve(start, end, country_code=country_code, 
                                      dirname=dirname, prefix=prefix, skip_existing=False)
    logging.info(paths)
    assert len(paths) == 4

    # test opening multifile xarray
    import xarray as xr
    ds = xr.open_mfdataset(paths)
    logging.info(ds)

    # test visualize
    from earthkit.plots import quickplot
    fig = quickplot(ds)
    fig.save(DATA_DIR / '../test_outputs/worldpop/quickplot.png')
