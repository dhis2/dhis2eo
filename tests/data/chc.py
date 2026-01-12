import logging
from pathlib import Path

import geopandas as gpd
import xarray as xr
from earthkit.plots import quickplot

from dhis2eo.data.chc import chirps3

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
                                   dirname=dirname, prefix=prefix, skip_existing=True)
    logging.info(paths)
    assert len(paths) == 3

    # test opening multifile xarray
    ds = xr.open_mfdataset(paths)
    logging.info(ds)

    # test aggregating to monthly
    monthly_temp = ds['precip'].resample(time='1M').mean().compute()
    logging.info(monthly_temp)

    # test visualize
    #fig = quickplot(ds.sel(time=start))
    #fig.save(dirname / 'quickplot.png')
