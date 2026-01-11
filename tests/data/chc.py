import logging
from pathlib import Path

import geopandas as gpd
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
    start = "2025-07"
    end = "2025-08"
    # download
    paths = chirps3.daily.retrieve(start=start, end=end, bbox=bbox,
                                   dirname=dirname, prefix=prefix, skip_existing=True)
    logging.info(paths)
    assert len(paths) == 2

    # test opening multifile xarray
    import xarray as xr
    ds = xr.open_mfdataset(paths)
    logging.info(ds)

    # test visualize
    from earthkit.plots import quickplot
    fig = quickplot(ds)
    fig.save(DATA_DIR / '../test_outputs/chc/quickplot.png')
