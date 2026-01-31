import logging
from pathlib import Path

import geopandas as gpd
import xarray as xr

from dhis2eo.data.ecmwf import tigge

DATA_DIR = Path(__file__).parent.parent / "test_data"
OUTPUT_DIR = Path(__file__).parent.parent / "test_outputs"

# set verbose logging (hacky for now)
logging.basicConfig(
    level=logging.INFO,  # or DEBUG for more details
)

def test_tigge_download():
    # get bbox
    org_units = gpd.read_file(f'{DATA_DIR}/sierra-leone-districts.geojson')

    # download
    start = '2026-01'
    end = '2026-01'
    files = tigge.hourly.download(
        start=start, end=end,
        bbox=org_units.total_bounds, 
        dirname=OUTPUT_DIR, 
        prefix='tigge-test', 
        variables=['2m_temperature']
    )

    # test open
    print(files)
    ds = xr.open_mfdataset(files)
    print(ds)
