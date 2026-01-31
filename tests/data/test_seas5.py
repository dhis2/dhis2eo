import logging
from pathlib import Path

import geopandas as gpd
import xarray as xr

from dhis2eo.data.ecmwf import seas5

DATA_DIR = Path(__file__).parent.parent / "test_data"
OUTPUT_DIR = Path(__file__).parent.parent / "test_outputs"

# set verbose logging (hacky for now)
logging.basicConfig(
    level=logging.INFO,  # or DEBUG for more details
)

def test_seas5_download():
    # get bbox
    org_units = gpd.read_file(f'{DATA_DIR}/sierra-leone-districts.geojson')

    # download
    start = '2025-11'
    end = '2025-11'
    files = seas5.hourly.download(
        start=start, end=end,
        bbox=org_units.total_bounds, 
        dirname=OUTPUT_DIR, 
        prefix='seas5-test', 
        variables=['2m_temperature']
    )

    # test open
    print(files)
    ds = xr.open_mfdataset(files)
    print(ds)

if __file__ == '__main__':
    test_seas5_download()
