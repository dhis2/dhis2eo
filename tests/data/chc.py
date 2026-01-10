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
    geojson_file = DATA_DIR / "sierra-leone-districts.geojson" #"geoBoundaries-MWI-ADM2.geojson"
    org_units = gpd.read_file(geojson_file)
    data = chirps3.daily.get(start="2025-07-01", end="2025-07-03", bbox=org_units.total_bounds)
    logging.info(data)
