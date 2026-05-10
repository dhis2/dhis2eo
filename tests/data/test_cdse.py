import logging
from pathlib import Path
from datetime import date, timedelta
import pytest

import geopandas as gpd
import xarray as xr

from dhis2eo.data.cdse import clms_water, clms_ndvi, modis_landcover

DATA_DIR = Path(__file__).parent.parent / "test_data"

# set verbose logging (hacky for now)
logging.basicConfig(
    level=logging.INFO,  # or DEBUG for more details
)


@pytest.mark.integration
def test_download_monthly_clms_water():
    # download args
    dirname = DATA_DIR / '../test_outputs/cdse'
    prefix = 'clms_water_sierra_leone'

    # get bbox
    geojson_file = DATA_DIR / "sierra-leone-districts.geojson"
    org_units = gpd.read_file(geojson_file)
    bbox = org_units.total_bounds

    # start/end dates
    start = '2025-01'
    end = '2025-12'

    # download
    paths = clms_water.monthly.download(start, end, bbox, dirname=dirname, prefix=prefix, 
                                        overwrite=True)
    logging.info(paths)
    assert len(paths) == 12

    # test opening multifile xarray
    ds = xr.open_mfdataset(paths) #, mask_and_scale=False)
    logging.info(ds)

    # test visualize
    logging.info(bbox)
    da = ds['wb100_wb'].coarsen(x=5, y=5, boundary="trim").max()
    fig = da.plot(col='time', col_wrap=3).fig
    fig.savefig(dirname / 'clms_water.png', dpi=300)


@pytest.mark.integration
def test_download_yearly_modis_landcover():
    # download args
    dirname = DATA_DIR / '../test_outputs/cdse'
    prefix = 'modis_landcover_sierra_leone'

    # get bbox
    geojson_file = DATA_DIR / "sierra-leone-districts.geojson"
    org_units = gpd.read_file(geojson_file)
    bbox = org_units.total_bounds

    # start/end dates
    start = '2023'
    end = '2024'

    # download
    paths = modis_landcover.yearly.download(start, end, bbox, dirname=dirname, prefix=prefix, 
                                            overwrite=True)
    logging.info(paths)
    assert len(paths) == 2

    # test opening multifile xarray
    ds = xr.open_mfdataset(paths) #, mask_and_scale=False)
    logging.info(ds)

    # test visualize
    # logging.info(bbox)
    # da = ds['wb100_wb'].coarsen(x=5, y=5, boundary="trim").max()
    # fig = da.plot(col='time', col_wrap=3).fig
    # fig.savefig(dirname / 'clms_water.png', dpi=300)


@pytest.mark.integration
def test_download_dekadal_clms_ndvi():
    # download args
    dirname = DATA_DIR / '../test_outputs/cdse'
    prefix = 'clms_ndvi_sierra_leone'

    # get bbox
    geojson_file = DATA_DIR / "sierra-leone-districts.geojson"
    org_units = gpd.read_file(geojson_file)
    bbox = org_units.total_bounds

    # start/end dates
    start = '2025-01'
    end = '2025-02'

    # download
    paths = clms_ndvi.dekadal.download(start, end, bbox, dirname=dirname, prefix=prefix, 
                                       overwrite=True)
    logging.info(paths)
    assert len(paths) == 2

    # test opening multifile xarray
    ds = xr.open_mfdataset(paths) #, mask_and_scale=False)
    logging.info(ds)

    # test visualize
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots()
    ds['ndvi'].isel(time=0).plot(ax=ax)
    fig.savefig(dirname / 'clms_ndvi.png', dpi=300)
