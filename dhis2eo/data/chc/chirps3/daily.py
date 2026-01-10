import logging
from datetime import date, datetime, timedelta
from typing import Iterable, List, Tuple, Union

from ....utils.time import iter_days, ensure_date
from ....utils.types import BBox, DateLike

import numpy as np
import xarray as xr
import rioxarray

from ...utils import netcdf_cache, force_logging

# -----------------------------------------------------------------------------
# Logging setup
# -----------------------------------------------------------------------------
# Use module-level logger and force logging on so progress messages
# are visible when running in notebooks or scripts.
logger = logging.getLogger(__name__)
force_logging(logger)

# -----------------------------------------------------------------------------
# Defaults
# -----------------------------------------------------------------------------

# CHIRPS v3 "stage":
# - "final": finalized, stable product (recommended for analysis)
# - "prelim": near-real-time preliminary product
DEFAULT_STAGE = "final"

# CHIRPS v3 "flavor":
# - For final: "rnl" or "sat"
# - For prelim: directory is "sat" but filename tag is "prelim"
DEFAULT_FLAVOR = "rnl"

# -----------------------------------------------------------------------------
# URL construction (provider-specific logic)
# -----------------------------------------------------------------------------

def url_for_day(
    d: DateLike,
    stage: str = DEFAULT_STAGE,
    flavor: str = DEFAULT_FLAVOR,
) -> str:
    """
    Construct the CHC URL for a single CHIRPS v3 daily GeoTIFF.
    Directory and filename conventions on CHC:
    FINAL:
      /products/CHIRPS/v3.0/daily/final/{rnl|sat}/{YYYY}/
      chirps-v3.0.{rnl|sat}.{YYYY}.{MM}.{DD}.tif
    PRELIM:
      /products/CHIRPS/v3.0/daily/prelim/sat/{YYYY}/
      chirps-v3.0.prelim.{YYYY}.{MM}.{DD}.tif
    """
    dd = ensure_date(d)

    if stage not in {"final", "prelim"}:
        raise ValueError("stage must be 'final' or 'prelim'")

    # Final products: both rnl and sat exist
    if stage == "final":
        if flavor not in {"rnl", "sat"}:
            raise ValueError("For stage='final', flavor must be 'rnl' or 'sat'")
        return (
            "https://data.chc.ucsb.edu/products/CHIRPS/v3.0/daily/final/"
            f"{flavor}/{dd.year}/chirps-v3.0.{flavor}.{dd.year}.{dd.month:02d}.{dd.day:02d}.tif"
        )

    # Preliminary products: directory is sat/, filename uses "prelim"
    if flavor != "sat":
        raise ValueError("For stage='prelim', flavor must be 'sat'")
    return (
        "https://data.chc.ucsb.edu/products/CHIRPS/v3.0/daily/prelim/sat/"
        f"{dd.year}/chirps-v3.0.prelim.{dd.year}.{dd.month:02d}.{dd.day:02d}.tif"
    )

# -----------------------------------------------------------------------------
# Public API: download and stack daily CHIRPS into an xarray Dataset
# -----------------------------------------------------------------------------

#@netcdf_cache()
def get(
    start: DateLike,
    end: DateLike,
    bbox: BBox,
    stage: str = DEFAULT_STAGE,
    flavor: str = DEFAULT_FLAVOR,
    var_name: str = "precip",
) -> xr.Dataset:
    """
    Fetch CHIRPS v3 daily precipitation for the given date range and bbox.
    Returns an xarray.Dataset with:
    - one variable: `var_name` (default: precip)
    - dimensions: (time, y, x)
    """
    start_d = ensure_date(start)
    end_d = ensure_date(end)
    if end_d < start_d:
        raise ValueError("end must be on/after start")

    logger.info(f"Fetching CHIRPS v3 daily from {start_d} to {end_d} (inclusive)")
    logger.info(f"Stage/flavor: {stage}/{flavor}")
    logger.info(f"BBox: {bbox}")

    das = []
    times = []

    # Loop over days, read each bbox window, and collect 
    xmin, ymin, xmax, ymax = bbox
    for d in iter_days(start_d, end_d):
        
        # Get file url based on the day
        url = url_for_day(d, stage=stage, flavor=flavor)
        logger.info(f"Reading {d} -> {url}")
        
        # Connect to global dataset lazily
        da = rioxarray.open_rasterio(
            url,
            chunks={'x': 1024, 'y': 1024}  # lazy Dask arrays
        )
        
        # Read only the bbox window
        da = da.rio.clip_box(minx=xmin, miny=ymin, maxx=xmax, maxy=ymax)
        
        # Ensure nodata value is masked and added to metadata
        nodata = -9999.0 # this should be the chirps3 nodata value
        da = da.where(da != nodata) # this adds nans where nodata for plotting
        da.rio.write_nodata(nodata, encoded=True, inplace=True) # should write to metadata for future saving

        # Collect data array and time extracted from filename
        das.append(da)
        times.append(np.datetime64(d))

    # Stack daily slices along the time dimension
    stacked = xr.concat(das, dim=xr.IndexVariable("time", times))
    ds = stacked.to_dataset(name=var_name)

    # Light, CF-ish metadata for downstream use
    ds[var_name].attrs.setdefault("long_name", "Precipitation")
    ds[var_name].attrs.setdefault("units", "mm/day")
    ds.attrs["bbox"] = bbox
    ds.attrs["dataset"] = "CHIRPS v3 (daily)"
    ds.attrs["stage"] = stage
    ds.attrs["flavor"] = flavor

    return ds