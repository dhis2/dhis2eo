import logging
import os
import calendar
from pathlib import Path
from datetime import date, timedelta

from ....utils.time import iter_days, iter_months, ensure_date, months_ago
from ....utils.types import BBox, DateLike

import numpy as np
import xarray as xr
import rioxarray

from ...utils import force_logging

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
# Internal logic and helpers
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

def fetch_day(day, bbox, var_name, stage, flavor):
    # Get file url based on the day
    url = url_for_day(day, stage=stage, flavor=flavor)
    logger.debug(f"Reading {day} -> {url}")
    
    # Connect to global dataset lazily
    da = rioxarray.open_rasterio(
        url,
        chunks=None, # disable dask, not needed and actually slows things down
    )
    
    # Read only the bbox window
    xmin, ymin, xmax, ymax = bbox
    da = da.rio.clip_box(minx=xmin, miny=ymin, maxx=xmax, maxy=ymax)
    
    # Ensure nodata value is masked and added to metadata
    nodata = -9999.0 # this should be the chirps3 nodata value
    da = da.where(da != nodata) # this adds nans where nodata for plotting
    da.rio.write_nodata(nodata, encoded=True, inplace=True) # should write to metadata for future saving

    # Convert to dataset
    ds = da.to_dataset(name=var_name)

    # Remove unnecessary band dim
    ds = ds.squeeze("band", drop=True)

    # Add day constant
    ds = ds.expand_dims(time=[np.datetime64(day)])

    # Light, CF-ish metadata for downstream use
    ds[var_name].attrs.setdefault("long_name", "Precipitation")
    ds[var_name].attrs.setdefault("units", "mm/day")
    ds.attrs["bbox"] = bbox
    ds.attrs["dataset"] = "CHIRPS v3 (daily)"
    ds.attrs["stage"] = stage
    ds.attrs["flavor"] = flavor

    # Return
    return ds

def fetch_month(year, month, bbox, var_name, stage, flavor):
    # Determine start and end date for the month
    _, days_in_month = calendar.monthrange(year, month)
    start_day = date(year=year, month=month, day=1)
    end_day = date(year=year, month=month, day=days_in_month)

    # Loop and fetch data for all days in the month
    logger.info("Extracting and combining daily data from CHC servers...")
    ds_list = []
    for day in iter_days(start_day, end_day):
        day_ds = fetch_day(day, bbox, var_name, stage, flavor)
        ds_list.append(day_ds)
    
    # Merge all day ds into a single month ds
    ds = xr.concat(ds_list, dim='time')

    # Return
    return ds

# -----------------------------------------------------------------------------
# Public API: download and stack daily CHIRPS into an xarray Dataset
# -----------------------------------------------------------------------------

# Public API to retrieve data for bbox between start and end date
def download(
    start: DateLike,
    end: DateLike,
    bbox: BBox,
    dirname: str, 
    prefix: str, 
    skip_existing=True,
    stage: str = DEFAULT_STAGE,
    flavor: str = DEFAULT_FLAVOR,
    var_name: str = "precip",
):
    """
    Retrieves CHIRPS v3 daily precipitation for the given date range and bbox.
    Saves to disk in monthly files, as specified by dirname and prefix.
    Returns list of file paths where data was downloaded, e.g. to use with xr.open_mfdataset().
    """
    os.makedirs(dirname, exist_ok=True)

    start_year, start_month = map(int, start.split('-')[:2])
    end_year, end_month = map(int, end.split('-')[:2])

    logger.info(f"Fetching CHIRPS v3 daily from {start_year}-{start_month} to {end_year}-{end_month} (inclusive)")
    logger.info(f"Stage/flavor: {stage}/{flavor}")
    logger.info(f"BBox: {bbox}")

    # Determine last date for which we can expect CHIRPS v3 to be complete
    # CHIRPS v3 seems to be released in complete months after the 20th of the following month
    # Meaning only after the 20th of a new month, can we expect that the previous month has been released
    current_date = date.today()
    last_month = months_ago(current_date, 1)
    month_before_last = months_ago(current_date, 2)
    last_updated_month = last_month if current_date.day > 20 else month_before_last

    # The last update date for 'prelim' stage data has different more complex rules
    # But in practice it seems it follows the same rule after the 20th
    # TODO: We may have to revisit this and implement the correct rule for 'prelim' data
    # For now, adding a warning for prelim data
    if stage == 'prelim':
        logger.warning(
            "CHIRPS 'prelim' data for the last updated month may not follow the usual update rules. "
            "Current logic assumes updates follow the same rule after the 20th of the month. "
        )

    # Loop over months
    files = []
    for year, month in iter_months(start_year, start_month, end_year, end_month):
        logger.info(f'Month {year}-{month}')

        # Skip if month is not expected to be published
        if (year,month) > (last_updated_month.year, last_updated_month.month):
            logger.warning(f'Skipping downloads for months that have not been published yet (after 20th of the following month).')
            logger.warning(f'Last available month in CHIRPS v3: {last_updated_month.isoformat()[:7]}')
            continue

        # Determine the save path
        save_file = f'{prefix}_{year}-{str(month).zfill(2)}.nc'
        save_path = (Path(dirname) / save_file).resolve()
        files.append(save_path)

        # Download or use existing file
        if skip_existing and save_path.exists():
            # File already exist, load from file instead
            logger.info(f'File already downloaded: {save_path}')
        
        else:
            # Download the data
            ds = fetch_month(year, month, bbox, var_name, stage, flavor)

            # Save to target path
            ds.to_netcdf(save_path)

    # return list of all file downloads
    return files
