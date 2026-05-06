import logging
import calendar
import sys

import xarray as xr

logger = logging.getLogger(__name__)


def force_logging(logger):
    # Since data modules are so download centric, force all info logs to be printed
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(levelname)s - %(asctime)s - %(name)s - %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    #logger.propagate = False # preserves this logging format so that the caller doesn't override with its own logging format


force_logging(logger)


def open_zarr(zarr_path, variables=None):
    # open with xarray
    logger.info(f'Opening zarr archive from {zarr_path}')
    ds = xr.open_dataset(
        zarr_path,
        storage_options={"client_kwargs":{"trust_env":True}},
        chunks={},
        engine="zarr",
    )

    # subset to only the specified variables
    if variables is not None:
        ds = ds[variables]

    return ds


def _ordered_slice(coord, v1, v2):
    """Return a slice with bounds ordered to match the coordinate direction."""
    ascending = coord.values[0] < coord.values[-1]
    lo, hi = min(v1, v2), max(v1, v2)
    return slice(lo, hi) if ascending else slice(hi, lo)


def select_ds_region(ds, bbox, x_dim, y_dim, fix_360_longitude=False):
    # convert ds longitudes from 0 to 360 to -180 to 180
    if fix_360_longitude:
        logger.info('Correcting longitude coords to range -180 to 180')
        ds = ds.assign_coords(**{x_dim: ((ds[x_dim] + 180) % 360 - 180)}).sortby(x_dim)

    # extract the coordinates from input bounding box
    logger.info(f'Computing bbox to use for subsetting')
    xmin, ymin, xmax, ymax = map(float, bbox)

    # add padding to include edge pixels
    xres = abs(float(ds[x_dim].diff(x_dim).median()))
    yres = abs(float(ds[y_dim].diff(y_dim).median()))
    xmin, xmax = xmin - xres, xmax + xres
    ymin, ymax = ymin - yres, ymax + yres

    # filter to bbox coords
    # note that we have to ensure we use same slice order as coordinate direction
    logger.info(f'Subsetting zarr archive to bbox with padding: {xmin} {ymin} {xmax} {ymax}')
    ds = ds.sel(**{
        x_dim: _ordered_slice(ds[x_dim], xmin, xmax), 
        y_dim: _ordered_slice(ds[y_dim], ymin, ymax),
    })

    return ds


def select_ds_month(ds, year, month, time_dim):
    # turn time query parameters into xarray selection
    _, last_day = calendar.monthrange(year, month)
    from_date = f"{year}-{month:02d}-01"
    to_date = f"{year}-{month:02d}-{last_day:02d}"
    
    logger.info(f'Subsetting zarr archive to date range: {from_date} to {to_date}')
    ds = ds.sel(**{time_dim: slice(from_date, to_date)})

    return ds
