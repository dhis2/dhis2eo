from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from collections.abc import Iterable

import xarray as xr
import s3fs

# IMPORTANT: this import registers the xarray backend
import omfiles

from ....utils.types import BBox
from ....utils.time import iter_days


def open_ifs_file(
    fs: s3fs.S3FileSystem,
    day: date,
    variable: str,
    chunks: dict | None = None,
) -> xr.Dataset:
    year = day.year
    datestr = day.strftime("%Y-%m-%d")

    # Open-Meteo OM layout (historical)
    if day.year > 2022:
        raise NotImplementedError('Dates after 2022 not yet supported')
    
    elif day.year >= 2017:
        s3_path = (
            f"s3://openmeteo/data/ecmwf_ifs/"
            f"{variable}/year_{year}.om"
        )
    
    else:
        raise ValueError('IFS forecast data not available prior to 2017')

    return xr.open_dataset(
        fs.open(s3_path),
        engine="om",
        chunks=chunks or {},
    )


# def open_ifs_day(
#     fs: s3fs.S3FileSystem,
#     day: date,
#     variable: str,
#     chunks: dict | None = None,
# ) -> xr.Dataset:
#     """
#     Open a single-day ECMWF IFS 9 km OM file lazily from S3.
#     """
#     ds = open_ifs_file(
#         fs=fs, 
#         day=day, 
#         variable=variable,
#         chunks=chunks,
#     )
#     return ds.sel(time=day)


def download(
    start_date: str,
    end_date: str,
    bbox: BBox,
    dirname: str,
    prefix: str,
    variables: list[str],
    chunks: dict | None = None,
) -> list[str]:
    """
    Download ECMWF IFS 9 km data for a bounding box and time range
    using OM files + xarray lazy slicing.

    Parameters
    ----------
    start_date : str
        ISO date (YYYY-MM-DD)
    end_date : str
        ISO date (YYYY-MM-DD)
    bbox : (south, west, north, east)
        Geographic bounding box in degrees
    out_path : str
        Output file or directory
    variables : iterable of str
        OM variable names (e.g. temperature_2m, precipitation)
    chunks : dict
        Optional xarray/dask chunking (e.g. {"time": 24})
    output_format : "netcdf" | "zarr"

    Returns
    -------
    xr.Dataset
    """

    south, west, north, east = bbox

    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)

    fs = s3fs.S3FileSystem(anon=True)

    datasets = []

    for var in variables:
        var_datasets = []

        for day in iter_days(start, end):
            ds = open_ifs_day(
                fs=fs,
                day=day,
                variable=var,
                chunks=chunks,
            )

            # Spatial subsetting happens lazily here
            ds = ds.sel(
                latitude=slice(south, north),
                longitude=slice(west, east),
            )

            var_datasets.append(ds)

        # concatenate time slices for this variable
        var_ds = xr.concat(var_datasets, dim="time")
        datasets.append(var_ds)

    # merge all variables
    out_ds = xr.merge(datasets)

    # trigger actual S3 range reads here
    out_ds = out_ds.load()

    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    if output_format == "netcdf":
        out_ds.to_netcdf(out_path)
    elif output_format == "zarr":
        out_ds.to_zarr(out_path, mode="w")
    else:
        raise ValueError("output_format must be 'netcdf' or 'zarr'")

    return out_ds
