import json
import logging
import os
from pathlib import Path
import tempfile
import zipfile

from ecmwf.datastores import Client
import xarray as xr

from ...utils import force_logging
from ....utils.types import DateLike, BBox

logger = logging.getLogger(__name__)
force_logging(logger)


# Internal function to fetch data from the CDS API
def save_years(client, save_path, years, months, bbox, variables, use_server_cache):
    """Download monthly era5-drought data"""

    # extract the coordinates from input bounding box
    xmin, ymin, xmax, ymax = map(float, bbox)

    # construct the query parameters
    # TODO: for now hardcoding to intermedate dataset type which allows us to get the most recent ~1 month delay
    # but only goes back a few years. if user can select consolidated dataset this would allow going back much further in time
    params = {
        "variable": variables,
        "accumulation_period": [ # getting all available periods by default
            "1",
            "3",
            "6",
            "12",
            "24",
            "36",
            "48"
        ],
        "version": "1_0",
        "product_type": ["reanalysis"],
        "dataset_type": "intermediate_dataset",
        "year": [str(year) for year in years],
        "month": [str(month).zfill(2) for month in months],
        "area": [ymax, xmin, ymin, xmax],  # notice how we reordered the bbox coordinate sequence
    }

    # if use_server_cache is False, add tiny numeric flag to invalidate request hash
    # see: https://forum.ecmwf.int/t/how-to-avoid-the-cds-cache-issue/905/2
    if not use_server_cache:
        unique_numeric_string = str(int(time.time()))
        params['nocache'] = unique_numeric_string

    # download the data
    logger.info("Downloading data from CDS API...")
    logger.info(f"Request parameters: \n{json.dumps(params)}")
    remote = client.submit(
        "derived-drought-historical-monthly",
        params
    )

    # download comes as zipfile with multiple nc files (accum periods, spi vs spei, etc)
    # save to temp dir, extract, and consolidate to single nc file
    with tempfile.TemporaryDirectory(delete=True) as tmpdir:
        # download zipfile
        zip_path = Path(tmpdir) / 'tempzip.zip'
        remote.download(zip_path)

        # extract all files to same folder
        with zipfile.ZipFile(zip_path) as archive:
            archive.extractall(tmpdir)

        # open all extracted nc files
        nc_paths = str(Path(tmpdir) / '*.nc')
        ds = xr.open_mfdataset(nc_paths)

        # save as single nc file and close file connections before cleanup
        ds.to_netcdf(save_path)
        ds.close()

# Public API to retrieve data for bbox between start and end date
def download(
    start: DateLike,
    end: DateLike,
    bbox: BBox,
    dirname: str,
    prefix: str,
    variables: list[str],
    use_server_cache: bool = True,
    overwrite: bool = False,
):
    """
    Retrieves ERA5-Drought monthly drought indices for a given bbox, variables, and start/end dates.
    Saves to disk in a single file for the entire period, as specified by dirname and prefix.
    Returns list with a single file path entry where data was downloaded, e.g. to open directly with xr.open_dataset().
    """
    os.makedirs(dirname, exist_ok=True)

    # Parse dates
    start_year = int(start)
    end_year = int(end)
    years = range(start_year, end_year+1)
    months = range(1, 12+1)

    # Create ecmwf client
    client = Client()
    client.check_authentication()

    # Determine the save path
    files = []
    save_file = f'{prefix}_{start_year}-{end_year}.nc'
    save_path = (Path(dirname) / save_file).resolve()
    files.append(save_path)

    # Download or use existing file
    if overwrite is False and save_path.exists():
        # File already exist, load from file instead
        logger.info(f'File already downloaded: {save_path}')
    
    else:
        # Submit job request and save results to save_path
        save_years(client=client, save_path=save_path, years=years, months=months, bbox=bbox, variables=variables, use_server_cache=use_server_cache)

    # return list of all file downloads
    return files
