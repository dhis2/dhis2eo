import json
import logging
import os
import tempfile
import zipfile
import shutil
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

from ecmwf.datastores import Client
import xarray as xr

from ...utils import force_logging
from ....utils.types import DateLike, BBox

logger = logging.getLogger(__name__)
force_logging(logger)


# Internal function to fetch data from the CDS API
def save_year(client, save_path, year, bbox, use_server_cache):
    """Download single year of esa landcover data"""

    # extract the coordinates from input bounding box
    xmin, ymin, xmax, ymax = map(float, bbox)

    # construct the query parameters
    if year <= 2015:
        version = "v2_0_7cds"
    else:
        version = "v2_1_1"
    params = {
        "variable": "all",
        "year": [str(year)],
        "version": [version],
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
        "satellite-land-cover",
        params
    )

    # Wait for results and save to target path
    with tempfile.TemporaryDirectory(delete=True) as tmpdir:
        # download zipfile to temporary folder
        temp_path = Path(tmpdir) / 'temp_zip.zip'
        remote.download(temp_path)

        # extract the only file from zipfile to save_path
        with zipfile.ZipFile(temp_path) as archive:
            first_name = archive.namelist()[0]
            with archive.open(first_name) as source, open(save_path, 'wb') as target:
                shutil.copyfileobj(source, target)


# Public API to retrieve data for bbox between start and end date
def download(
    start: DateLike,
    end: DateLike,
    bbox: BBox,
    dirname: str,
    prefix: str,
    use_server_cache: bool = True,
    overwrite: bool = False,
):
    """
    Retrieves ESA CCI yearly land cover data for a given bbox, variables, and start/end dates.
    Saves to disk as yearly files, as specified by dirname and prefix.
    Returns list with all file paths where data was downloaded, e.g. to open directly with xr.open_mfdataset().
    """
    os.makedirs(dirname, exist_ok=True)

    # Parse dates
    start_year = int(start)
    end_year = int(end)
    years = list(range(start_year, end_year+1))

    # Create ecmwf client
    client = Client()
    client.check_authentication()

    # Create multithread downloader
    max_threads = 10
    downloader = ThreadPoolExecutor(max_workers=max_threads)

    # Loop and download each year
    files = []
    for year in years:
        logger.info(f'Year {year}')

        # Determine the save path
        save_file = f'{prefix}_{year}.nc'
        save_path = (Path(dirname) / save_file).resolve()
        files.append(save_path)

        # Download or use existing file
        if overwrite is False and save_path.exists():
            # File already exist, load from file instead
            logger.info(f'File already downloaded: {save_path}')
        
        else:
            # Submit job request
            #save_year(client=client, save_path=save_path, year=year, bbox=bbox, use_server_cache=use_server_cache)
            downloader.submit(save_year, client, save_path, year, bbox, use_server_cache)

    # Wait
    downloader.shutdown(wait=True)

    # return list of all file downloads
    return files
