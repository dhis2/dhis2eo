'''
Shared utils for getting data from the ecmwf-opendata library
'''

import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from concurrent.futures import ThreadPoolExecutor

import xarray as xr


# Hack to prevent progress bars for each file download
# This has to happen before importing ecmwf.opendata
import tqdm
import tqdm.notebook
import tqdm.auto
def disabled_tqdm(*args, **kwargs):
    kwargs['disable'] = True
    return tqdm.std.tqdm(*args, **kwargs)
tqdm.tqdm = disabled_tqdm
tqdm.notebook.tqdm = disabled_tqdm
tqdm.auto.tqdm = disabled_tqdm
logging.getLogger("multiurl").setLevel(logging.WARNING)


##########
# Helpers for HRES (deterministic opendata forecasts)

# Internal function to fetch a single forecast step file download (API only allows one forecast step at a time)
def fetch_hres_forecast_step(client, day, step, variables, dirname):    
    # forecasts are actually available every 6 hours, but for simplicity we only return the midnight one
    run_time = 0

    # download to temp file
    temp_file = Path(dirname) / f'file_{day}-{step}.grib2'
    params = dict(
        date=day,
        time=run_time,
        stream="oper",
        type="fc",
        step=step,
        param=variables,
        target=temp_file,
    )
    client.retrieve(params)

    # return
    return temp_file


# Internal function to fetch all forecast steps for a single day
def fetch_hres_day(client, model, day, bbox, variables):
    if model == 'ifs':
        # define the full range of forecast steps: 0 to 144 hours every 3h, then 144 to 360 every 6h
        steps = list(range(0, 144 + 3, 3)) + list(range(150, 360 + 6, 6))
    elif model == 'aifs-single':
        # define the full range of forecast steps: 0 to 360 every 6h
        steps = list(range(0, 360 + 6, 6))
    else:
        raise ValueError(f'Model must be ifs or aifs-single, not {model}')

    # downloads go inside temp dir which handles the cleanup of ALL files generated inside it
    with TemporaryDirectory(delete=True) as tmpdir:

        # fetch each step efficiently using a threadpool executor
        max_threads = 10
        with ThreadPoolExecutor(max_workers=max_threads) as downloader:
            files = list(downloader.map(
                lambda step: fetch_hres_forecast_step(client, day, step, variables, dirname=tmpdir),
                steps,
            ))

        # lazy open all global files
        with xr.open_mfdataset(
            files, 
            combine='nested', 
            concat_dim='step', 
            coords='minimal',  # Explicitly set this to opt-in and silence future change warning
            compat='override'  # Highly recommended for GRIB to skip tiny float comparisons
        ) as ds:
            # extract only the bbox and load to memory
            xmin,ymin,xmax,ymax = map(float, bbox)
            crop = ds.sel(longitude=slice(xmin, xmax), latitude=slice(ymax, ymin))
            crop.load()

    # force 'time' to be a dimension even if it only has 1 value
    crop = crop.expand_dims("time")

    # return
    return crop
