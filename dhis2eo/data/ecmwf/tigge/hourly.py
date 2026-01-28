
def fetch_month(year, month, bbox, variables):
    # FINAL WORKING PARAMS
    variable_code_string = "167"  # need a way to map these
    xmin,ymin,xmax,ymax = bbox
    out_file = f"tigge-{year}-{str(month).zpad(2)}.grib"
    params = {
        "class": "ti",             # TIGGE class
        "dataset": "tigge",        # Dataset identifier
        #"date": "2023-10-01",      # YYYY-MM-DD
        "data": "20200101/to/20200130",
        "expver": "prod",          # Production version
        "grid": "0.2/0.25",         # Default and best is 0.5 grid resolution for ecmwf origin
        "area": [ymax, xmin, ymin, xmax], #"50/10/40/20",  # N/W/S/E (subsetting coordinates)
        "levtype": "sfc",          # Surface level
        "origin": "ecmf",          # Forecasting center (ECMWF)
        "param": variable_code_string,            # Parameter code (e.g., 2m temperature)
        "time": "00",           # Forecast base times
        "step": "0/6/12/18/24/30/36/42/48/54/60/66/72/78/84/90/96/102/108/114/120/126/132/138/144/150/156/162/168/174/180/186/192/198/204/210/216/222/228/234/240/246/252/258/264/270/276/282/288/294/300/306/312/318/324/330/336/342/348/354/360",
        "type": "cf",              # control forecast (best estimate)
        "target": out_file,
    }

    from ecmwfapi import ECMWFDataServer
    server = ECMWFDataServer()
    server.retrieve(params)
