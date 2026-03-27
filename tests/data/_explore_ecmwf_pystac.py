# this script is just to explore the azure pystac catalog of ecmwf opendata
# to see if i could find any mmsf stream data
# but 0 were found

import pystac_client
import planetary_computer

catalog = pystac_client.Client.open(
    "https://planetarycomputer.microsoft.com/api/stac/v1",
    modifier=planetary_computer.sign_inplace,
)
search = catalog.search(
    collections=["ecmwf-forecast"],
    query={
        "ecmwf:stream": {"eq": "mmsf"},
        #"ecmwf:type": {"eq": "fc"},
        #"ecmwf:step": {"eq": "0h"},
    },
)
items = search.get_all_items()
print(len(items))