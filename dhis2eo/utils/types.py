from datetime import datetime, date

# Bounding box type:
# (min_lon, min_lat, max_lon, max_lat) in EPSG:4326 (lon/lat)
BBox = tuple[float, float, float, float]

# Datelike type:
DateLike = str | date | datetime
