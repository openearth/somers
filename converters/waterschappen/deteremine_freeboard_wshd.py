import geopandas as gpd
from shapely.geometry import Point

path = r"p:/11207812-somers-ontwikkeling/1-data/1-external/Peilbestanden/versie 2020/Peilgebieden_all_zp_wp_single.shp"

peilgebieden = gpd.read_file(path)

x = 93128.8
y = 419435.1
point = Point(x, y)

peilgebied_mask = peilgebieden.geometry.contains(point)

peilgebied = peilgebieden[peilgebied_mask]
