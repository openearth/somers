# %%
import time

import numpy as np
import pandas as pd
import geopandas as gpd
from pathlib import Path

start = time.time()


def fill_ja_nee(value):
    if not np.isnan(value):
        return "ja"
    else:
        return "nee"


LOCATION_FULLNAMES_NOBV = {
    "ALB": "Aldeboarn",
    "ASD": "Assendelft",
    "ROV": "Rouveen",
    "VLI": "Vlist",
    "ZEG": "Zegveld",
    "LYD": "Lytse Deelen",
}

gwm_header_format = (
    "# naam_meetpunt: {naam_meetpunt}\n"
    "# x-coor: {x-coor}\n"
    "# y-coor: {y-coor}\n"
    "# maaiveld (m NAP): {maaiveld (m NAP)}\n"
    "# top filter (m-mv): {top filter (m-mv)}\n"
    "# onderkant filter (m-mv): {onderkant filter (m-mv)}\n"
    "# gefundeerd (ja/nee): {gefundeerd (ja/nee)}\n"
    "# slootafstand (m): {slootafstand (m)}\n"
    "# zomer streefpeil (m NAP): {zomer streefpeil (m NAP)}\n"
    "# winter streefpeil (m NAP): {winter streefpeil (m NAP)}\n"
    "# greppel aanwezig (ja/nee): {greppel aanwezig (ja/nee)}\n"
    "# drains aanwezig (ja/nee): {drains aanwezig (ja/nee)}\n"
    "# WIS aanwezig (ja/nee): {WIS aanwezig (ja/nee)}\n"
    "# greppelafstand (m): {greppelafstand (m)}\n"
    "# greppeldiepte (m-mv): {greppeldiepte (m-mv)}\n"
    "# drainafstand (m): {drainafstand (m)}\n"
    "# draindiepte (m-mv): {draindiepte (m-mv)}\n"
    "# WIS afstand (m): {WIS afstand (m)}\n"
    "# WIS diepte (m-mv): {WIS diepte (m-mv)}\n"
    "*\n"
    "> datumtijd (dd-mm-yyyy / dd-mm-yyyy HH:MM:SS)\n"
    "> grondwaterstand (m NAP)\n"
)

swm_header_format = (
    "# naam_meetpunt: {naam_meetpunt}\n"
    "# x-coor: {x-coor}\n"
    "# y-coor: {y-coor}\n"
    "*\n"
    "> datumtijd (dd-mm-yyyy / dd-mm-yyyy HH:MM:SS)\n"
    "> slootwaterstand (m NAP)\n"
)

# %%
basedir = Path(r"p:/11207812-somers-ontwikkeling/3-somers_development/QSOMERS")
data_dir = Path(
    r"p:/broeikasgassen-veenweiden/Grondwater/grondwaterstandanalyse/data/4-output/Gecorrigeerde_grondwaterstanden_hourly/gecorrigeerd"
)
# sites = ["ASD", "ROV", "VLI", "ALB", "ZEG", "LYD"]
sites = ["LYD"]

folder_names = {
    "ASD": "ASD",
    "ROV": "ROU",
    "VLI": "VLI",
    "ALB": "ALB",
    "ZEG": "ZEG",
    "LYD": "LYD",
}

metadata = pd.read_excel(
    basedir.joinpath("NOBV", "2026", "overzicht_nobv_2026.xlsx"),
    index_col="naam_meetpunt",
)

metadata_nobv = metadata[metadata.index.str.contains("|".join(sites))]

metadata_nobv_gpd = gpd.GeoDataFrame(
    metadata_nobv,
    geometry=gpd.points_from_xy(
        metadata_nobv["x-coor"],
        metadata_nobv["y-coor"],
    ),
    crs="EPSG:28992",
)

path_output = "p:/11207812-somers-ontwikkeling/3-somers_development/QSOMERS/NOBV/2026/Lytse_Deelen_wells.geoparquet"
metadata_nobv_gpd.to_parquet(path_output)
