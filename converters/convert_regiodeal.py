# %%
import time

import numpy as np
import pandas as pd
from pathlib import Path

start = time.time()


def fill_ja_nee(value):
    if not np.isnan(value):
        return "ja"
    else:
        return "nee"


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

metadata = pd.read_excel(
    basedir.joinpath("NOBV", "2026", "overzicht_nobv_2026.xlsx"),
    index_col="naam_meetpunt",
)

# %% regiodeal
data_dir = Path(
    r"p:/11207812-somers-ontwikkeling/3-somers_development/QSOMERS/Regiodeal/data/2-output"
)
sites = ["Berkenwoude", "Cabauw", "Bleskensgraaf", "Hazerswoude"]

metadata_regiodeal = metadata[metadata.index.str.contains("|".join(sites))]

mask_gwms = (metadata_regiodeal["Diver"] == "ja") & (
    metadata_regiodeal["categorie"] == "grondwaterpeil"
)
selection_gwms = metadata_regiodeal[mask_gwms]

mask_swms = (metadata_regiodeal["Diver"] == "ja") & (
    metadata_regiodeal["categorie"] == "slootpeil"
)
selection_swms = metadata_regiodeal[mask_swms]

for site in sites:

    print(f"Analyzing {site}")

    metadata_site_gwms = selection_gwms[selection_gwms.index.str.contains(site)]
    metadata_site_swms = selection_swms[selection_swms.index.str.contains(site)]

    well_names_gwm = metadata_site_gwms.index.tolist()
    well_names_swm = metadata_site_swms.index.tolist()

    if site == "Hazerswoude":
        location_fullname = "Hazerswoude-dorp"
    else:
        location_fullname = site

    for well_name_gwm in well_names_gwm:
        selection_gwms_single_well = metadata_site_gwms[
            metadata_site_gwms.index == well_name_gwm
        ]

        metadata_single_well = selection_gwms_single_well.squeeze(axis=0)

        fill_values = metadata_single_well.to_dict()
        new_lines = {
            "naam_meetpunt": well_name_gwm,
            "gefundeerd (ja/nee)": "ja",
            "greppel aanwezig (ja/nee)": "nee",
            "drains aanwezig (ja/nee)": "nee",
            "WIS aanwezig (ja/nee)": fill_ja_nee(
                metadata_single_well["WIS afstand (m)"]
            ),
            "greppelafstand (m)": np.nan,
            "greppeldiepte (m-mv)": np.nan,
            "drainafstand (m)": np.nan,
            "draindiepte (m-mv)": np.nan,
        }
        fill_values.update(new_lines)
        header = gwm_header_format.format(**fill_values)

        data = pd.read_csv(
            data_dir.joinpath(
                f"{location_fullname}_gwlevel.csv",
            ),
            parse_dates=["tijd"],
        )
        data.columns = ["datumtijd", "grondwaterstand (m NAP)"]
        data["grondwaterstand (m NAP)"] = data["grondwaterstand (m NAP)"] / 100
        data["datumtijd"] = data["datumtijd"].dt.strftime("%d-%m-%Y %H:%M:%S")

        path_out = f"p:/11207812-somers-ontwikkeling/3-somers_development/QSOMERS/Regiodeal/2026/Grondwaterreeksen/GWM_{well_name_gwm}.txt"
        with open(path_out, "w") as fp:
            fp.write(header)

        data.to_csv(path_out, mode="a", sep=";", index=False, header=False)

    for well_name_swm in well_names_swm:
        selection_swms_single_well = metadata_site_swms[
            metadata_site_swms.index == well_name_swm
        ]

        metadata_single_well = selection_swms_single_well.squeeze(axis=0)

        fill_values = metadata_single_well.to_dict()
        fill_values.update({"naam_meetpunt": well_name_swm})
        header = swm_header_format.format(**fill_values)

        data = pd.read_csv(
            data_dir.joinpath(
                f"{location_fullname}_ditch_level.csv",
            ),
            parse_dates=["tijd"],
        )

        data.columns = ["datumtijd", "slootwaterstand (m NAP)"]
        data["slootwaterstand (m NAP)"] = data["slootwaterstand (m NAP)"] / 100
        data["datumtijd"] = data["datumtijd"].dt.strftime("%d-%m-%Y %H:%M:%S")

        path_out = f"p:/11207812-somers-ontwikkeling/3-somers_development/QSOMERS/Regiodeal/2026/Grondwaterreeksen/SWM_{well_name_swm}.txt"
        with open(path_out, "w") as fp:
            fp.write(header)

        data.to_csv(path_out, mode="a", sep=";", index=False, header=False)

end = time.time()
print(f"Elapsed time: {end - start} seconds.")
