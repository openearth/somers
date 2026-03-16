# %%
import numpy as np
import pandas as pd
from pathlib import Path


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
basedir = Path(r"P:/11207812-somers-ontwikkeling/database_grondwater")
data_dir = Path(
    r"P:\broeikasgassen-veenweiden\Grondwater\grondwaterstandanalyse\data\4-output\Gecorrigeerde_grondwaterstanden\gecorrigeerd"
)
sites = ["ASD", "ROV", "VLI", "ALB", "ZEG"]

for site in sites:
    metadata = pd.read_excel(
        basedir.joinpath("NOBV", "overzicht_nobv_type1.xlsx"), index_col="naam_meetpunt"
    )
    metadata_site = metadata[
        metadata.index.str.contains(site) & ~metadata.index.str.contains("MP")
    ]

    mask_gwms = (metadata_site["Diver"] == "ja") & (
        metadata_site["categorie"] == "grondwaterpeil"
    )
    selection_gwms = metadata_site[mask_gwms]

    mask_swms = (metadata_site["Diver"] == "ja") & (
        metadata_site["categorie"] == "slootpeil"
    )
    selection_swms = metadata_site[mask_swms]

    if site == "ZEG":
        selection_gwms = selection_gwms[selection_gwms.index.str.contains("RF16")]

    for naam_meetpunt, metadata_meetpunt in selection_gwms.iterrows():
        print(metadata_meetpunt)
        fill_values = metadata_meetpunt.to_dict()
        new_lines = {
            "naam_meetpunt": naam_meetpunt,
            "gefundeerd (ja/nee)": "ja",
            "greppel aanwezig (ja/nee)": "nee",
            "drains aanwezig (ja/nee)": "nee",
            "WIS aanwezig (ja/nee)": fill_ja_nee(metadata_meetpunt["WIS afstand (m)"]),
            "greppelafstand (m)": np.nan,
            "greppeldiepte (m-mv)": np.nan,
            "drainafstand (m)": np.nan,
            "draindiepte (m-mv)": np.nan,
        }
        fill_values.update(new_lines)
        header = gwm_header_format.format(**fill_values)

        data = pd.read_csv(
            data_dir.joinpath(site, f"{naam_meetpunt}.csv"), parse_dates=["Datum"]
        )
        data.columns = ["datumtijd", "grondwaterstand (m NAP)"]
        data["grondwaterstand (m NAP)"] = data["grondwaterstand (m NAP)"] / 100
        data["datumtijd"] = data["datumtijd"].dt.strftime("%d-%m-%Y")

        # path_out = f'P:/11207812-somers-ontwikkeling/database_grondwater/handmatige_uitvraag_bestanden/NOBV/GWM_{naam_meetpunt}.txt'
        # with open(path_out, 'w') as fp:
        #     fp.write(header)

        # data.to_csv(path_out, mode='a', sep=';', index=False, header=False)

    for naam_meetpunt, metadata_meetpunt in selection_swms.iterrows():
        print(naam_meetpunt)
        fill_values = metadata_meetpunt.to_dict()
        fill_values.update({"naam_meetpunt": naam_meetpunt})
        header = swm_header_format.format(**fill_values)

        data = pd.read_csv(
            data_dir.joinpath(site, f"{naam_meetpunt}.csv"), parse_dates=["Datum"]
        )
        data.columns = ["datumtijd", "slootwaterstand (m NAP)"]
        data["slootwaterstand (m NAP)"] = data["slootwaterstand (m NAP)"] / 100
        data["datumtijd"] = data["datumtijd"].dt.strftime("%d-%m-%Y")

        # path_out = f'P:/11207812-somers-ontwikkeling/database_grondwater/handmatige_uitvraag_bestanden/NOBV/SWM_{naam_meetpunt}.txt'
        # with open(path_out, 'w') as fp:
        #     fp.write(header)

        # data.to_csv(path_out, mode='a', sep=';', index=False, header=False)

# %% regiodeal
data_dir = Path(
    r"N:\Projects\11206000\11206020\B. Measurements and calculations\Extensometers\Tijdreeks_analyse\data\2-interim"
)
sites = ["Berkenwoude", "Cabauw", "Bleskensgraaf", "Hazerswoude"]

for site in sites:
    metadata = pd.read_excel(
        basedir.joinpath("NOBV", "overzicht_nobv_type1.xlsx"), index_col="naam_meetpunt"
    )
    metadata_site = metadata[metadata.index.str.contains(site)]

    mask_gwms = (metadata_site["Diver"] == "ja") & (
        metadata_site["categorie"] == "grondwaterpeil"
    )
    selection_gwms = metadata_site[mask_gwms]

    mask_swms = (metadata_site["Diver"] == "ja") & (
        metadata_site["categorie"] == "slootpeil"
    )
    selection_swms = metadata_site[mask_swms]

    for naam_meetpunt, metadata_meetpunt in selection_gwms.iterrows():
        print(naam_meetpunt)
        fill_values = metadata_meetpunt.to_dict()
        new_lines = {
            "naam_meetpunt": naam_meetpunt,
            "gefundeerd (ja/nee)": "ja",
            "greppel aanwezig (ja/nee)": "nee",
            "drains aanwezig (ja/nee)": "nee",
            "WIS aanwezig (ja/nee)": fill_ja_nee(metadata_meetpunt["WIS afstand (m)"]),
            "greppelafstand (m)": np.nan,
            "greppeldiepte (m-mv)": np.nan,
            "drainafstand (m)": np.nan,
            "draindiepte (m-mv)": np.nan,
        }
        fill_values.update(new_lines)
        header = gwm_header_format.format(**fill_values)

        data = pd.read_csv(
            data_dir.joinpath(f"{naam_meetpunt}.csv"), parse_dates=["tijd"]
        )
        data.columns = ["datumtijd", "grondwaterstand (m NAP)"]
        data["datumtijd"] = data["datumtijd"].dt.strftime("%d-%m-%Y %H:%M:%S")

        path_out = f"P:/11207812-somers-ontwikkeling/database_grondwater/handmatige_uitvraag_bestanden/NOBV/GWM_{naam_meetpunt}.txt"
        with open(path_out, "w") as fp:
            fp.write(header)

        data.to_csv(path_out, mode="a", sep=";", index=False, header=False)

    for naam_meetpunt, metadata_meetpunt in selection_swms.iterrows():
        print(naam_meetpunt)
        fill_values = metadata_meetpunt.to_dict()
        fill_values.update({"naam_meetpunt": naam_meetpunt})
        header = swm_header_format.format(**fill_values)

        data = pd.read_csv(
            data_dir.joinpath(f"{naam_meetpunt}.csv"), parse_dates=["tijd"]
        )
        data.columns = ["datumtijd", "slootwaterstand (m NAP)"]
        data["datumtijd"] = data["datumtijd"].dt.strftime("%d-%m-%Y %H:%M:%S")

        path_out = f"P:/11207812-somers-ontwikkeling/database_grondwater/handmatige_uitvraag_bestanden/NOBV/SWM_{naam_meetpunt}.txt"
        with open(path_out, "w") as fp:
            fp.write(header)

        data.to_csv(path_out, mode="a", sep=";", index=False, header=False)
