from pathlib import Path
from glob import glob
import subprocess

import pandas as pd
import numpy as np


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

cols = [
    "Code peilbuis",
    "x",
    "y",
    "Maaiveldhoogte (mNAP)",
    "filterstelling (m.tov. Maaiveld)",
    "maaiveldhoogte",
]

selectie = pd.read_excel(
    r"p:\11207812-somers-ontwikkeling\3-somers_development\QSOMERS\Handmatige uitvraag 2024\handmatige_uitvraag_bestanden\HunzeenAas\veenperceel_selectie_HenA.xlsx"
)
name = [x for x in selectie["Code peilbuis"]]

selectie = selectie[cols]
selectie[["top filter (m-mv)", "onderkant filter (m-mv)"]] = selectie[
    "filterstelling (m.tov. Maaiveld)"
].str.split(" - ", expand=True)
selectie["top filter (m-mv)"] = (
    selectie["top filter (m-mv)"].str.replace(",", ".").astype(float)
)
selectie["onderkant filter (m-mv)"] = (
    selectie["onderkant filter (m-mv)"].str.replace(",", ".").astype(float)
)

basedir = Path(
    "p:/11207812-somers-ontwikkeling/3-somers_development/QSOMERS/Handmatige uitvraag 2026/handmatige_uitvraag_bestanden/HunzeEnAas"
)

# code peilbuis is name
# uiteindelijk moet GWM toegevoegd worden aan de name
# Selecteren welke kolom overeenkomt met wat
# filterstelling (m tov Maaiveld) opsplitsen in top en bottom filter
# data_dir = Path(
#     r"P:\11207812-somers-ontwikkeling\database_grondwater\handmatige_uitvraag_bestanden\HunzeenAas\Export"
# )
ontvangen_dir = basedir.joinpath("ontvangen", "Export levering 2")
for i in range(len(name)):
    print(name[i])
    df = selectie.loc[selectie["Code peilbuis"] == name[i]]

    fill_values = {
        "naam_meetpunt": name[i],
        "x-coor": df["x"].iloc[0],
        "y-coor": df["y"].iloc[0],
        "maaiveld (m NAP)": df["Maaiveldhoogte (mNAP)"].iloc[0] / 10,
        "top filter (m-mv)": df["top filter (m-mv)"].iloc[0],
        "onderkant filter (m-mv)": df["onderkant filter (m-mv)"].iloc[0],
        "slootafstand (m)": np.nan,
        "zomer streefpeil (m NAP)": np.nan,
        "winter streefpeil (m NAP)": np.nan,
        "gefundeerd (ja/nee)": "ja",
        "greppel aanwezig (ja/nee)": "nee",
        "drains aanwezig (ja/nee)": "nee",
        "WIS aanwezig (ja/nee)": "nee",
        "WIS afstand (m)": np.nan,
        "WIS diepte (m-mv)": np.nan,
        "greppelafstand (m)": np.nan,
        "greppeldiepte (m-mv)": np.nan,
        "drainafstand (m)": np.nan,
        "draindiepte (m-mv)": np.nan,
    }
    header = gwm_header_format.format(**fill_values)

    n = "NL33.HL." + name[i] + ".PBF1"

    try:
        data = pd.read_csv(ontvangen_dir.joinpath(f"{n}.csv"), skiprows=1)
        extra_2024_data_needed = True
    except FileNotFoundError:
        data = pd.read_csv(
            basedir.joinpath("meetreeksen_van_2024_kalibratie", f"{n}.csv"), skiprows=1
        )
        extra_2024_data_needed = False
        # data = data.iloc[:, [0, 2, 3]]    

    data = pd.concat([data.iloc[:,0], data.filter(regex="Momentaan")], axis=1)
    data = data.mask(data == -999)
    data = data.dropna(axis=1, how='all')

    if extra_2024_data_needed:
        data_2024 = pd.read_csv(rf"p:\11207812-somers-ontwikkeling\3-somers_development\QSOMERS\Handmatige uitvraag 2024\handmatige_uitvraag_bestanden\HunzeenAas\Export\{n}.csv", skiprows=1)
        data_final = pd.concat([data_2024, data])
        data_final.drop_duplicates()

        # data = data[mask]
    data.columns = ["datumtijd", "grondwaterstand (m NAP)"]
    data["grondwaterstand (m NAP)"] = data["grondwaterstand (m NAP)"]
    data["datumtijd"] = pd.to_datetime(data["datumtijd"])
    data["datumtijd"] = data["datumtijd"].dt.strftime("%d-%m-%Y %H:%M:%S")
    data["grondwaterstand (m NAP)"] = data["grondwaterstand (m NAP)"].where(
        data["grondwaterstand (m NAP)"] > -100
    )

    # path_out = f"P:/11207812-somers-ontwikkeling/database_grondwater/handmatige_uitvraag_bestanden/HunzeenAas/bewerkt/GWM_{name[i]}.txt"
    path_out = basedir.joinpath("bewerkt", f"GWM_{name[i]}.txt")
    with open(path_out, "w") as fp:
        fp.write(header)

    data.to_csv(path_out, mode="a", sep=";", index=False, header=False)


###########################################################################
# 2024 kalibratie data
###########################################################################

# basedir_2024 = Path(
#     "p:/11207812-somers-ontwikkeling/3-somers_development/QSOMERS/Handmatige uitvraag 2024/handmatige_uitvraag_bestanden/HunzeenAas"
# )

# ontvangen_dir_2024 = basedir_2024.joinpath("Export")

# list_of_2024_wells_for_2026_dataset = pd.read_csv(
#     r"p:\11207812-somers-ontwikkeling\3-somers_development\QSOMERS\Handmatige uitvraag 2026\handmatige_uitvraag_bestanden\HunzeEnAas\lijst_bestandsnamen_2024_2026.csv"
# )

# data_2024_for_2026_calibration = (
#     list_of_2024_wells_for_2026_dataset["Bestandsnaam 2024"]
#     .where(
#         list_of_2024_wells_for_2026_dataset["Bestandsnaam 2024 Aanwezig in 2026?"]
#         == False
#     )
#     .dropna()
# )

# # files = glob(str(ontvangen_dir_2024.joinpath("*.csv")))

# files = ontvangen_dir_2024.glob("*.csv")

# n = 0

# for file in files:
#     print(file.stem)

#     if file.stem in data_2024_for_2026_calibration.values.tolist():
#         """
#         read the files that are not in the 2026 calibration folder
#         """
#         subprocess.call(["cp ", "-l"])
#         n += 1

# print(n)
