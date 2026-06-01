import numpy as np
import pandas as pd
from pathlib import Path
from glob import glob


def remove_nans(df):
    df = df.index.dropna()
    first_valid_index = df.dropna(how="all").index[0]
    last_valid_index = df.dropna(how="all").index[-1]
    return df.loc[first_valid_index:last_valid_index]


gwm_header_format = (
    "# naam_meetpunt: {naam_meetpunt}\n"
    "# x-coor: {x-coor}\n"
    "# y-coor: {y-coor}\n"
    "# maaiveld (m NAP): {maaiveld (m NAP)}\n"
    "# top filter (m-mv): {top filter}\n"
    "# onderkant filter (m-mv): {onderkant filter}\n"
    "# gefundeerd (ja/nee): {gefundeerd}\n"
    "# slootafstand (m): \n"
    "# zomer streefpeil (m NAP): {zomer streefpeil}\n"
    "# winter streefpeil (m NAP): {winter streefpeil}\n"
    "# greppel aanwezig (ja/nee): \n"
    "# drains aanwezig (ja/nee): {drains aanwezig}\n"
    "# WIS aanwezig (ja/nee): {WIS aanwezig}\n"
    "# greppelafstand (m): \n"
    "# greppeldiepte (m-mv): \n"
    "# drainafstand (m): \n"
    "# draindiepte (m-mv): \n"
    "# WIS afstand (m): \n"
    "# WIS diepte (m-mv): \n"
    "*\n"
    "> datumtijd (dd-mm-yyyy)\n"
    "> grondwaterstand (m NAP)\n"
)

basedir = Path(
    r"p:/11207812-somers-ontwikkeling/3-somers_development/QSOMERS/Handmatige uitvraag 2026/handmatige_uitvraag_bestanden/HHSK/"
)

# ontvangen_dir = basedir.joinpath("ontvangen", "SWM_aanlevering_HHSK")

# swm_files = ontvangen_dir.glob("SWM_*.xlsx")

# for path in swm_files:

#     print(f"Working on {Path(path).stem}")

#     header = pd.read_excel(path, sheet_name="Blad1", nrows=5)

#     values = pd.read_excel(
#         path,
#         sheet_name="Blad1",
#         index_col=0,
#         header=None,
#         skiprows=[0, 1, 2, 3, 4, 5],
#     )

#     values.index.name = "datumtijd"
#     values.columns = ["slootwaterstand (m NAP)"]

#     values.index = pd.to_datetime(values.index)

#     values = values.resample("D").mean()
#     values.index = values.index.strftime("%d-%m-%Y")

#     path_out = basedir.joinpath("bewerkt", f"{Path(path).stem}.txt")
#     header.to_csv(path_out, mode="w", sep="\t", index=False)

#     values.to_csv(path_out, mode="a", sep=";", index=True, header=False)

#################################################################
# extra files for HHSK
#################################################################

ontvangen_dir = basedir.joinpath("ontvangen")

filename = ontvangen_dir.joinpath("WIS_export_Deltares.csv")

data = pd.read_csv(filename, sep=";", index_col=0, skiprows=[1], decimal=",")

data.index = pd.to_datetime(data.index, dayfirst=True)

# mask data that is "completed unreliable"
for col in data.columns[::2]:
    data.loc[data[f"{col} quality"] == "completed unreliable", col] = np.nan

# drop the quality columns
data = data.loc[:, ~data.columns.str.contains("quality", case=False)]

#################################################################
# read the metadata for 2026
#################################################################

filename = ontvangen_dir.joinpath("meetpunten_info_grondslag.csv")

metadata = pd.read_csv(filename, sep=";", decimal=",")


i = 0

for col in data.columns:
    print(f"Working on {col}")
    metadata_row1 = metadata[metadata["ID"] == col]
    metadata_row2 = metadata[metadata["ID"] == col[:-4]]

    values = data[col]

    fill_values = {
        "naam_meetpunt": metadata_row1["ID"].item(),
        "x-coor": metadata_row1["X"].item(),
        "y-coor": metadata_row1["Y"].item(),
        "maaiveld (m NAP)": metadata_row1["MAAILVELD"].item(),
        "top filter": "",
        "onderkant filter": "",
        "gefundeerd": "",
        "zomer streefpeil": "",
        "winter streefpeil": "",
        "drains aanwezig": "",
        "WIS aanwezig": "",
    }
    header = gwm_header_format.format(**fill_values)

    ##########################
    # 2024 metadata
    ##########################

    basedir_2024 = Path(
        "p:/11207812-somers-ontwikkeling/3-somers_development/QSOMERS/Handmatige uitvraag 2024/handmatige_uitvraag_bestanden/HHSK"
    )

    ontvangen_dir_2024 = basedir_2024.joinpath("bewerkt")

    # gwm_files = ontvangen_dir.glob("GWM_*.txt")
    gwm_files_2024 = glob(str(ontvangen_dir_2024.joinpath("GWM_*.txt")))

    for path in gwm_files_2024:

        df = pd.read_csv(
            path, header=None, nrows=19, delimiter=":"
        )  # , delimiter="\t", header=None)

        x_2024 = df.loc[df[0] == "# x-coor", 1].astype(float).item()
        y_2024 = df.loc[df[0] == "# y-coor", 1].astype(float).item()

        if metadata_row1["X"].item() == round(x_2024, 1):
            if metadata_row1["Y"].item() == round(y_2024, 1):
                print("Match found! Adding data from 2024")
                fill_values = {
                    "naam_meetpunt": metadata_row1["ID"].item(),
                    "x-coor": metadata_row1["X"].item(),
                    "y-coor": metadata_row1["Y"].item(),
                    "maaiveld (m NAP)": metadata_row1["MAAILVELD"].item(),
                    "top filter": df.loc[df[0] == "# top filter (m-mv)", 1]
                    # .astype(float)
                    .item(),
                    "onderkant filter": df.loc[df[0] == "# onderkant filter (m-mv)", 1]
                    # .astype(float)
                    .item(),
                    "gefundeerd": df.loc[df[0] == "# gefundeerd (ja/nee)", 1].item(),
                    "zomer streefpeil": df.loc[df[0] == "# zomer streefpeil (m NAP)", 1]
                    # .astype(float)
                    .item(),
                    "winter streefpeil": df.loc[
                        df[0] == "# winter streefpeil (m NAP)", 1
                    ]
                    # .astype(float)
                    .item(),
                    "drains aanwezig": df.loc[
                        df[0] == "# drains aanwezig (ja/nee)", 1
                    ].item(),
                    "WIS aanwezig": df.loc[
                        df[0] == "# WIS aanwezig (ja/nee)", 1
                    ].item(),
                }
                header = gwm_header_format.format(**fill_values)

                i += 1

    path_out = basedir.joinpath(
        "bewerkt",
        f"GWM_{metadata_row1["ID"].item()}.txt",
    )
    with open(path_out, "w") as fp:
        fp.write(header)

    values.index = data.index.strftime("%d-%m-%Y")

    values.to_csv(path_out, mode="a", sep=";", index=True, header=False)

print(f"Matches found: {i}")
#####################################################
# read metadata for 2024 and append if there is a match in the location (x, y)
#####################################################

# basedir = Path(
#     "p:/11207812-somers-ontwikkeling/3-somers_development/QSOMERS/Handmatige uitvraag 2024/handmatige_uitvraag_bestanden/HHSK"
# )

# ontvangen_dir = basedir.joinpath("bewerkt")

# # gwm_files = ontvangen_dir.glob("GWM_*.txt")
# gwm_files = glob(str(ontvangen_dir.joinpath("GWM_*.txt")))

# for path in gwm_files:

#     df = pd.read_csv(path, header=None, nrows=19, delimiter=':')# , delimiter="\t", header=None)

#     x_2024 = df.loc[df[0] == "# x-coor", 1].astype(float).item()
#     y_2024 = df.loc[df[0] == "# x-coor", 1].astype(float).item()

#     if
# header_lines = df.loc[:21]

# header = (
#     "\n".join(header_lines[0] + " " + header_lines[1].fillna("").astype(str)) + "\n"
# )
# header = header.replace("# draindiepte (m-mv)", "# draindiepte (m-mv):")


# for the values
# 1. loop over columns
# 2. Keep rows where the quality is reliable
# 3. Save the remainder to a file

# for the metadata
# 1. read the metadata csv
# 2. link the name to the name of the values
# 3. select metadata column name that matches the name in the values
# 4. Add the metadata as a header to the output file with destination "bewerkt"
