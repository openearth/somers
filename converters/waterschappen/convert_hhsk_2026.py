import numpy as np
import pandas as pd
from pathlib import Path


def remove_nans(df):
    df = df.index.dropna()
    first_valid_index = df.dropna(how="all").index[0]
    last_valid_index = df.dropna(how="all").index[-1]
    return df.loc[first_valid_index:last_valid_index]


basedir = Path(
    r"p:/11207812-somers-ontwikkeling/3-somers_development/QSOMERS/Handmatige uitvraag 2026/handmatige_uitvraag_bestanden/HHSK/"
)

# %% slootwaterbestaden

swm_header_format = (
    "# naam_meetpunt: {naam_meetpunt}\n"
    "# x-coor: {x-coor}\n"
    "# y-coor: {y-coor}\n"
    "*\n"
    "> datumtijd (dd-mm-yyyy / dd-mm-yyyy HH:MM:SS)\n"
    "> slootwaterstand (m NAP)\n"
)

ontvangen_dir = basedir.joinpath("ontvangen", "SWM_aanlevering_HHSK")

swm_files = ontvangen_dir.glob("SWM_*.xlsx")

for path in swm_files:

    print(f"Working on {Path(path).stem}")

    if Path(path).stem == "SWM_MPN323":

        header = pd.read_excel(path, sheet_name="Blad1", nrows = 6)

        values = pd.read_excel(
            path,
            sheet_name="Blad1",
            index_col=0,
            header=None,
            skiprows=[0, 1, 2, 3, 4, 5],
        )

        values.index.name = "datumtijd"
        values.columns = ["slootwaterstand (m NAP)"]

        values.index = pd.to_datetime(values.index)

# for naam_meetpunt, metadata_meetpunt in metadata.iterrows():
#     print(naam_meetpunt)
#     fill_values = metadata_meetpunt.to_dict()
#     fill_values.update({"naam_meetpunt": naam_meetpunt})
#     header = swm_header_format.format(**fill_values)

#     data_selected = data[[naam_meetpunt]]
#     data_selected = remove_nans(data_selected).reset_index()
#     data_selected.columns = ["datumtijd", "slootwaterstand (m NAP)"]
#     data_selected["datumtijd"] = data_selected["datumtijd"].dt.strftime("%d-%m-%Y")

# if not data_selected["slootwaterstand (m NAP)"].mean() > -0.5:
    path_out = basedir.joinpath("bewerkt", f"SWM_{naam_meetpunt}.txt")
#     with open(path_out, "w") as fp:
#         fp.write(header)

#     data_selected.to_csv(path_out, mode="a", sep=";", index=False, header=False)

#     data_selected.set_index("datumtijd").plot()
