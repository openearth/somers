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

ontvangen_dir = basedir.joinpath("ontvangen", "SWM_aanlevering_HHSK")

swm_files = ontvangen_dir.glob("SWM_*.xlsx")

for path in swm_files:

    print(f"Working on {Path(path).stem}")

    header = pd.read_excel(path, sheet_name="Blad1", nrows=5)

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

    values = values.resample("D").mean()
    values.index = values.index.strftime("%d-%m-%Y")

    path_out = basedir.joinpath("bewerkt", f"{Path(path).stem}.txt")
    header.to_csv(path_out, mode="w", sep="\t", index=False)

    values.to_csv(path_out, mode="a", sep=";", index=True, header=False)
