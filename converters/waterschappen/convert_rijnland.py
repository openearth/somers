# %%
from pathlib import Path
from glob import glob

import pandas as pd

basedir = Path(
    "p:/11207812-somers-ontwikkeling/3-somers_development/QSOMERS/Handmatige uitvraag 2026/handmatige_uitvraag_bestanden/Rijnland"
)

ontvangen_dir = basedir.joinpath("ontvangen")

# gwm_files = ontvangen_dir.glob("GWM_*.txt")
gwm_files = glob(str(ontvangen_dir.joinpath("GWM_*.txt")))

# %% gwm-files
for path in gwm_files:

    with open(path, "r", encoding="utf-8", errors="replace") as f:
        header_lines = [next(f).rstrip("\n") for _ in range(22)]

    header = "\n".join(header_lines) + "\n"
    header = header.replace("# draindiepte (m-mv)", "# draindiepte (m-mv):")
    header = header.replace(
        "> datumtijd (dd-mm-yyyy HH:MM:SS)", "> datumtijd (dd-mm-yyyy)"
    )

    values = pd.read_csv(
        path,
        sep=";",
        skiprows=21,
        header=None,
        names=["datumtijd", "grondwaterstand"],
        dayfirst=True,
        decimal=",",
    ).set_index("datumtijd")

    values = values[values.index.astype(str).str[0].str.isdigit()]

    values.index = pd.to_datetime(values.index, dayfirst=True)

    values = values.sort_index()

    values = values.resample("D").mean()

    values.index = values.index.strftime("%d-%m-%Y")

    path_out = basedir.joinpath("bewerkt", Path(path).stem + ".txt")

    with open(path_out, "w", encoding="utf-8") as fp:
        fp.write(header)

    values["grondwaterstand"] = values["grondwaterstand"].round(2)

    values.reset_index().to_csv(
        path_out,
        mode="a",
        sep=";",
        index=False,
        header=False,
    )

# %% swm-files
swm_files = ontvangen_dir.glob("SWM_*.txt")

for path in swm_files:

    # header_lines = pd.read_csv(path, header=None, on_bad_lines="skip").loc[:5]
    # header = "\n".join(header_lines[0].astype(str)) + "\n"

    with open(path, "r", encoding="utf-8", errors="replace") as f:
        header_lines = [next(f).rstrip("\n") for _ in range(6)]

    header = "\n".join(header_lines) + "\n"
    header = header.replace(
        "> datumtijd (dd-mm-yyyy HH:MM:SS)", "> datumtijd (dd-mm-yyyy)"
    )

    values = pd.read_csv(
        path,
        sep=";",
        skiprows=21,
        header=None,
        names=["datumtijd", "grondwaterstand"],
        dayfirst=True,
        decimal=",",
    ).set_index("datumtijd")

    values = values[values.index.astype(str).str[0].str.isdigit()]

    values.index = pd.to_datetime(values.index, dayfirst=True)

    values = values.sort_index()

    values = values.resample("D").mean()

    # z_scores = abs((values - values.mean()) / values.std())
    # valid = (z_scores < 5.0) & (values > -4.0) & (values < -2.0)

    path_out = basedir.joinpath("bewerkt", Path(path).stem + ".txt")
    with open(path_out, "w") as fp:
        fp.write(header)

    # values = values.where(valid)
    first_valid_idx = values.notna().any(axis=1).idxmax()
    values = values.loc[first_valid_idx:]
    values = values.dropna()
    values.index = values.index.strftime("%d-%m-%Y")
    values.reset_index().to_csv(path_out, mode="a", sep=";", index=False, header=False)

# %%
