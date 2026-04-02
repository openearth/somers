from pathlib import Path
from glob import glob

import pandas as pd

basedir = Path(
    "p:/11207812-somers-ontwikkeling/3-somers_development/QSOMERS/Handmatige uitvraag 2026/handmatige_uitvraag_bestanden/AGV"
)

ontvangen_dir = basedir.joinpath("ontvangen")

# gwm_files = ontvangen_dir.glob("GWM_*.txt")
gwm_files = glob(str(ontvangen_dir.joinpath("GWM_*.txt")))

# %% gwm-files
for path in gwm_files:

    df = pd.read_csv(path, delimiter="\t", header=None)

    header_lines = df.loc[:21]

    header = (
        "\n".join(header_lines[0] + " " + header_lines[1].fillna("").astype(str)) + "\n"
    )
    header = header.replace("# draindiepte (m-mv)", "# draindiepte (m-mv):")

    values = pd.read_csv(
        path, delimiter=";", header=21, parse_dates=True, index_col=0, dayfirst=True
    )

    path_out = basedir.joinpath("bewerkt", Path(path).stem + ".txt")
    with open(path_out, "w") as fp:
        fp.write(header)
    # values.plot()
    values = values.sort_index()
    values.index = values.index.strftime("%d-%m-%Y %H:%M:%S")
    values.reset_index().to_csv(path_out, mode="a", sep=";", index=False, header=False)


# %% swm-files
swm_files = ontvangen_dir.glob("SWM_*.csv")

for path in swm_files:

    header_lines = pd.read_csv(path, header=None).loc[:5]
    header = "\n".join(header_lines[0].astype(str)) + "\n"

    values = pd.read_csv(
        path, delimiter=";", header=5, parse_dates=True, index_col=0, dayfirst=True
    )
    values = values.sort_index()
    z_scores = abs((values - values.mean()) / values.std())
    valid = (z_scores < 5.0) & (values > -4.0) & (values < -2.0)

    path_out = basedir.joinpath("bewerkt", Path(path).stem + ".txt")
    with open(path_out, "w") as fp:
        fp.write(header)

    values = values.where(valid)
    values.index = values.index.strftime("%d-%m-%Y")
    values.reset_index().to_csv(path_out, mode="a", sep=";", index=False, header=False)
