# %%
from pathlib import Path
from glob import glob

import pandas as pd

basedir = Path(
    "p:/11207812-somers-ontwikkeling/3-somers_development/QSOMERS/Handmatige uitvraag 2026/handmatige_uitvraag_bestanden/Delfland"
)

ontvangen_dir = basedir.joinpath("ontvangen", "SOMERS_DATA_2026_per_dag")

# %% swm-files
swm_files = ontvangen_dir.glob("SWM_*.txt")

for i, path in enumerate(swm_files):

    print(i, path)

    # if i<10:
    header_lines = pd.read_csv(path, header=None).loc[:5]
    header = "\n".join(header_lines[0].astype(str)) + "\n"

    values = pd.read_csv(
        path, delimiter=";", header=5, parse_dates=True, index_col=0, dayfirst=True
    )
    # values = values.sort_index()
    # z_scores = abs((values - values.mean()) / values.std())
    # valid = (z_scores < 5.0) & (values > -4.0) & (values < -2.0)

    if values.empty:
        print("No values for this timeseries")

    else:
        values = values.sort_index()

        path_out = basedir.joinpath("bewerkt", Path(path).stem + ".txt")
        with open(path_out, "w") as fp:
            fp.write(header)

        # values = values.where(valid)
        first_valid_idx = values.notna().any(axis=1).idxmax()
        values = values.loc[first_valid_idx:]
        values.index = values.index.strftime("%d-%m-%Y")
        values.reset_index().to_csv(
            path_out, mode="a", sep=";", index=False, header=False
        )

# %%
