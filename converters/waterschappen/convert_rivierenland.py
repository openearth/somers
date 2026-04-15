# %%
from pathlib import Path
from glob import glob

import pandas as pd

basedir = Path(
    "p:/11207812-somers-ontwikkeling/3-somers_development/QSOMERS/Handmatige uitvraag 2026/handmatige_uitvraag_bestanden/Rivierenland"
)

ontvangen_dir = basedir.joinpath("ontvangen", "nieuwe aanlevering")

# gwm_files = ontvangen_dir.glob("GWM_*.txt")
gwm_files = glob(str(ontvangen_dir.joinpath("GWM_*.txt")))

# # %% gwm-files
# for path in gwm_files:

#     with open(path, "r", encoding="utf-8", errors="replace") as f:
#         header_lines = [next(f).rstrip("\n") for _ in range(22)]

#     header = "\n".join(header_lines) + "\n"
#     header = header.replace("# draindiepte (m-mv)", "# draindiepte (m-mv):")
#     header = header.replace(
#         "> datumtijd (dd-mm-yyyy / dd-mm-yyyy HH:MM:SS)", "> datumtijd (dd-mm-yyyy)"
#     )

#     values = pd.read_csv(
#         path,
#         sep=";",
#         skiprows=22,
#         header=None,
#         names=["datumtijd", "grondwaterstand"],
#         # dayfirst=True,
#         decimal=",",
#         # parse_dates=True
#     )  # .set_index("datumtijd")

#     values["datumtijd"] = pd.to_datetime(
#         values["datumtijd"].str[:10], format="%d-%m-%Y"
#     ) + pd.to_timedelta(values["datumtijd"].str[10:])

#     values = values.set_index("datumtijd")

#     # values = values[values.index.astype(str).str[0].str.isdigit()]

#     values.index = pd.to_datetime(values.index, dayfirst=True)

#     values = values.sort_index()

#     values["grondwaterstand"] = values["grondwaterstand"].astype(float)

#     values = values.resample("D").mean()

#     values.index = values.index.strftime("%d-%m-%Y")

#     path_out = basedir.joinpath("bewerkt", Path(path).stem.replace('[', '').replace(']','') + ".txt")

#     with open(path_out, "w", encoding="utf-8") as fp:
#         fp.write(header)

#     values["grondwaterstand"] = values["grondwaterstand"].round(2)

#     values.reset_index().to_csv(
#         path_out,
#         mode="a",
#         sep=";",
#         index=False,
#         header=False,
#     )


swm_files = glob(str(ontvangen_dir.joinpath("SWM_*.txt")))

# %% swm-files
for path in swm_files:

    with open(path, "r", encoding="utf-8", errors="replace") as f:
        header_lines = [next(f).rstrip("\n") for _ in range(6)]

    header = "\n".join(header_lines) + "\n"
    header = header.replace(
        "> datumtijd (dd-mm-yyyy HH:MM:SS)", "> datumtijd (dd-mm-yyyy)"
    )

    header = header.replace(":", ": ")

    values = pd.read_csv(
        path,
        sep=";",
        skiprows=6,
        header=None,
        names=["datumtijd", "slootwaterstand"],
        # dayfirst=True,
        decimal=",",
        # parse_dates=True
    )  # .set_index("datumtijd")

    values["datumtijd"] = pd.to_datetime(
        values["datumtijd"].str[:10], format="%d-%m-%Y"
    ) + pd.to_timedelta(values["datumtijd"].str[10:])

    values = values.set_index("datumtijd")

    # values = values[values.index.astype(str).str[0].str.isdigit()]

    values.index = pd.to_datetime(values.index, dayfirst=True)

    values = values.sort_index()

    values["slootwaterstand"] = values["slootwaterstand"].astype(float)

    values = values.resample("D").mean()

    values.index = values.index.strftime("%d-%m-%Y")

    path_out = basedir.joinpath(
        "bewerkt", Path(path).stem.replace("[", "").replace("]", "") + ".txt"
    )

    with open(path_out, "w", encoding="utf-8") as fp:
        fp.write(header)

    values["slootwaterstand"] = values["slootwaterstand"].round(2)

    values.reset_index().to_csv(
        path_out,
        mode="a",
        sep=";",
        index=False,
        header=False,
    )
