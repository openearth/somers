# %%
from pathlib import Path
from glob import glob
import pytz

import pandas as pd

# hier beginnen weer morgenmiddag (16-04-2026)

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
    "# greppel aanwezig (ja/nee): {Greppel aanwezig}\n"
    "# drains aanwezig (ja/nee): {drains aanwezig (ja/nee)}\n"
    "# WIS aanwezig (ja/nee): {WIS aanwezig (ja/nee)}\n"
    "# greppelafstand (m): {greppelafstand (m)}\n"
    "# greppeldiepte (m-mv): {greppeldiepte (m-mv)}\n"
    "# drainafstand (m): {drainafstand (m)}\n"
    "# draindiepte (m-mv): {draindiepte (m-mv)}\n"
    "# WIS afstand (m): {WIS afstand (m)}\n"
    "# WIS diepte (m-mv): {WIS diepte (m-mv)}\n"
    "*\n"
    "> datumtijd (dd-mm-yyyy)\n"
    "> grondwaterstand (m NAP)\n"
)

swm_header_format = (
    "# naam_meetpunt: {naam_meetpunt}\n"
    "# x-coor: {x-coor}\n"
    "# y-coor: {y-coor}\n"
    "*\n"
    "> datumtijd (dd-mm-yyyy)\n"
    "> slootwaterstand (m NAP)\n"
)

amsterdam_time = pytz.timezone("Europe/Amsterdam")

basedir = Path(
    "p:/11207812-somers-ontwikkeling/3-somers_development/QSOMERS/Handmatige uitvraag 2026/handmatige_uitvraag_bestanden/WSHD"
)

ontvangen_dir = basedir.joinpath("ontvangen")

# gwm_files = ontvangen_dir.glob("GWM_*.txt")
gwm_files = glob(str(ontvangen_dir.joinpath("Data_WSHD_2026", "B*.csv")))


# %% gwm-files
for path in gwm_files:

    print(f"Working on {Path(path).stem}")

    values = pd.read_csv(
        path,
        sep=";",
        header=0,
        usecols=[
            "Datum en tijd (UTC+00:00)",
            "Waterstanden gerefereerd tov Verticaal referentievlak (cm)",
        ],
        # dayfirst=True,
        decimal=",",
    )

    values["Datum en tijd (UTC+00:00)"] = pd.to_datetime(
        values["Datum en tijd (UTC+00:00)"], dayfirst=True
    )

    values.columns = ["datumtijd", "grondwaterstand"]

    values = values.set_index("datumtijd")

    values.index = (
        values.index.tz_localize(pytz.utc).tz_convert(amsterdam_time).tz_localize(None)
    )

    values = values.resample("D").mean()

    values.index = values.index.strftime("%d-%m-%Y")

    path_out = basedir.joinpath("bewerkt", "GWM_" + Path(path).stem + ".txt")

    values["grondwaterstand"] = values["grondwaterstand"].round(2)

    values.reset_index().to_csv(
        path_out,
        mode="a",
        sep=";",
        index=False,
        header=False,
    )
