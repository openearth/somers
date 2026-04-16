# %%
from pathlib import Path
from glob import glob
import pytz

import pandas as pd
import numpy as np

# hier beginnen weer morgenmiddag (16-04-2026)

gwm_header_format = (
    "# naam_meetpunt: {naam_meetpunt}\n"
    "# x-coor: {x-coor}\n"
    "# y-coor: {y-coor}\n"
    "# maaiveld (m NAP): {maaiveld (m NAP)}\n"
    "# top filter (m-mv): \n"
    "# onderkant filter (m-mv): \n"
    "# gefundeerd (ja/nee): \n"
    "# slootafstand (m): \n"
    "# zomer streefpeil (m NAP): {zomer streefpeil (m NAP)}\n"
    "# winter streefpeil (m NAP): {winter streefpeil (m NAP)}\n"
    "# greppel aanwezig (ja/nee): \n"
    "# drains aanwezig (ja/nee): \n"
    "# WIS aanwezig (ja/nee): \n"
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

xs = {"B01": 78600.35, "B02": 95915.13}
ys = {"B01": 427727.44, "B02": 420636.71}
maaiveldhoogtes = {"B01": -1.87, "B02": -1.98}
zomerpeilen = {"B01": -2.15, "B02": -2.35}
winterpeilen = {"B01": -2.0, "B02": -2.35}

# %% gwm-files
for path in gwm_files:

    print(f"Working on {Path(path).stem}")

    peilbuisnaam = Path(path).stem
    location = Path(path).stem.split("-")[0]
    x = xs[location]
    y = ys[location]
    maaiveldhoogte = maaiveldhoogtes[location]
    zomerpeil = zomerpeilen[location]
    winterpeil = winterpeilen[location]

    new_lines = {
        "naam_meetpunt": peilbuisnaam,
        "x-coor": x,
        "y-coor": y,
        "maaiveld (m NAP)": maaiveldhoogte,
        "zomer streefpeil (m NAP)": zomerpeil,
        "winter streefpeil (m NAP)": winterpeil,
    }
    # fill_values.update(new_lines)
    header = gwm_header_format.format(**new_lines)

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
    with open(path_out, "w") as fp:
        fp.write(header)

    values["grondwaterstand"] = values["grondwaterstand"].round(2)

    values.reset_index().to_csv(
        path_out,
        mode="a",
        sep=";",
        index=False,
        header=False,
    )

# %% swm files

xs_swm = {
    "TREND_03375ST_20260323 Oudeland vS": "95883.96799875477",
    "TREND_26925GM_20260323 Biert": "78288.47840480926",
}
ys_swm = {
    "TREND_03375ST_20260323 Oudeland vS": "419827.0292777744",
    "TREND_26925GM_20260323 Biert": "428148.99452653556",
}

# B01 is Biert is 26925GM
# B02 is Oudeland van Strijen is 03375ST

swm_files = glob(str(ontvangen_dir.joinpath("Data_WSHD_2026", "TREND*.csv")))

for path in swm_files:

    print(f"Working on {Path(path).stem}")

    location = Path(path).stem
    x = xs_swm[location]
    y = ys_swm[location]

    new_lines = {
        "naam_meetpunt": location,
        "x-coor": x,
        "y-coor": y,
    }
    # fill_values.update(new_lines)
    header = swm_header_format.format(**new_lines)

    values = pd.read_csv(
        path,
        sep=",",
        header=0,
        skiprows=[1],
        usecols=[
            "Timestamp",
            "Value",
        ],
        # dayfirst=True,
        # decimal=",",
    )

    values["Timestamp"] = pd.to_datetime(values["Timestamp"], dayfirst=True)

    values.columns = ["datumtijd", "slootwaterstand"]

    values = values.set_index("datumtijd")

    values["slootwaterstand"] = values["slootwaterstand"].str.replace(" mNAP", "")
    values["slootwaterstand"] = values["slootwaterstand"].astype(float)

    values = values.resample("D").mean()

    values.index = values.index.strftime("%d-%m-%Y")

    path_out = basedir.joinpath("bewerkt", "SWM_" + Path(path).stem + ".txt")
    with open(path_out, "w") as fp:
        fp.write(header)

    values["slootwaterstand"] = values["slootwaterstand"].round(2)

    values.reset_index().to_csv(
        path_out,
        mode="a",
        sep=";",
        index=False,
        header=False,
    )
