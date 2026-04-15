from pathlib import Path

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


def set_xaxis_datelabels(ax):

    # maj_loc = mdates.MonthLocator(bymonth=np.arange(1, 12, 3))
    maj_loc = mdates.AutoDateLocator(minticks=2, maxticks=7)
    ax.xaxis.set_major_locator(maj_loc)

    # horizontal labels
    ax.xaxis.set_tick_params(rotation=0)

    for label in ax.get_xticklabels():
        label.set_horizontalalignment("center")

    zfmts = ["", "%b\n%Y", "%b", "%b-%d", "%H:%M", "%H:%M"]
    offset_formats = [
        "",
        "%Y",
        "%b %Y",
        "%d %b %Y",
        "%d %b %Y",
        "%d %b %Y %H:%M",
    ]

    maj_fmt = mdates.ConciseDateFormatter(maj_loc, show_offset=False)
    maj_fmt.zero_formats = zfmts
    maj_fmt.offset_formats = offset_formats

    ax.xaxis.set_major_formatter(maj_fmt)
    ax.figure.autofmt_xdate(rotation=0, ha="center")

    # ax.xaxis.set_major_formatter(formatter)
    ax.xaxis.set_minor_locator(mdates.MonthLocator())


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

basedir = Path("p:/11207812-somers-ontwikkeling/3-somers_development/QSOMERS/")

# code peilbuis is name
# uiteindelijk moet GWM toegevoegd worden aan de name
# Selecteren welke kolom overeenkomt met wat
# filterstelling (m tov Maaiveld) opsplitsen in top en bottom filter
ontvangen_dir = basedir.joinpath(
    "Handmatige uitvraag 2026",
    "handmatige_uitvraag_bestanden",
    "HunzeEnAas",
    "ontvangen",
    "Export levering 2",
)
datadir_2024 = basedir.joinpath(
    "Handmatige uitvraag 2024",
    "handmatige_uitvraag_bestanden",
    "HunzeenAas",
    "Export",
)

figdir = basedir.joinpath(
    "Handmatige uitvraag 2026",
    "handmatige_uitvraag_bestanden",
    "HunzeEnAas",
    "bewerkt",
    "figuren",
)

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

    data = pd.read_csv(datadir_2024.joinpath(f"{n}.csv"), skiprows=1)

    data = pd.concat([data.iloc[:, 0], data.filter(regex="Momentaan")], axis=1)
    data = data.mask(data == -999)
    data = data.dropna(axis=1, how="all")

    try:
        data_2026 = pd.read_csv(ontvangen_dir.joinpath(f"{n}.csv"), skiprows=1)
        data_2026 = pd.concat(
            [data_2026.iloc[:, 0], data_2026.filter(regex="Momentaan")], axis=1
        )
        data_2026 = data_2026.mask(data_2026 == -999)
        data_2026 = data_2026.dropna(axis=1, how="all")
        data_2026 = data_2026.rename(
            columns={
                "WATHTE [mNAP]_ts_Momentaan.F": "WATHTE [mNAP]_Momentaan.F",
                "WATHTE [mNAP]_ts_Momentaan.P": "WATHTE [mNAP]_Momentaan.P",
            }
        )
        data = pd.concat([data, data_2026])
        data.drop_duplicates(inplace=True)
    except FileNotFoundError:
        print(f"File {n} not found for 2026 calibration")

    data.columns = ["datumtijd", "grondwaterstand (m NAP)"]
    data["grondwaterstand (m NAP)"] = data["grondwaterstand (m NAP)"]
    data["datumtijd"] = pd.to_datetime(data["datumtijd"])
    data["datumtijd"] = data["datumtijd"].dt.strftime("%d-%m-%Y %H:%M:%S")
    data["grondwaterstand (m NAP)"] = data["grondwaterstand (m NAP)"].where(
        data["grondwaterstand (m NAP)"] > -100
    )

    data = data.set_index("datumtijd")
    # data = data.dropna()

    data.index = pd.to_datetime(data.index, dayfirst=True)

    # calculate daily means
    data = data.resample('D').mean()

    # make a quick figure
    fig, ax = plt.subplots()

    ax.plot(data.index, data)

    ax.set_xlim(data.first_valid_index(), data.last_valid_index())
    ax.grid()
    ax.set_ylabel("Slootwaterstand (cm t.o.v. NAP)")
    ax.set_title(f"{n}")

    set_xaxis_datelabels(ax)

    # plt.show()
    plt.savefig(
        figdir.joinpath(f"Grondwaterstand {n}.png"),
        bbox_inches="tight",
        dpi=300,
    )

    plt.close()

    path_out = basedir.joinpath(
        "Handmatige uitvraag 2026",
        "handmatige_uitvraag_bestanden",
        "HunzeEnAas",
        "bewerkt",
        f"GWM_{name[i]}.txt",
    )
    with open(path_out, "w") as fp:
        fp.write(header)

    data.index = data.index.strftime("%d-%m-%Y")

    data.to_csv(path_out, mode="a", sep=";", index=True, header=False)
