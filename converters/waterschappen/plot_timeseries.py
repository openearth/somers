from pathlib import Path
from glob import glob

import pandas as pd
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


basedir = Path("p:/11207812-somers-ontwikkeling/3-somers_development/QSOMERS/")

waterschap = "Delfland"

ongecontroleerd_dir = basedir.joinpath(
    "Handmatige uitvraag 2026",
    "handmatige_uitvraag_bestanden",
    waterschap,
    "bewerkt",
)

figdir = ongecontroleerd_dir.joinpath("figuren")

files = ongecontroleerd_dir.glob("*.txt")

for i, path in enumerate(files):
    print(i, path.stem)

    if path.stem.startswith("GWM"):
        # print("GWM file")
        skiprows = 22
        ylabel = "Grondwaterstand (cm t.o.v. NAP)"

    elif path.stem.startswith("SWM"):
        # print("SWM_file")
        skiprows = 6
        ylabel = "Slootwaterstand (cm t.o.v. NAP)"

    else:
        skiprows = 0
        ylabel = ""

    data = pd.read_csv(
        path,
        sep=";",
        skiprows=skiprows,
        header=None,
        names=["datumtijd", "waterstand"],
        decimal=",",
    ).set_index("datumtijd")

    data = data.astype("float64")

    data.index = pd.to_datetime(data.index, dayfirst=True)

    # make a quick figure
    fig, ax = plt.subplots()

    ax.plot(data.index, data.values)

    ax.set_xlim(data.first_valid_index(), data.last_valid_index())
    ax.grid()
    ax.set_ylabel(ylabel)
    ax.set_title(path.stem)

    set_xaxis_datelabels(ax)

    # plt.show()
    plt.savefig(
        figdir.joinpath(f"{path.stem}.png"),
        bbox_inches="tight",
        dpi=300,
    )

    plt.close()
