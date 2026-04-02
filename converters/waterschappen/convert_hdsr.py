from pathlib import Path
import time
import os

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

start = time.time()


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


basedir = Path(
    "p:/11207812-somers-ontwikkeling/3-somers_development/QSOMERS/Handmatige uitvraag 2026/handmatige_uitvraag_bestanden/HDSR"
)

ontvangen_dir = basedir.joinpath("ontvangen")
figdir = basedir.joinpath("bewerkt", "figuren")

# slootpeil meetreeksen (swm)
swm_file = ontvangen_dir.joinpath("slootpeil_meetreeksen_hdsr.csv")

data = pd.read_csv(
    swm_file,
    sep=";",
    skiprows=[0, 1, 3],
)

data.iloc[:, 0] = data.iloc[:, 0].str.replace("/", "-")
data.set_index(data.iloc[:, 0].name, inplace=True)
data.index.name = ""

data.index = pd.to_datetime(data.index)
data = data.sort_index()

# average over moments when the clock is set back an hour (zomer naar wintertijd)
data = data.groupby(data.index).mean()
# data = data[~data.index.duplicated(keep="first")]

# slootpeil locaties van meetreeksen
swm_locatiegegevens = ontvangen_dir.joinpath("slootpeil_locatiegegevens_hdsr.csv")

metadata = pd.read_csv(swm_locatiegegevens, sep=";", index_col=0)
metadata.drop_duplicates(inplace=True)

for column in data:
    metadata_location = metadata[metadata["Locatie Naam"] == column]
    print(f"Is there metadata for {column}: {not metadata_location.empty}")

    header = [
        f"# naam_meetpunt: {column}",
        f"# x-coor: {metadata_location["X"].item()}",
        f"# y-coor: {metadata_location["Y"].item()}",
        "*",
        "> datumtijd (dd-mm-yyyy / dd-mm-yyyy HH:MM:SS)",
        "slootwaterstand (m NAP)",
    ]

    path_out = basedir.joinpath("bewerkt", f"SWM_{column}.txt")

    with open(path_out, "w") as f:
        for line in header:
            f.write(f"{line}\n")

    meetgegevens = data[column]

    # make a quick figure
    fig, ax = plt.subplots()

    ax.plot(meetgegevens.index, meetgegevens)

    ax.set_xlim(meetgegevens.first_valid_index(), meetgegevens.last_valid_index())
    ax.grid()
    ax.set_ylabel("Slootwaterstand (cm t.o.v. NAP)")
    ax.set_title(f"{column}")

    set_xaxis_datelabels(ax)

    # plt.show()
    plt.savefig(
        figdir.joinpath(f"Slootwaterstand {column}.png"),
        bbox_inches="tight",
        dpi=300,
    )

    plt.close()

    meetgegevens.index = meetgegevens.index.strftime("%d-%m-%Y %H:%M:%S")
    meetgegevens.reset_index().to_csv(
        path_out, mode="a", sep=";", index=False, header=False
    )

end = time.time()
print(f"Elapsed time: {end - start} seconds.")
