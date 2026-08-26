# %%
import os
from datetime import time
import pandas as pd
import configparser
import numpy as np
import matplotlib.pyplot as plt
import sys
from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


def find_timeseries_data(schema, locationkey):
    """Schema and location can either be derived from well_id or ditch_id"""
    sqlstr = """select tsv.datetime, tsv.scalarvalue, l.name, l.locationkey from {schema}_timeseries.timeseriesvaluesandflags tsv
    join {schema}_timeseries.timeseries t on t.timeserieskey=tsv.timeserieskey
    join {schema}_timeseries.location l on l.locationkey=t.locationkey
    where l.locationkey={locationkey}
    """.format(
        schema=schema, locationkey=locationkey
    )

    with engine.connect() as conn:
        timeseries = pd.read_sql(sqlstr, conn)
    return timeseries


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


def plot_gwlevel_timeseries(
    data,
    title,
    ditch_data,
    ditch_title,
    surface_level,
    ylabel="Waterstand (cm t.o.v. NAP)",
):
    fig, ax = plt.subplots()

    ax.axhline(
        y=surface_level, linestyle="--", color="saddlebrown", label="Maaiveldhoogte"
    )
    ax.plot(data.index, data.values, label=title, color="deepskyblue")
    ax.plot(
        ditch_data.index,
        ditch_data.values,
        label=ditch_title,
        color="darkblue",
        alpha=0.8,
    )

    ax.set_xlim(data.first_valid_index(), data.last_valid_index())
    ax.grid()
    ax.set_ylabel(ylabel)
    ax.set_xlabel("Datum")

    set_xaxis_datelabels(ax)

    ax.legend(loc="lower center", bbox_to_anchor=(0.25, -0.40))

    return fig, ax


# Get the directory of the current script
current_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the root of the project
project_root = os.path.abspath(os.path.join(current_dir, ".."))

# Add the parent directory to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
# Add the project root to the PYTHONPATH
sys.path.append(project_root)

# third party packages
from sqlalchemy import text
from ts_helpers.ts_helpers_waterschappen import establishconnection

figdir = Path(
    "p:/11207812-somers-ontwikkeling/3-somers_development/QSOMERS/Dataverzameling 2026/Dataverzameling/timeseries_metadata_ongecontroleerd/"
)

fc = "p:/11207812-somers-ontwikkeling/3-somers_development/QSOMERS/credentials/connection_online_jr.txt"
session, engine = establishconnection(fc)

# %%
sqlstr = text("""select * from metadata_ongecontroleerd.kalibratie""")
with engine.connect() as conn:
    df = pd.read_sql(sqlstr, conn)
# result is df met alle locaties

wells_ids = df["well_id"].to_list()
names = df["name"].to_list()
ditch_ids = df["ditch_id"]
ditch_names = df["ditch_name"]
surface_levels = df["altitude_m_nap"]

# # testje voor slootpeil Berkenwoude
timeseries = find_timeseries_data("regiodeal", 4) #Berkenwoude_sloot
timeseries = find_timeseries_data("nobv", 47) #LYD_OP_12
timeseries = find_timeseries_data("bro", 1149)
timeseries = find_timeseries_data("waterschappen", 137)  # WDOD_SWM_10485S
timeseries = find_timeseries_data("bro", 1152)
timeseries = find_timeseries_data("waterschappen", 297) # Wetterskip_SWM_MM_SPC_PBS
timeseries = find_timeseries_data("waterschappen", 288) # Wetterskip_SWM_GLB_BOR1
timeseries = find_timeseries_data("nobv", 38) # ZEG_RF16_14

print(timeseries)

for i, well_id in enumerate(wells_ids):
    print(f"Working on {well_id}")

    well_origin = well_id.split("_")[0]
    well_nr = int(well_id.split("_")[1])
    well_name = names[i]
    ditch_id = ditch_ids[i]
    ditch_name = ditch_names[i]
    ditch_origin = ditch_id.split("_")[0]
    ditch_nr = int(ditch_id.split("_")[1])
    surface_level = surface_levels[i]

    timeseries = find_timeseries_data(well_origin, well_nr)
    timeseries_ditch = find_timeseries_data(ditch_origin, ditch_nr)

    timeseries = timeseries.set_index("datetime")
    timeseries_ditch = timeseries_ditch.set_index("datetime")

    timeseries = timeseries["scalarvalue"]
    timeseries_ditch = timeseries_ditch["scalarvalue"]

    # make a simple figure #
    plot_gwlevel_timeseries(
        timeseries,
        title=f"Grondwaterstand - {well_name}",
        ditch_data=timeseries_ditch,
        ditch_title=f"Slootpeil - {ditch_name}",
        surface_level=surface_level,
    )

    # plt.show()
    plt.savefig(
        figdir.joinpath(f"{well_id}.png"),
        bbox_inches="tight",
        dpi=300,
    )

    plt.close()

print("Done")

## -- p.id=SWM for slootwatermeetpunt
# %%
