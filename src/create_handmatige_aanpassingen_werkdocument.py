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
    "p:/11207812-somers-ontwikkeling/3-somers_development/QSOMERS/Dataverzameling 2026/Dataverzameling/Figuren_timeseries_metadata_ongecontroleerd/"
)

fc = "p:/11207812-somers-ontwikkeling/3-somers_development/QSOMERS/credentials/connection_online_jr.txt"
session, engine = establishconnection(fc)

# %%
sqlstr = text("""select * from metadata_ongecontroleerd.kalibratie""")
with engine.connect() as conn:
    df = pd.read_sql(sqlstr, conn)
# result is df met alle locaties

df["source"] = df["well_id"].str.split("_").str[0]

selected_columns = [
    "well_id",
    "name_bgt",
    "name",
    "transect",
    "measure",
    "ditch_id",
    "ditch_name",
    "soil_class",
    "altitude_m_nap",
    "ahn4_m_nap",
    "start_date",
    "end_date",
    "parcel_width_m",
    "distance_to_ditch_m",
    "trenches",
    "trench_depth_m_sfl",
    "wis_distance_m",
    "wis_depth_m_sfl",
    "distance_to_wis_m",
    "selection",
    "description",
    "source"
]

df_selection = df[selected_columns]


df_2024 = pd.read_excel(
    r"p:\11207812-somers-ontwikkeling\3-somers_development\QSOMERS\Dataverzameling 2024\handmatige_aanpassingen\handmatige_aanpassingen_kalibratie_werkdocument.xlsx"
)

outputpath = Path("p:/11207812-somers-ontwikkeling/3-somers_development/QSOMERS/Dataverzameling 2026/handmatige_aanpassingen/handmatige_aanpassingen_2026_werkdocument.xlsx")

df_selection.to_excel(outputpath, index=False)

print("Done")

## -- p.id=SWM for slootwatermeetpunt
# %%
