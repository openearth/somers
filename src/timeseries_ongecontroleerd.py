# %%
import os
from datetime import time
import pandas as pd
import configparser
import glob
import numpy as np
import matplotlib.pyplot as plt
import sys
from pathlib import Path
# Get the directory of the current script
current_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the root of the project
project_root = os.path.abspath(os.path.join(current_dir, '..'))

# Add the parent directory to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# Add the project root to the PYTHONPATH
sys.path.append(project_root)

# third party packages
from sqlalchemy import ARRAY,text
from ts_helpers.ts_helpers_waterschappen import (
    establishconnection,
    read_config,
    loadfilesource,
    location,
    sparameter,
    sserieskey,
    sflag,
    dateto_integer,
    convertlttodate,
    stimestep,
)

local = False
if local:
    # fc = r"C:\develop\somers\configuration_local.txt"
    fc = r'C:\projecten\groundwater\config_local_qsomers.txt'
else:
    # fc = r"C:\develop\somers\configuration_somers.txt"
    fc = r'C:\projecten\groundwater\config_online_qsomers.txt'
session, engine = establishconnection(fc)

#%%
sqlstr = text("""select * from metadata_ongecontroleerd.kalibratie""")
with engine.connect() as conn:
    df = pd.read_sql(sqlstr, conn)
#result is df met alle locaties
def find_timeseries_data(schema, locationkey):
    """Schema and location can either be derived from well_id or ditch_id.
    Parameter is GWM or SWM"""
    sqlstr = """select tsv.datetime, tsv.scalarvalue, l.name, l.locationkey from {schema}_timeseries.timeseriesvaluesandflags tsv
    join {schema}_timeseries.timeseries t on t.timeserieskey=tsv.timeserieskey
    join {schema}_timeseries.location l on l.locationkey=t.locationkey
    where l.locationkey={locationkey}
    """.format(schema=schema, locationkey=locationkey)

    with engine.connect() as conn:
        timeseries = pd.read_sql(sqlstr, conn)
    return timeseries


timeseries = find_timeseries_data("bro", 320)
print(timeseries.head(5))
timeseries = find_timeseries_data("nobv", 0)
print(timeseries.head(5))

## -- p.id=SWM for slootwatermeetpunt
# %%
