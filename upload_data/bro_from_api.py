#  Copyright notice
#   --------------------------------------------------------------------
#   Copyright (C) 2023 Deltares
#                 Somers project with PostgreSQL/PostGIS database
#   Gerrit Hendriksen (gerrit.hendriksen@deltares.nl)
#
#   This library is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This library is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this library.  If not, see <http://www.gnu.org/licenses/>.
#   --------------------------------------------------------------------
#
# This tool is part of <a href="http://www.OpenEarth.eu">OpenEarthTools</a>.
# OpenEarthTools is an online collaboration to share and manage data and
# programming tools in an open source, version controlled environment.
# Sign up to recieve regular updates of this function, and to contribute
# your own tools.

# Gerrit Hendriksen
# retrieving timeseries data from waterboards from Lizard API (v4!)~
# before use, check current lizard version
# later, data will be put in a database

# %%
import os
import sys
import re
import pandas as pd
import requests
from datetime import datetime, timedelta
import numpy as np

# third party packages
from sqlalchemy.sql.expression import update
from sqlalchemy import exc, func, text
from sqlalchemy.dialects import postgresql
from sqlalchemy import text
import hydropandas as hpd

# Add the parent directory to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# local procedures
from orm_timeseries.orm_timeseries_bro import (
    Base,
    FileSource,
    Location,
    Parameter,
    Unit,
    TimeSeries,
    TimeSeriesValuesAndFlags as tsv,
    Flags,
)
from ts_helpers.ts_helpers_bro import (
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
    convertdatetostring,
)


# %%
# ----------------postgresql connection
# data is stored in PostgreSQL/PostGIS database. A connection string is needed to interact with the database. This is typically stored in
# a file.

local = False
if local:
    fc = r"C:\develop\somers\configuration_local.txt"
else:
    fc = r"C:\develop\somers\configuration_somers.txt"
session, engine = establishconnection(fc)


def normalize_gmw_id(raw_id):
    """Extract normalized GMW id from database identifier fields.

    Accepts values like 'GMW000000000001' or strings that contain this id
    (for example local ids with namespace prefixes).
    """
    if raw_id is None:
        return None
    match = re.search(r"(GMW\d{12})", str(raw_id).upper())
    if match:
        return match.group(1)
    return None


def lastgwstage(engine, bro_id, t, pid, fid):
    """Retrieves last entrance in the database for the given combination of BROid, filesourckey and paramaterkey

    Args:
        brolocation (string): location of bro_id, incl. filternumber
        pid (integer): parameterkey
        fid (integer): filesourckey
    """
    strsql = f"""
    select max(datetime) from bro_timeseries.location l
    join bro_timeseries.timeseries ts on ts.locationkey = l.locationkey
    join bro_timeseries.parameter p on p.parameterkey = ts.parameterkey
    join bro_timeseries.filesource f on f.filesourcekey = ts.filesourcekey
    join bro_timeseries.timeseriesvaluesandflags tsf on tsf.timeserieskey = ts.timeserieskey
    where l.name = '{bro_id}_{t}' and f.filesourcekey = {fid} and p.parameterkey = {pid}
    """
    with engine.connect() as conn:
        ld = conn.execute(text(strsql)).fetchall()
    adate = ld[0][0]
    if adate is None:
        strdate = None
    else:
        adate = adate + timedelta(hours=2)
        strdate = adate.strftime("%Y-%m-%d")
        print(bro_id, strdate)
    return strdate


# %%
# set parameter, timeseries and flag
flagid = sflag(fc, "goedgekeurd")
fid = loadfilesource("BRO data", fc, remark="derived with Hydropandas package")[0][0]
pid = sparameter(fc, "grondwater", "grondwater", ("stand", "m-NAP"), "grondwater")


# %%
# First get a list of BRO id's from the table with the following requirements:
# - veenparcel = True
# - removode = 'nee'
# With this priority list, data will be retrieved from BRO and loaded into the database.
# in version 2026, the number of tubes is not available in the database, and the bro_id is ... similar to gml_id.
version = 2026
if version != 2026:
    strSql = """select bro_id,number_of_monitoring_tubes from bro_timeseries.groundwater_monitoring_well 
            where veenperceel and removed = 'nee'"""
else:   
    strSql = """select localid as bro_identifier from bro_timeseries.groundwater_monitoring_well 
            where veenperceel 
            and localid not in (select replace(name,'_','_00') from bro_timeseries.location)"""
updatedb = True  # in this case there is already data available, data will be updated record by record
# set to False if complete reload of the BRO data is necessary

with engine.connect() as conn:
    res = conn.execute(text(strSql))
    for i in res:
        raw_bro_id = i[0]
        #bro_id = normalize_gmw_id(raw_bro_id)
        if raw_bro_id is None:
            print(f"Skipping unsupported BRO id format: {raw_bro_id}")
            continue
        
        raw_bro_id = i[0]
        bro_id = raw_bro_id.split('_')[0]
        t = int(raw_bro_id.split('_')[1])
        
        # determine last date in table
        lastdate = lastgwstage(engine, bro_id, t, pid, fid)
        print(bro_id, t, lastdate)
        if lastdate is None:
            lastdate = "2010-01-01"
        # by getting lastdata and using that in the request, it is possible to only 
        # retrieve new data from BRO, which is more efficient than retrieving all 
        # data and then checking which data is new. So if the request is hindered by anything
        # it is easy to start all over again, without the need to check which data is already in the database and which not.
        try:
            gw_bro = hpd.GroundwaterObs.from_bro(bro_id, t, tmin=lastdate)
            is_empty = gw_bro.empty
            if is_empty:
                print(f"No data for {bro_id} since {lastdate}")
                continue
            descr = gw_bro.describe()
            cnt = descr["values"]["count"]
            if cnt == 0:
                print(f"No new data for {bro_id} since {lastdate}")
                continue
        except Exception as e:
            print(f"BRO download failed for {bro_id} (source value: {raw_bro_id}): {e}")
            continue
        if cnt > 0:
            print(f"adding {cnt} values for BROID {gw_bro.name}")
            lid = location(
                fc,
                fid,
                name=gw_bro.name,
                x=gw_bro.x,
                y=gw_bro.y,
                filterid=int(gw_bro.tube_nr),
                epsg="EPSG:28992",
                shortname=gw_bro.filename,
                description="",
                altitude_msl=gw_bro.ground_level,
                z=gw_bro.tube_top,
                tubetop=gw_bro.screen_top,
                tubebot=gw_bro.screen_bottom,
            )

            sid = sserieskey(fc, pid, lid, fid, "nonequidistant")
            dfval = gw_bro.copy(deep=True)
            dfval["timeserieskey"] = sid
            dfval["flags"] = flagid
            dfval.rename(columns={"values": "scalarvalue"}, inplace=True)
            dfval.index.name = "datetime"
            dfval.reset_index(level=["datetime"], inplace=True)
            dfval.drop(["qualifier"], axis=1, inplace=True)
            dfval.dropna(inplace=True)
            if len(dfval) > 0 and updatedb:
                for i, r in dfval.iterrows():
                    date_time_obj = r["datetime"]
                    vl = r["scalarvalue"]
                    flagid = r["flags"]
                    sid = r["timeserieskey"]
                    anid = (
                        session.query(tsv)
                        .filter_by(
                            timeserieskey=sid, datetime=date_time_obj, scalarvalue=vl
                        )
                        .first()
                    )
                    if anid == None:
                        # print(
                        #     "adding:",
                        #     r["datetime"],
                        #     r["scalarvalue"],
                        #     r["timeserieskey"],
                        #     r["flags"],
                        # )
                        insert = tsv(
                            timeserieskey=sid,
                            datetime=date_time_obj,
                            scalarvalue=vl,
                            flags=flagid,
                        )
                        session.merge(insert)
                        session.commit()
            # in case complete redo of the data then updatedb = false
            elif len(dfval) > 0 and not updatedb:
                dfval.to_sql(
                    "timeseriesvaluesandflags",
                    engine,
                    if_exists="append",
                    schema="bro_timeseries",
                    index=False,
                    method="multi",
                )

# %%
strsql = 'reassign owned by hendrik_gt to qsomers'
with engine.connect() as conn:
    conn.execute(text(strsql))
# %%
