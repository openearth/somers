# -*- coding: utf-8 -*-
# Copyright notice
#   --------------------------------------------------------------------
#   Copyright (C) 2024, 2026 Deltares
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

# set of functions that gets data for every location available regarding a list of parameters:
# - for every location querys (spatial query) the input parcels
#%%
# import math
import time

# import StringIO
import os

from sqlalchemy import text

from ts_helpers.ts_helpers import establishconnection, testconnection
from db_helpers import preptable


# ----- set various generic (location dependend) data in metadata table (xy from well)
def assign_parcelvalues(engine, tbl, nwtbl):
    """Update metadata table with the input_parcels by performing a spatial query
    2026: input_parcels_2022 is deprecated, the new function uses b_2024_ahn3

    Args:
        cf  (string): link to connection file with credentials
        tbl (string): schema.table name with locations that act as basedata.

    Returns:
        ...
    """
    # Step 1: fetch data
    strsql = f"""
        SELECT 
            l.locationkey AS well_id, 
            ip.name AS aan_id, 
            ROUND(summer_stage::numeric, 2) AS summer_stage,
            ROUND(winter_stage::numeric, 2) AS winter_stage,
            ROUND(width::numeric, 2) AS width,
            ROUND(ip.x::numeric, 2) AS x,
            ROUND(ip.y::numeric, 2) AS y,
            ip.measure 
        FROM {tbl} l
        JOIN b_2024_ahn3 ip 
            ON ST_Within(l.geom, ip.geom)
    """

    with engine.begin() as connection:
        locs = connection.execute(text(strsql)).fetchall()

    # Step 2: upsert query (parameterized ✅)
    upsert_sql = f"""
        INSERT INTO {nwtbl} (
            well_id,
            aan_id, 
            x_centre_parcel,
            y_centre_parcel,
            parcel_width_m,
            summer_stage_m_nap,
            winter_stage_m_nap,
            measure
        ) 
        VALUES (
            :well_id,
            :aan_id,
            :x,
            :y,
            :width,
            :summer_stage,
            :winter_stage,
            :measure
        )
        ON CONFLICT (well_id)
        DO UPDATE SET
            aan_id = EXCLUDED.aan_id,
            x_centre_parcel = EXCLUDED.x_centre_parcel,
            y_centre_parcel = EXCLUDED.y_centre_parcel,
            parcel_width_m = EXCLUDED.parcel_width_m,
            summer_stage_m_nap = EXCLUDED.summer_stage_m_nap,
            winter_stage_m_nap = EXCLUDED.winter_stage_m_nap,
            measure = EXCLUDED.measure
    """

    # Step 3: execute updates safely
    with engine.begin() as connection:
        for row in locs:
            connection.execute(
                text(upsert_sql),
                {
                    "well_id": row[0],
                    "aan_id": row[1],  
                    "summer_stage": row[2],
                    "winter_stage": row[3],
                    "width": row[4],
                    "x": row[5],
                    "y": row[6],
                    "measure": row[7],
                },
            )



def test():
    cf = r"C:\develop\extensometer\connection_online.txt"
    session, engine = establishconnection(cf)
    tbl = "bro_timeseries.location_metadata2"

# %%
