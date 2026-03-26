"""
Declaration via ORM mapping of Subsurface datamodel, including FEWS time series
compatibla datamodel, requires:
Pyhton packages
 - sqlalchemy
 - geoalchemy2
PostgreSQL/PostGIS
 - schema fews
 - schema borehole
"""

#  Copyright notice
#   --------------------------------------------------------------------
#   Copyright (C) 2026 Deltares for SOMERS project
#                 PostgreSQL/PostGIS database used in Water Information Systems
#   2026  --> prepare BRO GMW selection (indicate veenperceel, distance to roads, railways, ditches)
#
#   Gerrit Hendriksen@deltares.nl
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

import os
import sys

# Add the parent directory to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

## Connect to the DB
from sqlalchemy import create_engine, text
from ts_helpers.ts_helpers import establishconnection

local = False
if local:
    fc = r"C:\develop\somers\configuration_local.txt"
else:
    fc = r"C:\develop\somers\configuration_somers.txt"
session, engine = establishconnection(fc)


tblname = "groundwater_monitoring_well"
schema = "bro_timeseries"

strsql = f"""
ALTER TABLE {schema}.{tblname}
    ADD COLUMN IF NOT EXISTS veenperceel boolean,
    ADD COLUMN IF NOT EXISTS wl_dist_m numeric,
    ADD COLUMN IF NOT EXISTS rd_dist_m numeric,
    ADD COLUMN IF NOT EXISTS rr_dist_m numeric;
"""

with engine.begin() as conn:
    conn.execute(text(strsql))

# add indication of whether the groundwater monitoring well is located in a veenperceel, by checking if it intersects with a veenperceel in the input_parcels_2022 table
strsql = f"""
UPDATE bro_timeseries.groundwater_monitoring_well
SET veenperceel = EXISTS (
    SELECT 1
    FROM public.b_2024_ahn3
    WHERE ST_Intersects(
        ST_Transform(bro_timeseries.groundwater_monitoring_well.geometry, 28992),
        public.b_2024_ahn3.geom)
);"""
with engine.begin() as conn:
    conn.execute(text(strsql))

# update groundwater_monitoring_well with distance to railways
# first set all distances to null, then calculate distance to railways, roads and ditches
strsql = """UPDATE bro_timeseries.groundwater_monitoring_well set rr_dist_m = Null"""
with engine.begin() as conn:
    conn.execute(text(strsql))      

strsql = """UPDATE bro_timeseries.groundwater_monitoring_well AS pt
  SET  rr_dist_m = (
    SELECT ST_Distance(st_transform(pt.geometry,28992), ln.geom)
    FROM   top10.top10nl_spooras AS ln
    ORDER BY
           st_transform(pt.geometry,28992) <-> ln.geom
    LIMIT  1
  )
  where veenperceel
;""""

with engine.begin() as conn:
    conn.execute(text(strsql))      

# first set all distances to null, then calculate distance to roads, railways and ditches
"""UPDATE bro_timeseries.groundwater_monitoring_well set wl_dist_m = Null;"""
with engine.begin() as conn:
    conn.execute(text(strsql))  

# calculate distance to ditches
strsql = """UPDATE bro_timeseries.groundwater_monitoring_well AS pt
  SET  wl_dist_m = (
    SELECT ST_Distance(st_transform(pt.geometry,28992), ln.geom)
    FROM   top10.top10nl_waterdeel_lijn AS ln
    ORDER BY
           st_transform(pt.geometry,28992) <-> ln.geom
    LIMIT  1
  )
  where veenperceel
;"""
with engine.begin() as conn:
    conn.execute(text(strsql))

# first set all distances to null, then calculate distance to roads, railways and ditches
strsql = """UPDATE bro_timeseries.groundwater_monitoring_well set rd_dist_m = Null"""
with engine.begin() as conn:
    conn.execute(text(strsql))      

# calculate distance to roads
strsql = """UPDATE bro_timeseries.groundwater_monitoring_well AS pt
  SET  rd_dist_m = (
    SELECT ST_Distance(st_transform(pt.geometry,28992), ln.geom)
    FROM   top10.top10nl_wegdeel_hartlijn AS ln
    ORDER BY
           st_transform(pt.geometry,28992) <-> ln.geom
    LIMIT  1
  )
  where veenperceel
;"""
 
with engine.begin() as conn:
    conn.execute(text(strsql))  