# -*- coding: utf-8 -*-
# Copyright notice
#   --------------------------------------------------------------------
#   Copyright (C) 2024 Deltares
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

#%%
## some helper functions
# Add the parent directory to the system path
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


from ts_helpers.ts_helpers import establishconnection, testconnection
from db_helpers import create_location_metadatatable, tablesetup
from sqlalchemy import text
import assign_soiltype
import assign_parcelvalues
import assign_ahn4
import assign_top10
import assign_timeseriesstats

# globals
cf = r"C:\projecten\groundwater\config_online_qsomers.txt"
# cf = r"C:\develop\extensometer\connection_online.txt"

session, engine = establishconnection(cf)

# -------------- section for regiodeal data
# step 1. setup location_metadata table
# step 2. fill with location data for selection of data
# step 3. assign ahn4
# step 4. assign soiltype
# step 5. assign parcelvalues
# step 6. assing top10
# step 7. compile info to location.metadata table

# 1 setup metadata table (tbl should be new name)
tbl = "regiodeal_timeseries.location"
nwtbl = "regiodeal_timeseries.location_metadata2"
dctcolumns = tablesetup()
create_location_metadatatable(cf, nwtbl,dctcolumns)

#location_metadata2 needs a primary key

strsql = f"""alter table {nwtbl} add primary key (well_id)"""
with engine.begin() as connection:
    connection.execute(text(strsql))

# 2 data source specific
# this part is different for every source, since the data is not exactly the same
# for each data source, all values are with respect to reference level (m-NAP), while model expects m-mv
strsql = """
SELECT 
	locationkey,
	st_x(geom),
	st_y(geom),
	altitude_msl as z_surface_level_m_nap,
	tubetop as screen_top_m_sfl,
	tubebot as screen_bot_m_sfl
FROM regiodeal_timeseries.location
order by locationkey
"""
with engine.begin() as connection:
    locs = connection.execute(text(strsql)).fetchall()


strsql = text(f"""
INSERT INTO {nwtbl}
(well_id, x_well, y_well, z_surface_level_m_nap, screen_top_m_sfl, screen_bot_m_sfl)
VALUES (:lockey, :x, :y, :z, :zt, :zb)
ON CONFLICT (well_id)
DO UPDATE SET
    x_well = :x,
    y_well = :y,
    z_surface_level_m_nap = :z,
    screen_top_m_sfl = :zt,
    screen_bot_m_sfl = :zb
""")
try:
    for loc in locs:
        params = {
            "lockey": loc[0],
            "x": loc[1],
            "y": loc[2],
            "z": loc[3],
            "zt": loc[4],
            "zb": loc[5],
        }

        with engine.begin() as connection:
            connection.execute(strsql, params)

except Exception as e:
    # Handle the conflict (e.g., log the error or ignore it)
    print(f"Error: {e}. {loc[0]}.")

# # create list to loop over
# rename_cols = [ ##oud denk ik dit dus volgens mij niet meer nodig maar laat het even staan
#     "parcel_width_m",
#     "trenches",
#     "trench_depth_m_sfl",
#     "summer_stage_m_nap",
#     "winter_stage_m_nap",
#     "wis_distance_m",
#     "wis_depth_m_sfl",
# ]
# for i in range(len(rename_cols)):
#     strsql = f"""
#     UPDATE regiodeal_timeseries.location_metadata2 m2
#     SET {rename_cols[i]} = m1.{rename_cols[i]}
#     FROM regiodeal_timeseries.location_metadata m1
#     WHERE m1.well_id = m2.well_id
#     """
#     with engine.begin() as connection:
#         connection.execute(text(strsql))

# 3 assign ahn4 (needs some small changes to get it working)

# need of geometry column for conversion to Lat-long, it is expected that geom is in 28992
assign_ahn4.assign_ahn(engine, tbl, nwtbl)

# 4 assign soiltype
assign_soiltype.assign_soiltype(engine, nwtbl)

# 5 assign parcelvalues
assign_parcelvalues.assign_parcelvalues(engine, tbl, nwtbl)
print('assigned parcel values')

#5.5 extra needed for saving the parcel_width_m data
# strsql = """   -> volgens mij niet meer nodig want parcel_width_m komt nu mee bij assign_parcelvalues
# SELECT 
# 	well_id,
# 	parcel_width_m
# FROM regiodeal_timeseries.location_metadata
# order by well_id
# """
# with engine.begin() as connection:
#     locs = connection.execute(text(strsql)).fetchall()
# for i in range(len(locs)):
#     lockey = locs[i][0]
#     p = locs[i][1]
#     try:
#         strsql = f"""insert into {nwtbl} (well_id,parcel_width_m) 
#                     VALUES ({lockey},{p})
#                     ON CONFLICT(well_id)
#                     DO UPDATE SET
#                     parcel_width_m = {p}"""
#         with engine.begin() as connection:
#             connection.execute(text(strsql))
#     except Exception as e:
#         # Handle the conflict (e.g., log the error or ignore it)
#         print(f"Error: {e}. {lockey}.")

# 6 assign_top10
assign_top10.assign_t10(engine, tbl, nwtbl)

# 7 assign timeseries timewindow and number of records
assign_timeseriesstats.settimeseriesstats(engine, tbl, nwtbl)


# %%
