# -*- coding: utf-8 -*-
# Copyright notice
#   --------------------------------------------------------------------
#   Copyright (C) 2024 Deltares
#   Gerrit Hendriksen (gerrit.hendriksen@deltares.nl)
#   Nathalie Dees (nathalie.dees@deltares.nl)
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
# - derivation of AHN4 surface levels (DTM)
# - assignment of SOILTYPE from locally loaded SOILMAP
# - assignment of parcelwidth and distance of ditches?
# - assignment distance to roads or waterbodies

# %%
from sqlalchemy import text
from ts_helpers.ts_helpers import establishconnection, testconnection
from db_helpers import preptable, tablesetup, create_location_metadatatable

# globals
# cf = r"C:\develop\extensometer\connection_online.txt"
# cf = r"C:\projecten\grondwater_monitoring\nobv\2023\connection_online_qsomers.txt"
cf = r"C:\projecten\groundwater\config_online_qsomers.txt"

session, engine = establishconnection(cf)

if not testconnection(engine):
    print("Connecting to database failed")

# for security, remove _old tables and copy current (if exists) tables to _old
lsttables = ["gwm", "swm", "kalibratie", "validatie"]
schema = "metadata_ongecontroleerd"
for tbl in lsttables:
    strsql = f"drop table if exists {schema}.{tbl}_old"
    with engine.begin() as connection:
        connection.execute(text(strsql))
    strsqln = f"alter table if exists {schema}.{tbl} rename to {tbl}_old"
    with engine.begin() as connection:
        connection.execute(text(strsqln))

# create table
nwtbl = "metadata_ongecontroleerd.gwm"
dctcolumns = tablesetup()
create_location_metadatatable(cf, nwtbl, dctcolumns)
print("table created", nwtbl)
preptable(engine, nwtbl, "name", "text")
preptable(engine, nwtbl, "source", "text")
preptable(engine, nwtbl, "geom", "geometry(POINT, 28992)")

strsql = f"""alter table {nwtbl} alter column well_id TYPE text"""
with engine.begin() as connection:
    connection.execute(text(strsql))

# setup dcttable with tables
dcttable = {}
dcttable["bro_timeseries.location"] = "placeholder"
# dcttable["hdsr_timeseries.location"] = "placeholder"
# dcttable["hhnk_timeseries.location"] = "placeholder"
# dcttable["wskip_timeseries.location"] = "placeholder"
dcttable["regiodeal_timeseries.location"] = "placeholder"
dcttable["waterschappen_timeseries.location"] = "placeholder"  # handmetingen
dcttable["nobv_timeseries.location"] = "placeholder"  # nobv handmatige bewerkingen data

# retrieve for every table in the dicttable all relevant data and transfer to nwtbl
for tbl in dcttable.keys():
    n = tbl.split("_")[0]
    print("attempt to exectute queries for", n)
    # NOBV and Waterschappen can have multipe parameters per location, only GWM now required.
    if n == "nobv" or n == "waterschappen" or n == 'regiodeal':
        strsql = f"""insert into {nwtbl} (well_id, 
            name_bgt,
            name,
            transect,
            measure,
            ditch_id,
            ditch_name,
            soil_class,
            z_surface_level_m_nap,
            ahn4_m_nap,
            start_date,
            end_date,
            records,
            parcel_width_m,
            summer_stage_m_nap,
            winter_stage_m_nap,
            x_well,
            y_well,
            distance_to_ditch_m,
            trenches,
            trench_depth_m_sfl,
            wis_distance_m,
            wis_depth_m_sfl,
            distance_to_wis_m,
            screen_top_m_sfl,
            screen_bot_m_sfl,
            altitude_m_nap,
            geometry, 
            parcel_geom,
            selection,
            description)
        SELECT ('{n}_'||l.locationkey::text) as well_id, 
            i.name::text,
            l.name, 
            mt.transect::integer,
            mt.measure as measure,
            mt.ditch_id,
            '' as ditch_name, 
            i.soilcode as soil_class,
            mt.z_surface_level_m_nap as z_surface_level_m_nap, 
            mt.ahn4_m_nap as ahn4_m_nap, 
            mt.start_date,
            mt.end_date,
            mt.records,
            mt.parcel_width_m, 
            mt.summer_stage_m_nap,
            mt.winter_stage_m_nap, 
            st_x(l.geom),
            st_y(l.geom),            
            mt.distance_to_ditch_m,
            mt.trenches,
            mt.trench_depth_m_sfl,
            mt.wis_distance_m,
            mt.wis_depth_m_sfl,
            Null::double precision as distance_to_wis_m,
            l.tubetop as screen_top_m_sfl, 
            l.tubebot as screen_bot_m_sfl,
            l.altitude_msl as altitude_m_nap,
            l.geom,
            st_astext(st_force2d(i.geom)),
            'yes' as selection,
            l.description
            FROM {n}_timeseries.location l
            JOIN {n}_timeseries.location_metadata2 mt on mt.well_id = l.locationkey
            JOIN {n}_timeseries.timeseries t on t.locationkey = l.locationkey
            JOIN {n}_timeseries.parameter p on p.parameterkey = t.parameterkey
            JOIN public.b_2024_ahn3 i on st_within(l.geom,i.geom)
            where p.id = 'GWM' and mt.distance_to_railroad_m > 10 and mt.distance_to_road_m > 10 and mt.distance_to_ditch_m > 5
            ON CONFLICT(well_id)
            DO NOTHING;"""
        with engine.begin() as connection:
            connection.execute(text(strsql))

    else:
        strsql = f"""insert into {nwtbl} (well_id, 
            name_bgt,
            name,
            transect,
            measure,
            ditch_id,
            ditch_name,
            soil_class,
            z_surface_level_m_nap,
            ahn4_m_nap,
            start_date,
            end_date,
            records,
            parcel_width_m,
            summer_stage_m_nap,
            winter_stage_m_nap,
            x_well,
            y_well,
            distance_to_ditch_m,
            trenches,
            trench_depth_m_sfl,
            wis_distance_m,
            wis_depth_m_sfl,
            distance_to_wis_m,
            screen_top_m_sfl,
            screen_bot_m_sfl,
            altitude_m_nap,
            geometry, 
            parcel_geom,
            selection,
            description)
        SELECT ('{n}_'||l.locationkey::text) as well_id, 
            i.name::text,
            l.name, 
            mt.transect::integer,
            mt.measure as measure,
            mt.ditch_id,
            '' as ditch_name, 
            i.soilcode as soil_class,
            Null::double precision as z_surface_level_m_nap, 
            mt.ahn4_m_nap as ahn4_m_nap, 
            mt.start_date,
            mt.end_date,
            mt.records,
            mt.parcel_width_m, 
            mt.summer_stage_m_nap,
            mt.winter_stage_m_nap, 
            st_x(l.geom),
            st_y(l.geom),            
            mt.distance_to_ditch_m,
            mt.trenches,
            mt.trench_depth_m_sfl,
            mt.wis_distance_m,
            mt.wis_depth_m_sfl,
            Null::double precision as distance_to_wis_m,
            l.tubetop as screen_top_m_sfl, 
            l.tubebot as screen_bot_m_sfl,
            l.altitude_msl as altitude_m_nap,
            l.geom,
            st_astext(st_force2d(i.geom)),
            'yes' as selection,
            l.description
            FROM {n}_timeseries.location l
            JOIN {n}_timeseries.location_metadata2 mt on mt.well_id = l.locationkey
            JOIN {n}_timeseries.timeseries t on t.locationkey = l.locationkey
            JOIN {n}_timeseries.parameter p on p.parameterkey = t.parameterkey
            JOIN public.b_2024_ahn3 i on st_within(l.geom,i.geom)
            where mt.distance_to_railroad_m > 10 and mt.distance_to_road_m > 10 and mt.distance_to_ditch_m > 5
            ON CONFLICT(well_id)
            DO NOTHING;"""
        with engine.begin() as connection:
            connection.execute(text(strsql))

nwtbl = "metadata_ongecontroleerd.swm"
strsql = f"""drop table if exists {nwtbl}; 
create table if not exists {nwtbl} (well_id text primary key)"""
with engine.begin() as connection:
    connection.execute(text(strsql))

print("table created", nwtbl)
preptable(engine, nwtbl, "name", "text")
preptable(engine, nwtbl, "geom", "geometry(POINT, 28992)")

for tbl in dcttable.keys():
    n = tbl.split("_")[0]
    if n == "nobv" or n == "waterschappen" or n == 'regiodeal':
        print(n)
        strsql = f"""insert into {nwtbl} (well_id, name, geom)
            SELECT ('{n}_'||l.locationkey::text) as well_id, l.name, l.geom FROM {n}_timeseries.location l
            JOIN {n}_timeseries.location_metadata2 mt on mt.well_id = l.locationkey
            JOIN {n}_timeseries.timeseries t on t.locationkey = l.locationkey
            JOIN {n}_timeseries.parameter p on p.parameterkey = t.parameterkey where p.id = 'SWM'
            ON CONFLICT(well_id)
            DO NOTHING;"""
        with engine.begin() as connection:
            connection.execute(text(strsql))


# %%
# !!!! query does not work inside of python, but does work in pg admin. Run this part in PG admin
strsql = f"""WITH updated_values AS (
    SELECT DISTINCT ON (l.well_id) 
        l.well_id AS all_source, 
        swm.well_id AS swm_source,
        swm.name as ditch_name
    FROM
        public.peilbesluiten2024 p
    JOIN
        metadata_ongecontroleerd.gwm l ON ST_DWithin(l.geometry, p.geom, 0)
    LEFT JOIN
        metadata_ongecontroleerd.swm swm ON ST_DWithin(swm.geom, p.geom, 0)
    ORDER BY 
        l.well_id
)
UPDATE metadata_ongecontroleerd.gwm
SET ditch_id = CASE 
                WHEN updated_values.swm_source IS NULL THEN NULL 
                ELSE updated_values.swm_source 
            END,
 ditch_name = CASE 
                WHEN updated_values.ditch_name IS NULL THEN NULL 
                ELSE updated_values.ditch_name 
            END
FROM updated_values
WHERE metadata_ongecontroleerd.gwm.well_id = updated_values.all_source;"""
with engine.begin() as connection:
    connection.execute(text(strsql))

# %%
strsql = f"""drop table if exists metadata_ongecontroleerd.kalibratie; 
create table metadata_ongecontroleerd.kalibratie as
select * from metadata_ongecontroleerd.gwm
where ditch_id is not Null;"""
with engine.begin() as connection:
    connection.execute(text(strsql))

strsql = f"""drop table if exists metadata_ongecontroleerd.validatie;
create table metadata_ongecontroleerd.validatie as
select * from metadata_ongecontroleerd.gwm
where ditch_id is Null;"""
with engine.begin() as connection:
    connection.execute(text(strsql))

print("created table kalibratie, validatie")

# bear in mind ownership of the tables
# does not work inside python and needs to be done in pgadmin
user = "dees"
strsql = f"reassign owned by {user} to qsomers"
with engine.begin() as connection:
    connection.execute(text(strsql))

# %%
