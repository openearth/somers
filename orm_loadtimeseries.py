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
#   Copyright (C) 2021, 2026 Deltares for Projects with a FEWS datamodel in 
#                 PostgreSQL/PostGIS database used in Water Information Systems
#   2026 update by Gerrit Hendriksen --> update to current way of executing sqls with SQLAlchemy of Python (3.11)
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

## Declare a Mapping to the database
from orm_timeseries.orm_timeseries_wskip import Base

def checkschema(engine,schema):
    strsql = f"create schema if not exists {schema}"
    with engine.begin() as conn:
        conn.execute(text(strsql))

def readcredentials(fc):
    with open(fc) as f:
        engine = create_engine(f.read().strip(), echo=False)
    return engine

# function to create the database, bear in mind, only to be executed when first started
def createdb(engine):
    ## Create the Table in the Database
    Base.metadata.create_all(engine)

# drop (delete) database
def dropdb(engine):
    Base.metadata.drop_all(engine)

def resetindex(engine,schema):
    strsql = text(
        """
        SELECT tablename, indexname, indexdef
        FROM pg_indexes
        WHERE schemaname = :schema
        ORDER BY tablename, indexname
        """
    )
    with engine.begin() as conn:
        lst = conn.execute(strsql, {"schema": schema})
        for i in lst:
            print(i.tablename, i.indexname)
            strSql = f"ALTER SEQUENCE {schema}.{i.indexname} RESTART WITH 1"
            conn.execute(text(strSql))
        
if __name__ == "__main__":
    local = False
    if local:
        fc = r"C:\develop\somers\configuration_local.txt"
    else:
        fc = r"C:\develop\somers\configuration_somers.txt"
    engine = readcredentials(fc)
    #when multiple schemas
    # schemas = ('subsurface_second')
    # for schema in schemas:
    #     checkschema(engine,schema)
    lschema = ('wskip_timeseries',)
    for schema in lschema:
        checkschema(engine,schema)
    # format is #postgres://user:password@hostname/database (in this case hydrodb)    
    dropdb(engine)    
    createdb(engine) # bear in mind deletes database before creation


