-- calculate distance to rail roads
UPDATE bro_timeseries.groundwater_monitoring_well set rr_dist_m = Null
UPDATE bro_timeseries.groundwater_monitoring_well AS pt
  SET  rr_dist_m = (
    SELECT ST_Distance(st_transform(pt.geometry,28992), ln.geom)
    FROM   top10.top10nl_spooras AS ln
    ORDER BY
           st_transform(pt.geometry,28992) <-> ln.geom
    LIMIT  1
  )
  where veenperceel
;

-- calculate distance to waterlopen
UPDATE bro_timeseries.groundwater_monitoring_well set wl_dist_m = Null;
UPDATE bro_timeseries.groundwater_monitoring_well AS pt
  SET  wl_dist_m = (
    SELECT ST_Distance(st_transform(pt.geometry,28992), ln.geom)
    FROM   top10.top10nl_waterdeel_lijn AS ln
    ORDER BY
           st_transform(pt.geometry,28992) <-> ln.geom
    LIMIT  1
  )
  where veenperceel
;

-- calculate distance to road
UPDATE bro_timeseries.groundwater_monitoring_well set rd_dist_m = Null;
UPDATE bro_timeseries.groundwater_monitoring_well AS pt
  SET  rd_dist_m = (
    SELECT ST_Distance(st_transform(pt.geometry,28992), ln.geom)
    FROM   top10.top10nl_wegdeel_hartlijn AS ln
    ORDER BY
           st_transform(pt.geometry,28992) <-> ln.geom
    LIMIT  1
  )
  where veenperceel
;