-- the previous primary key was not allowing duplicates in the datetime columns, which could exist but not in combination with scalarvalue, so
-- the primary key is altered and completed with scalarvalue
ALTER TABLE bro_timeseries.timeseriesmanualeditshistory 
DROP CONSTRAINT timeseriesmanualeditshistory_pkey CASCADE,
ADD PRIMARY KEY (timeserieskey, editdatetime, datetime, scalarvalue);

-- copy all data to the timeseriesmanualeditshistory table
INSERT INTO bro_timeseries.timeseriesmanualeditshistory (timeserieskey, editdatetime, datetime, userkey, scalarvalue, flags, commenttext)
SELECT timeserieskey, CURRENT_TIMESTAMP, datetime, 1, scalarvalue, flags, 'copy of the data of 2023'
FROM bro_timeseries.timeseriesvaluesandflags

