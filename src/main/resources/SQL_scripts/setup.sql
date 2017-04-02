

beeline -u jdbc:hive2://localhost:10000 -n hive -pw "" -d org.apache.hive.jdbc.HiveDriver

beeline -u jdbc:hive2://localhost:10016 -n hive -pw "" -d org.apache.hive.jdbc.HiveDriver


su hdfs
hdfs dfs -chmod -R 777 /
exit

------------------------------------------------------------
------------------------------------------------------------

--- Create Sample Data Table

DROP TABLE sampleData;
CREATE TABLE sampleData(
    obsID int,
    dimID int,
    Value double)
row format delimited fields terminated by '|';

-- Insert data into the Sample Data table

load data local inpath 'sampledata.dat' OVERWRITE into table sampleData;

SELECT *
FROM sampleData LIMIT 10;

SELECT count(*)
FROM sampleData;

