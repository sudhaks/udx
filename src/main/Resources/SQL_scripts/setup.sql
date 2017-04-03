
beeline -u jdbc:hive2://localhost:10000

beeline -u jdbc:hive2://localhost:10016


su hdfs
hdfs dfs -chmod -R 777 /
exit

hdfs dfs -put -f experiments-1.2.jar

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


