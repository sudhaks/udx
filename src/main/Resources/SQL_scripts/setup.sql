
beeline -u jdbc:hive2://localhost:10000

beeline -u jdbc:hive2://localhost:10016


su - hdfs -c "hdfs dfs -chmod -R 777 /"

hdfs dfs -mkdir /user/root
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
hdfs dfs -put -f sampledata.dat

load data inpath 'hdfs:///user/root/sampledata.dat' OVERWRITE into table sampleData;

SELECT *
FROM sampleData LIMIT 10;

SELECT count(*)
FROM sampleData;


