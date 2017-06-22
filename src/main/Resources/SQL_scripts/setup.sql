
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
  ROW format delimited fields terminated by '|';

INSERT INTO sampleData
SELECT *
FROM ( SELECT stack(4,
       1,1,101.01,
       1,2,123.23,
       2,1,34.34,
       2,3,56.76)  ) a;


SELECT * FROM sampleData;
SELECT count(*) FROM sampleData;


-- Put a cample csv file for testing
hdfs dfs -put -f samplefile.csv
su - hdfs -c "hdfs dfs -chmod -R 777 /"


