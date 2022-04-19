# steps to do in a Hortonworks HDP 2.5.x  cluster with Spark2 installed

1.  Create auxlib folder for hive, example: /usr/hdp/2.5.3.0-37/hive/auxlib

2.  Copy spark2 jars into it, from /usr/hdp/2.5.3.0-37/spark2/jars/*.jar
    >cp /usr/hdp/2.5.3.0-37/spark2/jars/*.jar /usr/hdp/2.5.3.0-37/hive/auxlib
    
    
3.  Create example jar:
    >mvn clean package -DskipTests
    
4.  Copy target/examples-1.2.jar into /usr/hdp/2.5.3.0-37/hive/auxlib


5.  Copy target/examples-1.2.jar into hdfs:///user/root


6.  beeline -u jdbc:hive2://localhost:10000/ -n hive

7.  Create a table and load a value, then register the spark UDTF,  which access this table:
    >CREATE TABLE TE (i int);
    
    >INSERT INTO TE VALUES (99);
    
    >DROP FUNCTION SampleSparkUDTF_yarnV1_01;

    >CREATE FUNCTION SampleSparkUDTF_yarnV1_01 AS 'com.fuzzylogix.experiments.udf.hiveSparkUDF.SampleSparkUDTF_yarnV1' USING JAR 'hdfs:///user/root/experiments-1.2.jar';

    >DESCRIBE FUNCTION SampleSparkUDTF_yarnV1_01;

    >SELECT SampleSparkUDTF_yarnV1_01('table name', 'TE');
    
    
8.  Expected result:
    SampleSparkUDTF_yarnv1_01 to complete successfully.
