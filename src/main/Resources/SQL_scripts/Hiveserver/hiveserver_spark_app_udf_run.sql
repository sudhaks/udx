

-- beeline -u jdbc:hive2://localhost:10000

===========================================================
--        HiveServer : Plain Hive UDTF
===========================================================


DROP FUNCTION SampleUDTF_01;

CREATE FUNCTION SampleUDTF_01 AS 'com.fuzzylogix.experiments.udf.hiveUDF.SampleUDTF'
USING JAR 'hdfs:///user/root/experiments-1.2.jar';

DESCRIBE FUNCTION SampleUDTF_01;

SELECT SampleUDTF_01('Paris', 3, 2);


===========================================================
--        HiveServer : Hive UDTFs with Spark App
===========================================================

------------ UDTF Exmaple: Yarn Spark App : Reading table -------------

DROP FUNCTION SampleSparkUDTF_yarnV1_01;

CREATE FUNCTION SampleSparkUDTF_yarnV1_01
AS 'com.fuzzylogix.experiments.udf.hiveSparkUDF.SampleSparkUDTF_yarnV1'
USING JAR 'hdfs:///user/root/experiments-1.2.jar';

DESCRIBE FUNCTION SampleSparkUDTF_yarnV1_01;

SELECT SampleSparkUDTF_yarnV1_01('San Francisco', 'sampleData');  

------------ UDTF Exmaple: Local Spark App : Reading from CSV file -------------

DROP FUNCTION SampleSparkUDTF_yarnV2_01;

CREATE FUNCTION SampleSparkUDTF_yarnV2_01
AS 'com.fuzzylogix.experiments.udf.hiveSparkUDF.SampleSparkUDTF_yarnV2'
USING JAR 'hdfs:///user/root/experiments-1.2.jar';

DESCRIBE FUNCTION SampleSparkUDTF_yarnV2_01;

SELECT SampleSparkUDTF_yarnV2_01('San Francisco', 'hdfs:///user/root/samplefile.csv');


----------------------------------------------------------
------------ UDTF Exmaple: Local Spark App ----------------
----------------------------------------------------------

------------ UDTF Exmaple: Local Spark App : Reading table -------------

DROP FUNCTION SampleSparkUDTF_localV1_01;

CREATE FUNCTION SampleSparkUDTF_localV1_01
AS 'com.fuzzylogix.experiments.udf.hiveSparkUDF.SampleSparkUDTF_localV1'
USING JAR 'hdfs:///user/root/experiments-1.2.jar';

DESCRIBE FUNCTION SampleSparkUDTF_localV1_01;

SELECT SampleSparkUDTF_localV1_01('San Francisco', 'sampleData');

------------ UDTF Exmaple: Local Spark App : Reading from CSV file -------------

DROP FUNCTION SampleSparkUDTF_localV2_01;

CREATE FUNCTION SampleSparkUDTF_localV2_01
AS 'com.fuzzylogix.experiments.udf.hiveSparkUDF.SampleSparkUDTF_localV2'
USING JAR 'hdfs:///user/root/experiments-1.2.jar';

DESCRIBE FUNCTION SampleSparkUDTF_localV2_01;

SELECT SampleSparkUDTF_localV2_01('San Francisco', 'hdfs:///user/root/samplefile.csv' );


