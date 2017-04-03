
===========================================================
--        HiveServer : Hive UDFs with Spark App
===========================================================

------------ UDF  Exmaple -------------

DROP FUNCTION SampleUDF_SparkApp_v1_local_03;

CREATE FUNCTION SampleUDF_SparkApp_v1_local_03 AS 'com.fuzzylogix.experiments.udf.hiveSparkUDF.SampleUDF_SparkApp_v1_local' USING JAR '/root/Data/experiments-1.2.jar';

DESCRIBE FUNCTION SampleUDF_SparkApp_v1_local_03;

SELECT SampleUDF_SparkApp_v1_local_03('sampleData');


------------ UDTF Exmaple: Local Spark App : Reading table -------------

DROP FUNCTION SampleSparkUDTF_localV1_01;

CREATE FUNCTION SampleSparkUDTF_localV1_01
AS 'com.fuzzylogix.experiments.udf.hiveSparkUDF.SampleSparkUDTF_localV1'
USING JAR '/root/Data/experiments-1.2.jar';

DESCRIBE FUNCTION SampleSparkUDTF_localV1_01;

SELECT SampleSparkUDTF_localV1_01('San Francisco', 'sampleData');

------------ UDTF Exmaple: Local Spark App : Reading from CSV file -------------

DROP FUNCTION SampleSparkUDTF_localV2_01;

CREATE FUNCTION SampleSparkUDTF_localV2_01
AS 'com.fuzzylogix.experiments.udf.hiveSparkUDF.SampleSparkUDTF_localV2'
USING JAR '/root/Data/experiments-1.2.jar';

DESCRIBE FUNCTION SampleSparkUDTF_localV2_01;

SELECT SampleSparkUDTF_localV2_01('San Francisco', 'sampleData');

------------ UDTF Exmaple: Local Spark App : Doing nothing -------------

DROP FUNCTION SampleSparkUDTF_localV3_02;

CREATE FUNCTION SampleSparkUDTF_localV3_02
AS 'com.fuzzylogix.experiments.udf.hiveSparkUDF.SampleSparkUDTF_localV3'
USING JAR '/root/Data/experiments-1.2.jar';

DESCRIBE FUNCTION SampleSparkUDTF_localV3_02;

SELECT SampleSparkUDTF_localV3_02('San Francisco', 'sampleData');


----------------------------------------------------------
----------------------------------------------------------
------------ UDTF Exmaple: Yarn Spark App ----------------
----------------------------------------------------------

------------ UDTF Exmaple: Yarn Spark App : Reading table -------------

DROP FUNCTION SampleSparkUDTF_yarnV1_01;

CREATE FUNCTION SampleSparkUDTF_yarnV1_01
AS 'com.fuzzylogix.experiments.udf.hiveSparkUDF.SampleSparkUDTF_yarnV1'
USING JAR '/root/Data/experiments-1.2.jar';

DESCRIBE FUNCTION SampleSparkUDTF_yarnV1_01;

SELECT SampleSparkUDTF_yarnV1_01('San Francisco', 'sampleData');

------------ UDTF Exmaple: Local Spark App : Reading from CSV file -------------

DROP FUNCTION SampleSparkUDTF_yarnV2_01;

CREATE FUNCTION SampleSparkUDTF_yarnV2_01
AS 'com.fuzzylogix.experiments.udf.hiveSparkUDF.SampleSparkUDTF_yarnV2'
USING JAR '/root/Data/experiments-1.2.jar';

DESCRIBE FUNCTION SampleSparkUDTF_yarnV2_01;

SELECT SampleSparkUDTF_yarnV2_01('San Francisco', 'sampleData');

------------ UDTF Exmaple: yarn Spark App : Doing nothing -------------

DROP FUNCTION SampleSparkUDTF_yarnV3_02;

CREATE FUNCTION SampleSparkUDTF_yarnV3_02
AS 'com.fuzzylogix.experiments.udf.hiveSparkUDF.SampleSparkUDTF_yarnV3'
USING JAR '/root/Data/experiments-1.2.jar';

DESCRIBE FUNCTION SampleSparkUDTF_yarnV3_02;

SELECT SampleSparkUDTF_yarnV3_02('San Francisco', 'sampleData');


------------------------------------------------------------------------------
------------------------------------------------------------------------------
--------- UDTF Exmaple: Spark App using thrift server context ----------------
------------------------------------------------------------------------------

------------ Spark App: reading from table -------------

DROP FUNCTION SampleSparkUDTF_V1_01;

CREATE FUNCTION SampleSparkUDTF_V1_01
AS 'com.fuzzylogix.experiments.udf.hiveSparkUDF.SampleSparkUDTF_V1'
USING JAR '/root/Data/experiments-1.2.jar';

DESCRIBE FUNCTION SampleSparkUDTF_V1_01;

SELECT SampleSparkUDTF_V1_01('San Francisco', 'sampleData');

------------ Spark App: reading from file -------------

DROP FUNCTION SampleSparkUDTF_V2_01;

CREATE FUNCTION SampleSparkUDTF_V2_01
AS 'com.fuzzylogix.experiments.udf.hiveSparkUDF.SampleSparkUDTF_V2'
USING JAR '/root/Data/experiments-1.2.jar';

DESCRIBE FUNCTION SampleSparkUDTF_V2_01;

SELECT SampleSparkUDTF_V2_01('San Francisco', 'sampleData');


------------ Spark App: spark just being initialized, no process being done.  -------------

DROP FUNCTION SampleSparkUDTF_V3_01;

CREATE FUNCTION SampleSparkUDTF_V3_01
AS 'com.fuzzylogix.experiments.udf.hiveSparkUDF.SampleSparkUDTF_V3'
USING JAR '/root/Data/experiments-1.2.jar';

DESCRIBE FUNCTION SampleSparkUDTF_V3_01;

SELECT SampleSparkUDTF_V3_01('San Francisco', 'sampleData');


