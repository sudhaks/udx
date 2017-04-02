
# ===========================================
# ThriftServer : Running on YARN as a service in HDP
# ===========================================


# ---------- UDF  Exmaple -------------

DROP FUNCTION SampleUDF_01;

CREATE FUNCTION SampleUDF_01 AS 'com.fuzzylogix.experiments.udf.hiveUDF.SampleUDF' USING JAR '/root/Data/experiments-1.2.jar';

DESCRIBE FUNCTION SampleUDF_01;

SELECT SampleUDF_01('Rome');

# ---------- UDTF Exmaple -------------

DROP FUNCTION SampleUDTF_01;

CREATE FUNCTION SampleUDTF_01 AS 'com.fuzzylogix.experiments.udf.hiveUDF.SampleUDTF' USING JAR '/root/Data/experiments-1.2.jar';

DESCRIBE FUNCTION SampleUDTF_01;

SELECT SampleUDTF_01('Paris' );

# ---------------------------
# Observations:
# Basic UDTF: Thriftserver running on YARN in HDP 2.5
# successfully runs. Persists, can call from subsequent sessions and and local sessions as well.
#

