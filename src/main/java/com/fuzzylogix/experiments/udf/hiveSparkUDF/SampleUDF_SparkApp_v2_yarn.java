package com.fuzzylogix.experiments.udf.hiveSparkUDF;


import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantStringObjectInspector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

/*
 * This is the class for a sample User Defined Function (UDF)
 *
 * The function takes in input single column with STRING type,
 * which would be the table name.
 * and outputs a single column which has the count of rows stored
 * in the table name given in the input.
 *
 * Example:
 * Input  : "inputTable"
 * Output : 500
 *
 */

public class SampleUDF_SparkApp_v2_yarn extends GenericUDF {
    private PrimitiveObjectInspector tableNameOI = null;

    static String pTableName = null;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // input inspectors
        tableNameOI = (PrimitiveObjectInspector) arguments[0];

        pTableName = ((WritableConstantStringObjectInspector) tableNameOI).getWritableConstantValue()
                .toString();

        // output inspectors
        List<String> fieldNames = new ArrayList<String>();
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();


        fieldNames.add("TableInfo");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Long tableRowCount = sparkJob(pTableName);
        return tableRowCount;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "Abstract UDF";
    }

    static Long sparkJob(String tableName) {
        SparkSession spark = SparkSession
                .builder()
                .master("yarn")
                .appName("SampleUDF_SparkApp_v2_yarn")
                .getOrCreate();

        Dataset inputData = spark.read().table(tableName);

        Long countRows = inputData.count();

        spark.stop();

        return countRows;
    }
}

