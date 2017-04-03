package com.fuzzylogix.experiments.udf.hiveSparkUDF;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantStringObjectInspector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

/*
 * This is the class for a sample User Defined Table Function (UDTF)
 * inside which a spark app is launched with master:local
 *
 * The function takes in input two values, and output values
 * First is a random string, the outputs a mutated version of that string
 * as the first column.
 * Second is the name of a CSV file, which is to be read by spark app.
 * The count of number of records in that file is returned as the second column of the output.
 *
 * Requirement: The function requires the table to be present in the database.
 *
 * Example:
 * Input  : "London", "fileName"
 * Output: "***London***", 500
 *
 */

public class SampleSparkUDTF_V2 extends GenericUDTF {

    private PrimitiveObjectInspector nameOI = null;
    private PrimitiveObjectInspector fileNameOI = null;

    static String pName = null;
    static String pFileName = null;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {

        // input inspectors
        nameOI = (PrimitiveObjectInspector) args[0];
        fileNameOI = (PrimitiveObjectInspector) args[1];

        pName = ((WritableConstantStringObjectInspector) nameOI).getWritableConstantValue()
                .toString();
        pFileName = ((WritableConstantStringObjectInspector) fileNameOI).getWritableConstantValue()
                .toString();

        // output inspectors
        List<String> fieldNames = new ArrayList<String>();
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();


        fieldNames.add("Name");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        fieldNames.add("TableInfo");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] record) throws HiveException {

        ArrayList<Object> outList = new ArrayList<Object>();

        Long tableRowCount = sparkJob(pFileName);

        outList.add("***" + pName + "***");
        outList.add(tableRowCount);

        forward(outList);
    }

    @Override
    public void close() throws HiveException {
        // do nothing
    }

    Long sparkJob(String fileName)
    {
        SparkSession spark = SparkSession
                .builder()
                .master("yarn")
                .getOrCreate();

        Dataset inputData = spark.read().csv(fileName);

        Long countRows =  inputData.count();

        return countRows;
    }
}

