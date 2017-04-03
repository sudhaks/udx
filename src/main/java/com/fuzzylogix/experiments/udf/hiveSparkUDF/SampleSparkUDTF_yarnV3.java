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
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

/*
 * This is the class for a sample User Defined Table Function (UDTF)
 * inside which a spark app is launched with master:local
 *
 * The function takes in a single input.
 * It is a random string, and the output is a mutated version of that string
 * as the first column.
 *
 * Example:
 * Input  : "London"
 * Output: "***London***"
 *
 */

public class SampleSparkUDTF_yarnV3 extends GenericUDTF {

    private PrimitiveObjectInspector nameOI = null;

    static String pName = null;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {

        // input inspectors
        nameOI = (PrimitiveObjectInspector) args[0];

        pName = ((WritableConstantStringObjectInspector) nameOI).getWritableConstantValue()
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

        Long tableRowCount = sparkJob();

        outList.add("***" + pName + "***");
        outList.add(tableRowCount);

        forward(outList);
    }

    @Override
    public void close() throws HiveException {
        // do nothing
    }

    Long sparkJob( )
    {
        SparkSession spark = SparkSession
                .builder()
                .master("yarn-cluster")
                .enableHiveSupport()
                .appName("SampleSparkUDTF_yarnV3")
                .getOrCreate();


        Long countRows = 555L;

        return countRows;
    }
}

