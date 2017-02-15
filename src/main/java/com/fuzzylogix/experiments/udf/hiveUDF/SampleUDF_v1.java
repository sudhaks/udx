package com.fuzzylogix.experiments.udf.hiveUDF;


import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantStringObjectInspector;

import java.util.ArrayList;
import java.util.List;

/*
 * This is the class for a sample User Defined Function (UDF)
 *
 * The function takes in input single column with STRING type.
 * and outputs a single column with the mutated version of that input string
 *
 * Example:
 * Input  : "Rome"
 * Output : "===Rome==="
 *
 *
 */

public class SampleUDF_v1 extends GenericUDF
{
    private PrimitiveObjectInspector nameOI = null;

    static String pName = null;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException
    {
        // input inspectors
        nameOI = (PrimitiveObjectInspector) arguments[0];

        pName = ((WritableConstantStringObjectInspector) nameOI).getWritableConstantValue()
                .toString();

        // output inspectors
        List<String> fieldNames = new ArrayList<String>();
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();


        fieldNames.add("Name");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException
    {
        return "===" + pName + "===";
    }

    @Override
    public String getDisplayString(String[] children)
    {
        return "Abstract UDF";
    }
}

