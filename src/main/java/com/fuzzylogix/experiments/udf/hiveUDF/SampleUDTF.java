package com.fuzzylogix.experiments.udf.hiveUDF;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantStringObjectInspector;

import java.util.ArrayList;
import java.util.List;

/*
 * This is the class for a sample User Defined Table Function (UDTF)
 *
 * Example:
 * Input:
 *
 * Input  : "Paris"
 *
 * Output :
 * "***Paris***", "###Paris###"
 *
 */

public class SampleUDTF extends GenericUDTF {

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

        fieldNames.add("First");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("Second");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] record) throws HiveException {
        ArrayList<Object> outList = new ArrayList<Object>();

        outList.add("***" + pName + "***");
        outList.add("###" + pName + "###");

        forward(outList);
    }

    @Override
    public void close() throws HiveException {
        // do nothing
    }
}
