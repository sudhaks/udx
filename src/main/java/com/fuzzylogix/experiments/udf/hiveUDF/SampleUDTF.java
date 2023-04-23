package com.fuzzylogix.experiments.udf.hiveUDF;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantStringObjectInspector;

import java.util.ArrayList;
import java.util.List;

/*
 * This is the class for a sample User Defined Table Function (UDTF)
 *
 * The function takes in input three columns : A string and two integers.
 *
 * Input:
 * name : STRING
 * row  : INT
 * col  : INT
 *
 * Output:
 * The output is variable and depends on the input
 * the first column will be a mutated version of the string passed in the input.
 * Apart from that there will be number of columns as specified in the 'col' input field
 * and total rows would be as specified in the 'row' input field
 * The values in these cells would be doubles following a specific pattern.
 *
 *
 * Example:
 * Input:
 *
 * Input  : "Paris", 3, 2
 *
 * Output :
 * "***Paris***", 1007, 1014
 * "***Paris***", 2007, 2014
 * "***Paris***", 3007, 3014
 *
 */

public class SampleUDTF extends GenericUDTF {

    private PrimitiveObjectInspector nameOI = null;
    private PrimitiveObjectInspector rowsOI = null;
    private PrimitiveObjectInspector colsOI = null;

    static String pName = null;
    static int pRows = 0;
    static int pCols = 0;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {

        // input inspectors
        nameOI = (PrimitiveObjectInspector) args[0];
        rowsOI = (PrimitiveObjectInspector) args[1];
        colsOI = (PrimitiveObjectInspector) args[2];

        pName = ((WritableConstantStringObjectInspector) nameOI).getWritableConstantValue()
                .toString();
        pRows = ((WritableConstantIntObjectInspector) rowsOI).getWritableConstantValue().get();
        pCols = ((WritableConstantIntObjectInspector) colsOI).getWritableConstantValue().get();

        // output inspectors
        List<String> fieldNames = new ArrayList<String>();
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldNames.add("Name");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        for (int i = 0; i < pCols; i++) {
            fieldNames.add("Column-" + Integer.toString(i + 1));
            fieldOIs.add(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] record) throws HiveException {
        for (int i = 0; i < pRows; i += 1) {
            //arrayList for each output row
            ArrayList<Object> outList = new ArrayList<Object>();

            //the first column is just modification of the first input parameter
            outList.add("***" + pName + "***_feature 1 ");

            //the next columns would be just some numbers
            for (int j = 0; j < pCols; j += 1)
                outList.add((i + 1) * 1000 + (j + 1) * 7.0);
            forward(outList);
        }
    }

    @Override
    public void close() throws HiveException {
        // do nothing
    }
}
