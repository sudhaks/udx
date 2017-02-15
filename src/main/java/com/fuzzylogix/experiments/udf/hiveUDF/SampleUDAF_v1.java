package com.fuzzylogix.experiments.udf.hiveUDF;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public class SampleUDAF_v1 implements GenericUDAFResolver2 {

    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        return new AggEvaluator();
    }


    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        return new AggEvaluator();
    }

    public class AggEvaluator extends GenericUDAFEvaluator {

        PrimitiveObjectInspector inputOI;
        ObjectInspector outputOI;
        PrimitiveObjectInspector integerOI;

        double total = 0.0;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {

            assert (parameters.length == 1);
            super.init(m, parameters);

            // init input object inspectors
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
            } else {
                integerOI = (PrimitiveObjectInspector) parameters[0];
            }

            // init output object inspectors
            outputOI = ObjectInspectorFactory.getReflectionObjectInspector(Double.class,
                    ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
            return outputOI;

        }

        /**
         * class for storing the current sum of letters
         */
        class NumberSumAgg implements AggregationBuffer {
            double sum = 0;

            void add(double num) {
                sum += num;
            }
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            NumberSumAgg result = new NumberSumAgg();
            return result;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            NumberSumAgg myagg = new NumberSumAgg();
        }

        private boolean warned = false;

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {
            assert (parameters.length == 1);
            if (parameters[0] != null) {
                NumberSumAgg myagg = (NumberSumAgg) agg;
                Object p1 = ((PrimitiveObjectInspector) inputOI).getPrimitiveJavaObject(parameters[0]);
                myagg.add(String.valueOf(p1).length());
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            NumberSumAgg myagg = (NumberSumAgg) agg;
            total += myagg.sum;
            return total;
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            if (partial != null) {

                NumberSumAgg myagg1 = (NumberSumAgg) agg;

                Double partialSum = (Double) integerOI.getPrimitiveJavaObject(partial);

                NumberSumAgg myagg2 = new NumberSumAgg();

                myagg2.add(partialSum);
                myagg1.add(myagg2.sum);
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            NumberSumAgg myagg = (NumberSumAgg) agg;
            total = myagg.sum;
            return myagg.sum;
        }

    }


}
