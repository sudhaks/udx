package com.fuzzylogix.experiments.udf.sparkUDF

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

// This Function computes the sum of the input column

class SampleSparkUDAF_v3 extends UserDefinedAggregateFunction {


    // This is the input fields for your aggregate function.
    override def inputSchema: StructType = StructType(
        StructField("inputCol", DoubleType) :: Nil)

    // This is the internal fields you keep for computing your aggregate.
    override def bufferSchema: StructType = StructType(
        StructField("sum", DoubleType) ::
            Nil
    )

    // This is the output type of your aggregatation function.
    override def dataType: DataType = DoubleType

    override def deterministic: Boolean = true

    // This is the initial value for your buffer schema.
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = 0.0
    }

    // This is how to update your buffer schema given an input.
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        buffer(0) = buffer.getAs[Double](0) + input.getAs[Double](0)
    }

    // This is how to merge two objects with the bufferSchema type.
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        buffer1(0) = buffer1.getAs[Double](0) + buffer2.getAs[Double](0)
    }

    // This is where you output the final value, given the final value of your bufferSchema.
    override def evaluate(buffer: Row): Any = {
        buffer.getDouble(0)
    }

}