package com.fuzzylogix.experiments.udf.sparkUDF.test

import com.fuzzylogix.experiments.udf.sparkUDF.SampleSparkUDAF_v3
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * Created by Lokesh on 07-02-2017.
  */
object SparkUDFTest {

    def main(args: Array[String]): Unit = {

        val spark: SparkSession = SparkSession.builder
            .master("local[1]")
            .appName("spark session example")
            .config("spark.executor.memory", "3G")
            .config("spark.sql.warehouse.dir", "./warehouse")
            .getOrCreate()

        val input_data: Dataset[Row] = spark.read
            .option("inferSchema", "true")
            .option("delimiter", "|")
            .csv("DataFiles/tblLinRegr.dat")

        input_data.registerTempTable("tblLinRegr")
//        input_data.show()

        val udaf01: UserDefinedAggregateFunction = new SampleSparkUDAF_v3
        spark.udf.register( "sumAF", udaf01)

//        input_data.select( "value" )
        spark.sql( "Select sumAF(_c2) from tblLinRegr").show()

        spark.catalog.listFunctions().collect().foreach(println)

        spark.stop()

    }

}


