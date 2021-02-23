package edu.sogang.benchmark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{FloatType, IntegerType, LongType, StructField, StructType}


trait QueryBase {

  def inputSchema: StructType = StructType(
    Seq(
      StructField("time", LongType, nullable = false),
      StructField("missingInfo", LongType, nullable = true),
      StructField("jobId", LongType, nullable = false),
      StructField("taskId", LongType, nullable = false),
      StructField("machineId", LongType, nullable = true),
      StructField("eventType", IntegerType, nullable = false),
      StructField("userId", IntegerType, nullable = true),
      StructField("category", IntegerType, nullable = true),
      StructField("priority", IntegerType, nullable = false),
      StructField("cpu", FloatType, nullable = true),
      StructField("ram", FloatType, nullable = true),
      StructField("disk", FloatType, nullable = true),
      StructField("constraints", IntegerType, nullable = true),
    )
  )

  def runQuery(df: DataFrame): DataFrame
}