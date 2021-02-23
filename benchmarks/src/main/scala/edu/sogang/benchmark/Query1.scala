package edu.sogang.benchmark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, from_csv, sum, window}


class Query1 extends QueryBase {

  def runQuery(df: DataFrame): DataFrame = {

    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
      .select(col("key"),
        from_csv(col("value"), inputSchema, Map("delimiter" -> ",")).as("task_event"),
        col("timestamp"))
      .dropDuplicates("key", "timestamp")
      .withWatermark("timestamp", "5 minutes")
      .groupBy(
        window(col("timestamp"), "60 seconds", "3 seconds"),
        col("task_event.category")
      ).agg(sum("task_event.cpu").as("totalCpu"))

  }
}
