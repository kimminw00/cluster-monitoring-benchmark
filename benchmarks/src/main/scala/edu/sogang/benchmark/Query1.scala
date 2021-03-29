package edu.sogang.benchmark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, from_csv, max, struct, sum, to_json, window}


class Query1 extends QueryBase {

  def runQuery(df: DataFrame): DataFrame = {

    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
      .select(col("key"),
        from_csv(col("value"), inputSchema, Map("delimiter" -> ",")).as("task_event"),
        col("timestamp"))
      .dropDuplicates("key", "timestamp")
      .withWatermark("timestamp", "1 minutes")
      .groupBy(
        window(col("timestamp"), "60 seconds", "1 seconds"),
        col("task_event.category").as("category")
      ).agg(
        max("key").as("key"),
        sum("task_event.cpu").as("totalCpu")
      )
      .select(
        col("key"),
        to_json(
          struct(
            col("window"),
            col("category"),
            col("totalCpu"))
        ).as("value")
      )

  }
}
