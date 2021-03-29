package edu.sogang.benchmark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, col, from_csv, max, struct, to_json, window}


class Query2 extends QueryBase {

  def runQuery(df: DataFrame): DataFrame = {

    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
      .select(col("key"),
        from_csv(col("value"), inputSchema, Map("delimiter" -> ",")).as("task_event"),
        col("timestamp"))
      .where("task_event.eventType == 1")
      .dropDuplicates("key", "timestamp")
      .withWatermark("timestamp", "1 minutes")
      .groupBy(
        window(col("timestamp"), "60 seconds", "1 seconds"),
        col("task_event.jobId")
      ).agg(
        max("key").as("key"),
        avg("task_event.cpu").as("avgCpu")
      )
      .select(
        col("key"),
        to_json(
          struct(
            col("window"),
            col("avgCpu"))
        ).as("value")
      )

  }
}
