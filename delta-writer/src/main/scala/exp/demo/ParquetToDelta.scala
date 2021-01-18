package exp.demo

import exp.api.SystemEventRecord
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Encoders, SparkSession}

import scala.concurrent.duration.DurationInt

object ParquetToDelta {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
      .master("local[1]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    val dataFrame = spark.readStream
      .format("parquet")
      .schema(Encoders.product[SystemEventRecord].schema)
      .load("target/data/parquet")

    val query = dataFrame.writeStream
      .format("delta")
      .partitionBy("exposureId", "obsEventName")
      .option("checkpointLocation", "target/data/cp/backup")
//      .trigger(Trigger.ProcessingTime(1.seconds))
      .start("target/data/delta")

    query.awaitTermination()
  }
}
