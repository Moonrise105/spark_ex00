
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{dayofyear, max, sum}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object main {
  def main(args : Array[String]) : Unit = {
    val spark : SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("Test")
      .getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "5")
    spark.sparkContext.setLogLevel("ERROR")
    val mySchema =  new StructType(Array(
      new StructField("vendor_id", ByteType, true),
      new StructField("tpep_pickup_datetime", TimestampType, true),
      new StructField("tpep_dropoff_datetime", TimestampType, true),
      new StructField("passenger_count", ByteType, true),
      new StructField("trip_distance", FloatType, true),
      new StructField("rate_code_id", ByteType, true),
      new StructField("store_and_fwd_flag", StringType, true),
      new StructField("pu_Location_id", IntegerType, true),
      new StructField("do_location_id", IntegerType, true),
      new StructField("payment_type", ByteType, true),
      new StructField("fare_amount", FloatType, true),
      new StructField("extra", FloatType, true),
      new StructField("mta_tax", FloatType, true),
      new StructField("tip_amount", FloatType, true),
      new StructField("tolls_amount", FloatType, true),
      new StructField("improvement_surcharge", FloatType, true),
      new StructField("total_amount", FloatType, true),
      new StructField("congestion_surcharge", FloatType, true)
    ))

    val yellowTaxiTripData_2020_04 = spark.read.format("csv")
      .option("header", "true")
      .schema(mySchema)
      .load("src/data/yellow_tripdata_2020-04.csv")
    yellowTaxiTripData_2020_04.createOrReplaceTempView("taxi")

    val sum_by_day_pas = spark.sql(
      """
        |SELECT passenger_count,
        |sum_by_day_pas.day, sum_day_pas / sum_day as percent
        |FROM (
        |SELECT passenger_count,
        |dayofyear(tpep_dropoff_datetime) as day,
        |SUM(trip_distance) as sum_day_pas
        |FROM taxi
        |GROUP BY day, passenger_count
        |) as sum_by_day_pas
        |LEFT JOIN
        |(
        |SELECT dayofyear(tpep_dropoff_datetime) as day,
        |SUM(trip_distance) as sum_day
        |FROM taxi
        |GROUP BY day
        |) as sum_by_day
        |ON sum_by_day_pas.day = sum_by_day.day
        |ORDER BY day, passenger_count
        |""".stripMargin).toDF()
    sum_by_day_pas.createOrReplaceTempView("sum_by_day_pas")
    sum_by_day_pas.show()
    val sum_by_day_pas_new = sum_by_day_pas.na.fill(0)
    val df_transpose = sum_by_day_pas_new.groupBy("day").pivot("passenger_count").sum("percent").na.fill(0.0)

    val df_renamed = df_transpose
      .withColumnRenamed("0", "percentage_zero")
      .withColumnRenamed("1", "percentage_1p")
      .withColumnRenamed("2", "percentage_2p")
      .withColumnRenamed("3", "percentage_3p")
      .withColumnRenamed("4", "percentage_4p")
      .withColumnRenamed("5", "percentage_5p")
      .withColumnRenamed("6", "percentage_6p")
      .withColumnRenamed("7", "percentage_7p")
    df_renamed.createOrReplaceTempView("renamed")
    val out = spark.sql(
      """
        |SELECT day as date,
        |percentage_zero, percentage_1p, percentage_2p, percentage_3p,
        |percentage_4p + percentage_5p + percentage_6p + percentage_7p as percentage_4p_plus
        |FROM renamed
        |ORDER BY date
        |""".stripMargin).toDF()
    out.show(200)

   out.repartition(2).write.format("parquet")
      .mode("overwrite")
      .option("path", "src/data/out")
      .save()

    Thread.sleep(Int.MaxValue)
  }
}
