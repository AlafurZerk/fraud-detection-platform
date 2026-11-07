import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object SparkStreamingApp {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    // Отключение NativeIO на Windows
    try {
      org.apache.hadoop.io.nativeio.NativeIO.isAvailable()
      val field = classOf[org.apache.hadoop.io.nativeio.NativeIO].getDeclaredField("nativeLoaded")
      field.setAccessible(true)
      field.setBoolean(null, false)
      println("Windows NativeIO is stopped")
    } catch {
      case _: Throwable => println(">>> Warning: NativeIO")
    }

    // master("local[*]") означает локальный запуск и использование всех ядер процессора
    val spark = SparkSession.builder()
      .appName("FraudDetectionSparkEngine")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println(">>> Spark Structured Streaming Engine successfully launch! <<<")

    val transactionSchema = new StructType()
      .add("transactionId", StringType, nullable = false)
      .add("userId", StringType, nullable = false)
      .add("amount", DoubleType, nullable = false)
      .add("currency", StringType, nullable = false)
      .add("timestamp", LongType, nullable = false)
      .add("merchantCategory", StringType, nullable = false)
      .add("latitude", DoubleType, nullable = false)
      .add("longitude", DoubleType, nullable = false)

    // Подключение к кафке и создание непрерывного стрима
    val kafkaRawStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:29092")
      .option("subscribe", "transaction.raw")
      .option("startingOffsets", "latest") // чтение только свежих транзакций
      .option("failOnDataLoss", "false")
      .load()

    // Парсинг потока
    // Кафка хранит данные в бинарном виде. Перевод значения в строку, а затем распаковка джсон в колонки таблиц
    val parsedTransactions = kafkaRawStream
      .selectExpr("CAST(key AS STRING) as user_key", "CAST(value AS STRING) as json_payload")
      .select(from_json(col("json_payload"), transactionSchema).as("data"))
      .select("data.*")
      .withColumn("event_time", to_timestamp(col("timestamp") / 1000)) // Преобразование миллисекунд Long в Spark Timestamp

    // Правило антифрода и детекции аномалий
    // Транзакция критическая, если сумма >= 10,000$
    // и покупка совершена в зоне риска
    val fraudAlerts = parsedTransactions
      .filter(
        col("amount") >= 10000.0 && (
          col("merchantCategory").contains("crypto") || col("merchantCategory").contains("gambling") || col("merchantCategory").contains("electronics")
        )
      )
      .withColumn("alert_type", lit("CRITICAL_HIGH_AMOUNT_SUSPICIOUS_MCC"))
      .withColumn("detected_at", current_timestamp())

    val kafkaAlertsPayload = fraudAlerts
      .select(
        col("userId").cast("string").as("key"),
        to_json(
          struct(
            col("alert_type"),
            col("userId"),
            col("amount"),
            col("merchantCategory"),
            col("event_time"),
            col("detected_at")
          )
        ).as("value")
      )

    val kafkaQuery = kafkaAlertsPayload.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:29092")
      .option("topic", "transaction.alerts")
      .option("checkpointLocation", "checkpoints/kafka-alerts")
      .outputMode("append")
      .start()

    val consoleQuery = fraudAlerts
      .select(
        col("alert_type"),
        col("userId"),
        col("amount"),
        col("merchantCategory"),
        col("event_time")
      )
      .writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .start()

    val postgresQuery = fraudAlerts.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF.foreachPartition { partition: Iterator[Row] =>
          if (partition.hasNext) {
            Class.forName("org.postgresql.Driver")
            val conn = java.sql.DriverManager.getConnection(
              "jdbc:postgresql://localhost:5435/fraud_detection",
              "postgres",
              "postgres_password"
            )
            try {
              val sql =
                """
                  INSERT INTO fraud_alerts (
                    transaction_id, user_id, amount, merchant_category, alert_type, event_time, detected_at
                  ) VALUES (?, ?, ?, ?, ?, ?, ?)
                  ON CONFLICT (transaction_id) DO NOTHING
                """.stripMargin
              val stmt = conn.prepareStatement(sql)
              try {
                partition.foreach { row =>
                  stmt.setString(1, row.getAs[String]("transactionId"))
                  stmt.setString(2, row.getAs[String]("userId"))
                  stmt.setBigDecimal(3, java.math.BigDecimal.valueOf(row.getAs[Double]("amount")))
                  stmt.setString(4, row.getAs[String]("merchantCategory"))
                  stmt.setString(5, row.getAs[String]("alert_type"))
                  stmt.setTimestamp(6, row.getAs[java.sql.Timestamp]("event_time"))
                  stmt.setTimestamp(7, row.getAs[java.sql.Timestamp]("detected_at"))
                  stmt.addBatch()
                }
                stmt.executeBatch()
                ()
              } finally {
                stmt.close()
              }
            } finally {
              conn.close()
            }
          }
          ()
        }
      }
      .option("checkpointLocation", "checkpoints/postgres-alerts")
      .outputMode("append")
      .start()

    spark.streams.awaitAnyTermination()
  }
}
