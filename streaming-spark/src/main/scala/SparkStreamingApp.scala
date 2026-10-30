import org.apache.spark.sql.SparkSession
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

    spark.streams.awaitAnyTermination()
  }
}
