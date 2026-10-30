import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

import java.util.Properties

object ProducerApp {

  private val logger = LoggerFactory.getLogger(getClass)

  private val TopicName = "transaction.raw"

  def main(args: Array[String]): Unit = {
    logger.info("Launch Transaction Producer...")

    // Конфигурация продюсера
    val props = new Properties()

    // Подключаемся с хоста Windows к Kafka в Docker через внешний порт 29092
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092")

    // Сериализаторы (ключ и значение передаем как обычные строки)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    // Параметры надёжности (acks=all значит, что брокер записал сообщение во все синхронные реплики)
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.RETRIES_CONFIG, "3")

    // Пакетирование сообщений
    props.put(ProducerConfig.LINGER_MS_CONFIG, "10")

    // Экземпляр продюсера
    val producer = new KafkaProducer[String, String](props)

    var isRunning = true

    // Перехват нажатия cntrl c или кнопки стоп в IDEA
    sys.addShutdownHook {
      logger.info("Termination signal received. Closing Kafka Producer...")
      isRunning = false
      producer.flush()
      producer.close()
      logger.info("Kafka Producer successfully stopped.")
    }

    logger.info(s"Starting generation and sending of transactions to topic '$TopicName'...")

    // Бесконечный цикл генерации транзакций
    var counter = 0
    while (isRunning) {
      val tx = TransactionGenerator.nextTransaction()
      val jsonString = Transaction.toJson(tx)

      // Передаем userId в качестве ключа сообщения, поэтому все транзакции пользователя
      // гарантировано попадут в одну и ту же партицию Кафки
      val record = new ProducerRecord[String, String](TopicName, tx.userId, jsonString)

      // Асинхронная отправка сообщения с Callback-функцией
      producer.send(record, new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if (exception != null) {
            logger.error(s"Error sending transaction tx.transactionId: {exception.getMessage}")
          } else {
            // Раз в 10 соо вывод в лог
            if (counter % 10 == 0) {
              logger.info(s"Sending: user=${tx.userId}, amount=$$${tx.amount}, " +
                s"category=${tx.merchantCategory} -> [partition ${metadata.partition()}, offset ${metadata.offset()}")
            }
          }
        }
      })

      counter += 1

      try {
        Thread.sleep(200)
      } catch {
        case _: InterruptedException => isRunning = false
      }
    }
  }


}
