import java.util.UUID
import scala.util.Random

object TransactionGenerator {

  private val random = new Random()

  // Пул из постоянных 50 юзеров системы
  private val users: Vector[String] = (1 to 50).map(i => s"user_$i").toVector

  private val regularCategories = Vector(
    "5411_groceries_supermarkets",
    "5812_restaurants",
    "5814_fast_food",
    "5541_service_stations_fuel",
    "4121_taxicabs_rideshare",
    "5651_clothing_stores",
    "5912_pharmacies",
    "5999_misc_retail",
    "4814_telecom_services"
  )

  private val highRiskCategories = Vector(
    "6051_crypto_quasi_cash",
    "7995_betting_casino_gambling",
    "5732_electronics_stores",
    "5944_jewelry_watch_luxury",
    "6540_p2p_money_transfer"
  )

  private val homeLocations = Vector(
    (55.7558, 37.6173),   // Москва
    (59.9343, 30.3351),   // Санкт-Петербург
    (56.8389, 60.6057),   // Екатеринбург
    (55.7961, 49.1064),   // Казань
    (55.0084, 82.9357)    // Новосибирск
  )

  private val foreignLocations = Vector(
    (40.7128, -74.0060),  // Нью-Йорк (США)
    (51.5074, -0.1278),   // Лондон (Великобритания)
    (35.6762, 139.6503),  // Токио (Япония)
    (25.2048, 55.2708),   // Дубай (ОАЭ)
    (1.3521, 103.8198),   // Сингапур
    (52.5200, 13.4050),   // Берлин (Германия)
    (22.3193, 114.1694)   // Гонконг
  )

  // Точка входа: 95 процентов легит, 5 процентов фрод
  def nextTransaction(): Transaction = {
    val userId = users(random.nextInt(users.length))
    val isAnomaly = random.nextDouble() < 0.05 // true с вероятностью 5 процентов

    if (isAnomaly) {
      generateFraudTransaction(userId)
    } else {
      generateNormalTransaction(userId)
    }
  }

  /**
   * Обычная транзакция: чек от 5 до 300 usd, РФ страна транзакции и повседневная категория
   */

  private def generateNormalTransaction(userId: String): Transaction = {
    val (lat, lon) = homeLocations(random.nextInt(homeLocations.length))
    val amount = BigDecimal(5.0 + random.nextDouble() * 295.0).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

    Transaction(
      transactionId = UUID.randomUUID().toString,
      userId = userId,
      amount = amount,
      currency = "USD",
      timestamp = System.currentTimeMillis(),
      merchantCategory = regularCategories(random.nextInt(regularCategories.length)),
      latitude = lat,
      longitude = lon
    )
  }

  /**
   * Мошенническая транзакция (случайно выбирается один из двух сценариев)
   */

  private def generateFraudTransaction(userId: String): Transaction = {
    val fraudType = random.nextInt(2)

    fraudType match {
      case 0 =>
        // Сценарий 1 - крупная сумма (взлом аккаунта и попытка вывести деньги)
        val hugeAmount = BigDecimal(10000.0 + random.nextDouble() * 40000.0).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

        val (lat, lon) = homeLocations(random.nextInt(homeLocations.length))

        Transaction(
          transactionId = UUID.randomUUID().toString,
          userId = userId,
          amount = hugeAmount,
          currency = "USD",
          timestamp = System.currentTimeMillis(),
          merchantCategory = highRiskCategories(random.nextInt(highRiskCategories .length)),
          latitude = lat,
          longitude = lon
        )

      case _ =>
        // Сценарий 2 - географическая аномалия (карта использована за границей в рискованной зоне)
        val (lat, lon) = foreignLocations(random.nextInt(foreignLocations.length))
        val amount = BigDecimal(500.0 + random.nextDouble() * 2500.0).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

        Transaction(
          transactionId = UUID.randomUUID().toString,
          userId = userId,
          amount = amount,
          currency = "USD",
          timestamp = System.currentTimeMillis(),
          merchantCategory = highRiskCategories(random.nextInt(highRiskCategories.length)),
          latitude = lat,
          longitude = lon
        )
    }
  }

}