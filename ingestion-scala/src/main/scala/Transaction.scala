import io.circe.generic.auto._
import io.circe.syntax._

// Модель финансовой транзакции
case class Transaction(
  transactionId: String,
  userId: String,
  amount: Double,
  currency: String,
  timestamp: Long,
  merchantCategory: String,
  latitude: Double,
  longitude: Double
)

object Transaction {
  // Сериализация транзакции в json-строку
  def toJson(tx: Transaction): String = {
    tx.asJson.noSpaces
  }
}