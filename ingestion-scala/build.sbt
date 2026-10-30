name := "ingestion-scala"

version := "0.1.0"

scalaVersion := "2.13.12"

libraryDependencies ++= Seq(
  // Клиент для работы с кафка
  "org.apache.kafka" % "kafka-clients" % "3.6.1",

  // Библиотеки Circe для работы с json
  "io.circe" %% "circe-core" % "0.14.6",
  "io.circe" %% "circe-generic" % "0.14.6",
  "io.circe" %% "circe-parser" % "0.14.6",

  // Движок логирования
  "ch.qos.logback" % "logback-classic" % "1.4.14"
)