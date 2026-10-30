name := "streaming-spark"

version := "0.1.0"

scalaVersion := "2.13.12"

val sparkVersion = "3.5.1"

libraryDependencies ++= Seq(
  // Ядро апачи спарк
  "org.apache.spark" %% "spark-core" % sparkVersion,

  // Спарк sql и structured streaming
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  // Официальный коннектор спарк и кафки
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
)