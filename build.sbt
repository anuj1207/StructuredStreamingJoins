name := "DataEnggProj2"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "1.1.0"
libraryDependencies += "com.typesafe" % "config" % "1.3.2"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.4"
)

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.48"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.1"
