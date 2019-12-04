package structuredStreaming

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

case class Customers(CustomerID: Int, fName: String, lName: String, Email: String, Password: String, Street: String,
                     City: String, State: String, Zipcode: String)

object SparkConsumer {

  private val CUSTOMER_DATA_FILE_PATH = "/user/sarfu/dataengg/project/retail_db/customers"

  def saveToCassandra(batchDf: DataFrame, uri: Map[String, String]): Unit = {

    val batchId = batchDf.select("BatchID").rdd.collect()

    println(s"Saving to Cassandra with BatchId: ${batchId(0)}")

    batchDf.write.
      format("org.apache.spark.sql.cassandra").
      mode("append").
      options(uri).
      save()
  }

  def main(args: Array[String]): Unit = {
    println("Structured Streaming Application Started....")

    val conf = ConfigFactory.load.getConfig(args(0))

    val topicOrderItems = "order_items"
    val topicOrders = "orders"

    val spark = SparkSession.
      builder.
      master(conf.getString("execution.mode")).
      appName("Stream Processing App").
      getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("INFO")
    spark.conf.set("spark.sql.shuffle.partitions", "2")

    val cassandra_uri: Map[String, String] = Map("spark.cassandra.connection.host" -> conf.getString("cassandra.hostname"),
                                                 "spark.cassandra.connection.port" -> conf.getString("cassandra.port_no"),
                                                 "keyspace" -> conf.getString("cassandra.keyspace_name"),
                                                 "table" -> conf.getString("cassandra.table_name "))

    val HDFS_URI = conf.getString("hdfs.uri")

    //Reading OrderItems from Kafka
    val orderItemsDF = spark.
      readStream.
      format("kafka").
      option("kafka.bootstrap.servers", conf.getString("bootstrap.server")).
      option("subscribe", topicOrderItems).
      option("startingOffsets", "earliest").
      load.
      withColumn("value", $"value".cast("string")).
      withColumn("OrderItemsTimestamp", $"timestamp".cast("timestamp"))

    println("Printing the schema of Order Items DF: ")
    orderItemsDF.printSchema()

    //Reading Orders from Kafka
    val ordersDF = spark.
        readStream.
        format("kafka").
        option("kafka.bootstrap.servers", conf.getString("bootstrap.server")).
        option("subscribe", topicOrders).
        option("startingOffsets", "earliest").
        load.
        withColumn("value", $"value".cast("string")).
        withColumn("OrdersTimestamp", $"timestamp".cast("timestamp"))

    println("Printing the schema of Orders DF: ")
    ordersDF.printSchema()


    //Separating value column of orderItemsDF and giving it a structure
    val orderItemsDF1 = orderItemsDF.withColumn("temp", split(col("value"), "\\,")).select(
                $"temp".getItem(0).cast("int").as("ItemID"),
                $"temp".getItem(1).cast("int").as("OrderID"),
                $"temp".getItem(2).cast("int").as("ProductID"),
                $"temp".getItem(3).cast("int").as("Quantity"),
                $"temp".getItem(4).cast("double").as("Subtotal"),
                $"temp".getItem(5).cast("double").as("ProductPrice"),
                col("OrderItemsTimestamp")).drop($"temp")

    println("Printing the schema of Order Items DF1 after Structuring: ")
    orderItemsDF1.printSchema()

    val ordersDF1 = ordersDF.withColumn("temp", split(col("value"), "\\,")).select(
      $"temp".getItem(0).cast("int").as("OrderID"),
      $"temp".getItem(1).cast("string").as("OrderDate"),
      $"temp".getItem(2).cast("int").as("CustomerID"),
      $"temp".getItem(3).cast("string").as("Status"),
      col("OrdersTimestamp")).drop($"temp")

    println("Printing the schema of Orders DF1 after Structuring: ")
    ordersDF1.printSchema()

//    val res = orderItemsDF1.writeStream.outputMode("append").format("console").trigger(Trigger.ProcessingTime("20 seconds")).option("truncate", "false").start()
//    res.awaitTermination()

    val orderItemsDFwithWatermark = orderItemsDF1.withWatermark("OrderItemsTimestamp", "2 minutes")
    val ordersDFwithWatermark = ordersDF1.withWatermark("OrdersTimestamp", "2 minutes")

    //Joining the result || Stream-Stream Join
    val ordersAndOrderItemsJoinedDF = orderItemsDFwithWatermark.join(ordersDFwithWatermark, Seq("OrderID"), joinType = "inner")
    println("Printing the schema of Joined DF: ")
    ordersAndOrderItemsJoinedDF.printSchema()

//    val res = ordersAndOrderItemsJoinedDF.writeStream.outputMode("append").format("console").trigger(Trigger.ProcessingTime("20 seconds")).option("truncate", "false").start()
//    res.awaitTermination()

    /**
     * END of STREAM-STREAM JOIN
     */

    //ToDo: Find count of ordersAndOrderItemsJoinedDF

    //Saving the result of Joined DF to Cassandra

//    val result = ordersAndOrderItemsJoinedDF.writeStream.
//      trigger(Trigger.ProcessingTime("20 seconds")).
//      outputMode("append").
//      foreachBatch{ (df: DataFrame, batchId: Long) =>
//        val batchDf = df.withColumn("BatchID", lit(batchId))
//        saveToCassandra(batchDf, cassandra_uri)
//      }.start()

    //result.awaitTermination()


    //Reading customer data from HDFS and adding schema to it as well.
    val customersDF = spark.sparkContext.
      textFile(HDFS_URI + CUSTOMER_DATA_FILE_PATH).
      map(_.split(",")).
      map(attributes => Customers(attributes(0).trim.toInt, attributes(1).trim, attributes(2).trim, attributes(3).trim, attributes(4).trim,
                                  attributes(5).trim, attributes(6).trim, attributes(7).trim, attributes(8).trim)).
      toDF()

    println("Printing the schema of Customers DF:")
    customersDF.printSchema()

    //Joining streaming `ordersAndOrderItemsJoinedDF` and static `customersDF`
    val orderDetailsAndCustomersJoined = ordersAndOrderItemsJoinedDF.join(customersDF, Seq("CustomerID"), joinType = "inner")

//    val res = orderDetailsAndCustomersJoined.writeStream.outputMode("append").format("console").trigger(Trigger.ProcessingTime("20 seconds")).option("truncate", "false").start()
//    res.awaitTermination()


    val aggResult = orderDetailsAndCustomersJoined.withWatermark("OrdersTimestamp", "10 seconds").
      groupBy(window($"OrdersTimestamp", "5 seconds"), $"CustomerID", $"OrderID").
      agg(sum("Subtotal")).
      select(col("CustomerID"), col("OrderID"), col("sum(Subtotal)").alias("Total Amount"))
    //aggResult.awaitTermination()

    val res = aggResult.writeStream.outputMode("append").format("console").trigger(Trigger.ProcessingTime("5 seconds")).option("truncate", "false").start()
    res.explain()
    res.awaitTermination()
//trigger(Trigger.ProcessingTime("20 seconds")).

//   val res = orderDetailsAndCustomersJoined.withWatermark("OrdersTimestamp", "3 minutes").
//      groupBy($"CustomerID", $"OrderID", window($"OrdersTimestamp", "1 minute")).
//      count().writeStream.format("console").trigger(Trigger.ProcessingTime("20 seconds")).start()
//
//    res.awaitTermination()

  }
}