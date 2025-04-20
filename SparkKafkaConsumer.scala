import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.TableName

object SparkKafkaConsumer {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KafkaSparkHBase").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("sensor_data")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    stream.foreachRDD { rdd =>
      rdd.foreachPartition { partition =>
        val config = HBaseConfiguration.create()
        val connection = ConnectionFactory.createConnection(config)
        val table = connection.getTable(TableName.valueOf("sensordata"))

        partition.foreach { record =>
          val data = ujson.read(record.value())
          val put = new Put(Bytes.toBytes(data("timestamp").str))
          put.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("device_id"), Bytes.toBytes(data("device_id").num.toInt))
          put.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("temperature"), Bytes.toBytes(data("temperature").num.toFloat))
          put.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("humidity"), Bytes.toBytes(data("humidity").num.toFloat))
          table.put(put)
        }
        table.close()
        connection.close()
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
