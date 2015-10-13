import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka._
import org.apache.spark.rdd.RDD

import java.util.HashMap
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}

object KafkaReceiver {
    def main(args: Array[String]) {
        val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaReceiver")
        val ssc = new StreamingContext(conf, Seconds(10))

        val kafkaStream = KafkaUtils.createStream(ssc, "localhost:2181","group", Map("streaming-topic" -> 1))
        /*kafkaStream.print()*/

        // Print out the count of events received from this server in each batch
        var totalCount = 0L
        kafkaStream.foreachRDD((rdd: RDD[_], time: Time) => {
            val count = rdd.count()
            println("\n-------------------")
            println("Time: " + time)
            println("-------------------")
            println("Received " + count + " events\n")
            totalCount += count
        })

        ssc.start()
        Thread.sleep(20 * 1000)
        ssc.stop()
        if (totalCount > 0) {
            println("PASSED")
        } else {
            println("FAILED")
        }

        /*ssc.start()*/
        /*ssc.awaitTermination()*/
    }
}

// Produces some random words between 1 and 100.
object KafkaInputProducer {
    def main(args: Array[String]) {
        if (args.length < 4) {
            System.err.println("Usage: KafkaInputProducer <metadataBrokerList> <topic> " +
                "<messagesPerSec> <wordsPerMessage>")
            System.exit(1)
        }

        val Array(brokers, topic, messagesPerSec, wordsPerMessage) = args

        // Zookeeper connection properties
        val props = new HashMap[String, Object]()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer")

        val producer = new KafkaProducer[String, String](props)

        // Send some messages
        while(true) {
            (1 to messagesPerSec.toInt).foreach { messageNum =>
                val str = (1 to wordsPerMessage.toInt).map(x => scala.util.Random.nextInt(10).toString)
                .mkString(" ")
                val message = new ProducerRecord[String, String](topic, null, str)
                producer.send(message)
            }

            Thread.sleep(1000)
        }
    }
}
