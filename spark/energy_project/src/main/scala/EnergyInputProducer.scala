package energy_stream

import java.util.HashMap
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}
import scala.io.Source

// Produces some random words between 1 and 100.
object EnergyInputProducer {

	def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println("Usage: KafkaInputProducer <metadataBrokerList> <topic> " +
          "<messagesPerSec> <wordsPerMessage> <inputCSV>")
      System.exit(1)
    }

    val Array(brokers, topic, messagesPerSec, wordsPerMessage, inputCSV) = args

    // Zookeeper connection properties
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

		// Open CSV for reading
    val bufferedSource = Source.fromFile(inputCSV)

    println("id,timestamp,property,plug_id,household_id,house_id")
    // Send measures to Kafka
    for (line <- bufferedSource.getLines) {
//      (1 to messagesPerSec.toInt).foreach { messageNum =>
//        val str = (1 to wordsPerMessage.toInt).map(x => scala.util.Random.nextInt(10).toString)
//        .mkString(" ")
//        val message = new ProducerRecord[String, String](topic, null, str)
//        producer.send(message)
//      }

      // Convert string to measurement
      val item = line.split(",").map(_.trim)
//        val measure = Measurement(item(0).toInt,
//        item(1).toInt,
//        item(2).toFloat,
//        item(3).toInt,
//        item(4).toInt,
//        item(5).toInt,
//        item(6).toInt
//      )
//      println(measure)

      // Send serialized measure through Kafka
      val message = new ProducerRecord[String, String](topic, null, line)
      println(line)
      producer.send(message)

      //Thread.sleep(1000)
      Thread.sleep(200)
    }

    bufferedSource.close
  }
}
