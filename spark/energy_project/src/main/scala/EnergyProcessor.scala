package energy_stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.rdd.RDD

object EnergyProcessor {
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
