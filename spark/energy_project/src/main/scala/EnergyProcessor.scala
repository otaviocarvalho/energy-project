package energy_stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.rdd.RDD

import java.lang.Math.copySign
import com.redis._

import scala.reflect.io.File

object EnergyProcessor {

		// Get a rolling average over a DStream
    def avgFunc(measurements: Seq[Double], state: Option[(Double,Int)]): Option[(Double,Int)] = {
      // Sum received measurements with previous ones and get average
      val current = state.getOrElse((0.0,0)) // State = (Aggregate of measurement values, Aggregate counter of values)
      val counter = current._2 + measurements.length
      val avg = (current._1 + measurements.sum) / counter
      Some(avg, counter)
    }

    // Get energy prediction over a DStream
    def loadPredictionFunc(measurements: Seq[Double], state: Option[(Double,Int,Double,Double)]): Option[(Double,Int,Double,Double)] = {
      val current = state.getOrElse((0.0, 0, 0.0, 0.0)) // State = (Aggregate of measurement values, Aggregate counter of values, Median)
      val counter = current._2 + measurements.length

      val avg = (current._1 + measurements.sum) / counter

      val median = (current._3 + copySign(avg*0.01, current._1 - current._3))

      val loadPrediction = (avg + median) / 2

      Some(avg, counter, median, loadPrediction)
    }

    def testFunc(measurements: Seq[HouseMeasurement], state: Option[(String,Double)]): Option[(String,Double)] = {
      val values = measurements.map(h => h.value)
      val sum = values.sum
      Some(measurements.last.hhp_id, sum)
    }

		def main(args: Array[String]) {
				val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaReceiver")
				val ssc = new StreamingContext(conf, Seconds(10))
				val output_path = "/home/omcarvalho/tcc/project/spark/energy_project/output/"
				ssc.checkpoint("/tmp/energy_project/")

				// Read Kafka data stream
				val kafkaStream = KafkaUtils.createStream(ssc, "localhost:2181","group", Map("streaming-topic" -> 1))
				//kafkaStream.print()

				// Process stream into Measurements
				val measurements = kafkaStream.map { line =>
					val item = line._2.split(",")
          Measurement(item(0).toInt,
            item(1).toInt,
            item(2).toFloat,
            item(3).toInt,
            item(4).toInt,
            item(5).toInt,
            item(6).toInt,
						item(6) + ":" + item(5) + ":" + item(4)
          )
				}
				//measurements.print()

				// Filter valid load measurements
				val validMeasurements = measurements.filter { m => m.property == 1 && m.value > 0.0 }.window(Seconds(30))

				// Process stream load measurements into PlugMeasurements
				val plugMeasurements = validMeasurements.map { m =>
          PlugMeasurement(m.house_id,
						m.household_id,
						m.plug_id,
						m.value,
						m.hhp_id
          )
				}
				//plugMeasurements.print()

        // Process stream load measurements into HouseMeasurements
        val houseMeasurements = plugMeasurements.map { p =>
          HouseMeasurement(p.house_id,
						p.value,
						p.hhp_id
          )
				}

        // Average per plug and window
				val avgPlugs = plugMeasurements.map(p => (p.hhp_id, p.value)).updateStateByKey(avgFunc)
				//avgPlugs.print()

				// Average per house and window
				val avgHouses = houseMeasurements.map(p => (p.house_id, p.value)).updateStateByKey(avgFunc)
        //avgHouses.print()

        // Prediction per plug and window
				val predictionPlugs = plugMeasurements.map(p => (p.hhp_id, p.value)).updateStateByKey(loadPredictionFunc)
				//predictionPlugs.print()

				// Prediction per house and window
				val predictionHouses = houseMeasurements.map(p => (p.house_id, p.value)).updateStateByKey(loadPredictionFunc)
        //predictionHouses.print()

				// Print house prediction output to a set of files
        predictionHouses.repartition(1).foreachRDD { rdd =>
					// Save using hadoop format
					//rdd.saveAsTextFile(output_path+"predict_houses_"+rdd.id)

					// Save using a simple set of files
					var count = 0
					val id = rdd.id
					if (!rdd.isEmpty()) {
						rdd.foreachPartition { partition =>
							val string = partition.map(house => house.copy().toString()).reduce { (h1, h2) =>
								(h1 + "\n" + h2)
							}
							println(string)
							File(output_path + "prediction_houses_" + id + "_" + count).writeAll(string)
							count += 1
						}
					}

				}

				// Insert into Redis
			 	plugMeasurements.foreachRDD { rdd =>
					rdd.foreachPartition { partition =>
						val clients = new RedisClientPool("localhost", 6379)
						partition.foreach { case plug_meas: PlugMeasurement =>
							clients.withClient { client =>
									// Defines a implicit schema (house_id:household_id:plug_id => value)
									//val key = plug_meas.house_id.toString + ":" + plug_meas.household_id.toString + ":" + plug_meas.plug_id.toString
									//client.lpush(key, plug_meas.value)
							}
							//println(plug_meas)
						}
					}
				}

				// Print out the count of events received from this server in each batch
				var totalCount = 0L
				kafkaStream.foreachRDD((rdd: RDD[_], time: Time) => {
						val count = rdd.count()
						//println("\n-------------------")
						//println("Time: " + time)
						//println("-------------------")
						//println("Received " + count + " events\n")
            //println("-------------------")
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
