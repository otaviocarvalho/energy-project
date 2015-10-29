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

import scala.collection.mutable.HashMap
import scala.reflect.io.File
import scala.tools.cmd.gen.AnyVals

object EnergyProcessor {
	final val initialTimestamp = 1377986401
  final val fullDayTimestamp = 86400

  // Get energy prediction over a DStream
  def loadPredictionRollingMedianFunc(measurements: Seq[Double], state: Option[(Double,Int,Double,Double)]): Option[(Double,Int,Double,Double)] = {
    val current = state.getOrElse((0.0, 0, 0.0, 0.0)) // State = (Aggregate of measurement values, Aggregate counter of values, Median)
    val counter = current._2 + measurements.length

    val avg = (current._1 + measurements.sum) / counter

    val median = (current._3 + copySign(avg*0.01, current._1 - current._3))

    val loadPrediction = (avg + median) / 2

    Some(avg, counter, median, loadPrediction)
  }

  // Get energy prediction over a DStream using previous calculated median
  def loadPredictionStaticMedianFunc(measurements: Seq[HouseMeasurement], state: Option[(Double,Int,Double,Double)]): Option[(Double,Int,Double,Double)] = {
    val current = state.getOrElse((0.0, 0, 0.0, 0.0)) // State = (Aggregate of measurement values, Aggregate counter of values, Median)
    val counter = current._2 + measurements.length

    val values = measurements.map(h => h.value)
    val sum = values.sum

    val avg = (current._1 + sum) / counter

    val median = measurements(0).median

    val loadPrediction = (avg + median) / 2

    Some(avg, counter, median, loadPrediction)
  }

  // Get time slice based on timestamp
  def getTimeSliceHouseFunc(h: HouseMeasurement, timeFrameSize: Int): String = {
    val realTimestamp = (h.timestamp - initialTimestamp)
    val daysSinceBeginning: Int = realTimestamp / fullDayTimestamp

    var timeSlice = 0
    if (daysSinceBeginning > 0)
      timeSlice =  (realTimestamp / timeFrameSize) - (daysSinceBeginning*(fullDayTimestamp/timeFrameSize))
    else
      timeSlice =  (realTimestamp / timeFrameSize)

    //(p.timestamp, timeSlice, daysSinceBeginning)
    timeSlice.toString
  }

  // Get time slice based on timestamp
  def getTimeSlicePlugFunc(p: PlugMeasurement, timeFrameSize: Int): String = {
    val realTimestamp = (p.timestamp - initialTimestamp)
    val daysSinceBeginning: Int = realTimestamp / fullDayTimestamp

    var timeSlice = 0
    if (daysSinceBeginning > 0)
      timeSlice =  (realTimestamp / timeFrameSize) - (daysSinceBeginning*(fullDayTimestamp/timeFrameSize))
    else
      timeSlice =  (realTimestamp / timeFrameSize)

    //(p.timestamp, timeSlice, daysSinceBeginning)
    timeSlice.toString
  }

//    def testFunc(measurements: Seq[HouseMeasurement], state: Option[(String,Double)]): Option[(String,Double)] = {
//      val values = measurements.map(h => h.value)
//      val sum = values.sum
//      Some(measurements.last.hhp_id, sum)
//    }

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
        m.timestamp,
        m.value,
        0,
        m.hhp_id
      )
    }
    //plugMeasurements.print()

    // Process stream load measurements into HouseMeasurements
    val houseMeasurements = plugMeasurements.map { p =>
      HouseMeasurement(p.house_id,
        p.timestamp,
        p.value,
        0,
        p.hhp_id
      )
    }

    // Prediction per plug and window
    val predictionPlugs = plugMeasurements.map(p => (p.hhp_id, p.value)).updateStateByKey(loadPredictionRollingMedianFunc)
    //predictionPlugs.print()

    // Get median per slice
    //def getMedianHouses(houses: DStream[HouseMeasurement], timeFrameSize: Int): HouseMeasurement = {
    def getMedianHouses(houses: DStream[HouseMeasurement], timeFrameSize: Int): HashMap[String,Double] = {
      val medianHouses = new HashMap[String,Double]
      houses.foreachRDD { rdd =>
        rdd.foreachPartition { partition =>
          partition.foreach { case house: HouseMeasurement =>
            val clients = new RedisClientPool("localhost", 6379)
            clients.withClient { client =>
              val timeSlice = getTimeSliceHouseFunc(house, timeFrameSize)
              val key = "house_measure:" + timeFrameSize + ":" + timeSlice.toString + ":" + house.house_id
              //val key = "house_measure:1:0:0"
              val arraySize: Option[Long] = client.zcard(key)

              //var listAverages = List("")
              var median = 0.0
              if (arraySize.get.toInt % 2 == 0) {
                //val medianLeft = client.zrange(key, arraySize.get.toInt, arraySize.get.toInt)
                //val medianRight = client.zrange(key, arraySize.get.toInt, arraySize.get.toInt)
                val listAveragesLeft = client.zrange(key, arraySize.get.toInt/2, arraySize.get.toInt/2).get
                val listAveragesRight = client.zrange(key, arraySize.get.toInt/2+1, arraySize.get.toInt/2+1).get
                median = (listAveragesLeft.head.toDouble + listAveragesRight.head.toDouble) / 2
              }
              else {
                val listAverages = client.zrange(key, (arraySize.get.toInt+1)/2, (arraySize.get.toInt+1)/2).get
                median = listAverages.head.toDouble
              }

              println("key: "+key+" median: "+median)
              medianHouses.put(key, median)
            }
          }
        }
      }

      //val medianSlice = getHouseMedianSlice(house.house_id, timeSlice, house.timestamp, client)
      //val houseWithMedian = house.copy(median = medianSlice)
      medianHouses
    }
    val housesMedians = getMedianHouses(houseMeasurements, 1)
    def setMedianHouses(houses: DStream[HouseMeasurement], medians: HashMap[String,Double], timeFrameSize: Int): DStream[HouseMeasurement] = {
      houses.map { house =>
        val timeSlice = getTimeSliceHouseFunc(house, timeFrameSize)
        val key = "house_measure:" + timeFrameSize + ":" + timeSlice.toString + ":" + house.house_id
        house.copy(median = medians.get(key).getOrElse(house.value))
      }
    }
    val houseMeasurementsWithMedian = setMedianHouses(houseMeasurements, housesMedians, 1)
    // Prediction per house and window
    val predictionHouses = houseMeasurements.map(h => (h.house_id, h)).updateStateByKey(loadPredictionStaticMedianFunc)
    //val predictionHouses = houseMeasurements.map(h => (h.house_id, h.value)).updateStateByKey(loadPredictionRollingMedianFunc)
    predictionHouses.print()

    // Insert plug measures into Redis based on time slice
    plugMeasurements.foreachRDD { rdd =>
      rdd.foreachPartition { partition =>
        val clients = new RedisClientPool("localhost", 6379)
        partition.foreach { case plugMeas: PlugMeasurement =>
          clients.withClient { client =>
            // Defines an implicit schema ("plug_measure":window:time_slice:house_id:household_id:plug_id => value)
            val key1min = "plug_measure:1:" + getTimeSlicePlugFunc(plugMeas,1) + ":" +
              plugMeas.house_id.toString + ":" +
              plugMeas.household_id.toString + ":" +
              plugMeas.plug_id.toString
            val key5min = "plug_measure:5:" + getTimeSlicePlugFunc(plugMeas,5) + ":" +
              plugMeas.house_id.toString + ":" +
              plugMeas.household_id.toString + ":" +
              plugMeas.plug_id.toString

            // Insert into Redis tables
            client.zadd(key1min,plugMeas.value,plugMeas.value)
            client.zadd(key5min,plugMeas.value,plugMeas.value)
          }
        }
      }
    }
    // Insert house measures into Redis based on time slice
    houseMeasurements.foreachRDD { rdd =>
      rdd.foreachPartition { partition =>
        val clients = new RedisClientPool("localhost", 6379)
        partition.foreach { case houseMeasurement: HouseMeasurement =>
          clients.withClient { client =>
            // Defines an implicit schema ("house_measure":window:time_slice:house_id => value)
            val key1min = "house_measure:1:" + getTimeSliceHouseFunc(houseMeasurement,1) + ":" +
              houseMeasurement.house_id.toString
            val key5min = "house_measure:5:" + getTimeSliceHouseFunc(houseMeasurement,5) + ":" +
              houseMeasurement.house_id.toString

            // Insert into Redis tables
            client.zadd(key1min,houseMeasurement.value,houseMeasurement.value)
            client.zadd(key5min,houseMeasurement.value,houseMeasurement.value)
          }
        }
      }
    }

//    // Insert house predictions into Redis based on time slice
//    predictionHouses.foreachRDD { rdd =>
//      rdd.foreachPartition { partition =>
//        val clients = new RedisClientPool("localhost", 6379)
//        partition.foreach { case prediction: Option[(Double,Int,Double,Double)] =>
//          clients.withClient { client =>
//            // Defines an implicit schema ("house_measure":window:time_slice:house_id => value)
//            val key1min = "house_prediction:1:" + getTimeSliceHouseFunc(house,1) + ":" +
//              house.house_id.toString
//            val key5min = "house_prediction:5:" + getTimeSliceHouseFunc(house,5) + ":" +
//              house.house_id.toString
//
//            // Insert into Redis tables
//            client.zadd(key1min,houseMeasurement.value,houseMeasurement.value)
//            client.zadd(key5min,houseMeasurement.value,houseMeasurement.value)
//          }
//        }
//      }
//    }

//    // Print house prediction output to a set of files
//    predictionHouses.repartition(1).foreachRDD { rdd =>
//      // Save using hadoop format
//      //rdd.saveAsTextFile(output_path+"predict_houses_"+rdd.id)
//
//      // Save using a simple set of files
//      var count = 0
//      val id = rdd.id
//      if (!rdd.isEmpty()) {
//        rdd.foreachPartition { partition =>
//          val string = partition.map(house => house.copy().toString()).reduce { (h1, h2) =>
//            (h1 + "\n" + h2)
//          }
//          //println(string)
//          File(output_path + "prediction_houses_" + id + "_" + count).writeAll(string)
//          count += 1
//        }
//      }
//
//    }

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