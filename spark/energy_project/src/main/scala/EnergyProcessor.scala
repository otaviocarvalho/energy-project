package energy_stream

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.rdd.RDD

import java.lang.Math.copySign
import com.redis._
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerReceiverError, StreamingListenerBatchCompleted}

import scala.collection.mutable.HashMap
import scala.reflect.io.File
import scala.tools.cmd.gen.AnyVals

object EnergyProcessor {
	final val initialTimestamp = 1377986401
  final val fullDayTimestamp = 86400

//  // Get energy prediction over a DStream
//  def loadPredictionRollingMedianFunc(measurements: Seq[Double], state: Option[(Double,Int,Double,Double)]): Option[(Double,Int,Double,Double)] = {
//    val current = state.getOrElse((0.0, 0, 0.0, 0.0)) // State = (Aggregate of measurement values, Aggregate counter of values, Median)
//    val counter = current._2 + measurements.length
//
//    val avg = (current._1 + measurements.sum) / counter
//
//    val median = (current._3 + copySign(avg*0.01, current._1 - current._3))
//
//    val loadPrediction = (avg + median) / 2
//
//    Some(avg, counter, median, loadPrediction)
//  }

  // Get energy prediction over a DStream using previous calculated median
  def loadPredictionHouseStaticMedianFunc(measurements: Seq[HouseMeasurement], state: Option[(HousePrediction,Double,Int)]): Option[(HousePrediction,Double,Int)] = {
    val current = state.getOrElse((HousePrediction,0.0,0)) // State = (Measurement object, Aggregate of values, Aggregate counter of values)
    val counter = current._3 + measurements.length

    val values = measurements.map(h => h.value)
    val sum: Double = values.sum
    val totalizator = current._2 + sum

    val avg = totalizator / counter
    val median = measurements(0).median
    val loadPrediction = (avg + median) / 2

    val house = measurements.head
    val prediction = HousePrediction(house_id = house.house_id, timestamp = house.timestamp, predicted_load = loadPrediction)

    Some(prediction, totalizator, counter)
  }

  // Get energy prediction over a DStream using previous calculated median
  def loadPredictionPlugStaticMedianFunc(measurements: Seq[PlugMeasurement], state: Option[(PlugPrediction,Double,Int)]): Option[(PlugPrediction,Double,Int)] = {
    val current = state.getOrElse((PlugPrediction,0.0,0)) // State = (Measurement object, Aggregate of values, Aggregate counter of values)
    val counter = current._3 + measurements.length

    val values = measurements.map(h => h.value)
    val sum: Double = values.sum
    val totalizator = current._2 + sum

    val avg = totalizator / counter
    val median = measurements(0).median
    val loadPrediction = (avg + median) / 2

    val house = measurements.head
    val prediction = PlugPrediction(house_id = house.house_id, household_id = house.household_id, plug_id = house.plug_id, timestamp = house.timestamp, predicted_load = loadPrediction)

    Some(prediction, totalizator, counter)
  }

  // Get time slice based on timestamp
  def getTimeSliceFunc(timestamp: Int, timeFrameSize: Int): String = {
    val realTimestamp = (timestamp - initialTimestamp)
    val daysSinceBeginning: Int = realTimestamp / fullDayTimestamp

    var timeSlice = 0
    if (daysSinceBeginning > 0)
      timeSlice =  (realTimestamp / timeFrameSize) - (daysSinceBeginning*(fullDayTimestamp/timeFrameSize))
    else
      timeSlice =  (realTimestamp / timeFrameSize)

    //(timestamp, timeSlice, daysSinceBeginning)
    timeSlice.toString
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("EnergyProcessor")
    val ssc = new StreamingContext(conf, Seconds(10))
    val output_path = "/home/omcarvalho/tcc/project/spark/energy_project/output/"
    ssc.checkpoint("/tmp/energy_project/")

    // Read Kafka data stream
    val readParallellism = 4
    val kafkaStream = (1 to readParallellism).map {_ =>
      KafkaUtils.createStream(ssc, "localhost:2181","group", Map("streaming-topic" -> 1), StorageLevel.MEMORY_ONLY_SER).map(_._2)
    }
    val unifiedKafkaUnionStream = ssc.union(kafkaStream)
    val sparkProcessingParallelism = 1
    //val sparkProcessingParallelism = 16
    val unifiedKafkaStream = unifiedKafkaUnionStream.repartition(sparkProcessingParallelism)

    // Process stream into Measurements
    val measurements = unifiedKafkaStream.map { line =>
      //val item = line._2.split(",")
      val item = line.split(",")
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

    //#############################################################

    // Insert house measures into Redis based on time slice
    houseMeasurements.foreachRDD { rdd =>
      rdd.foreachPartition { partition =>
        val clients = new RedisClientPool("localhost", 6379)
        partition.foreach { case houseMeasurement: HouseMeasurement =>
          clients.withClient { client =>
            // Defines an implicit schema ("house_measure":window:time_slice:house_id => value)
            val key1min = "house_measure:1:" + getTimeSliceFunc(houseMeasurement.timestamp,1) + ":" +
              houseMeasurement.house_id.toString

            // Insert into Redis tables
            client.zadd(key1min,houseMeasurement.value,houseMeasurement.value)
          }
        }
      }
    }
    // Insert plug measures into Redis based on time slice
    plugMeasurements.foreachRDD { rdd =>
      rdd.foreachPartition { partition =>
        val clients = new RedisClientPool("localhost", 6379)
        partition.foreach { case plugMeas: PlugMeasurement =>
          clients.withClient { client =>
            // Defines an implicit schema ("plug_measure":window:time_slice:house_id:household_id:plug_id => value)
            val key1min = "plug_measure:1:" + getTimeSliceFunc(plugMeas.timestamp,1) + ":" +
              plugMeas.house_id.toString + ":" +
              plugMeas.household_id.toString + ":" +
              plugMeas.plug_id.toString

            // Insert into Redis tables
            client.zadd(key1min,plugMeas.value,plugMeas.value)
          }
        }
      }
    }

    //##################################################################

    // Calculate house averages based on measures, time frame and nearby time slices
    def setAverageHouses(houses: DStream[HouseMeasurement], timeFrameSize: Int) = {
      houses.foreachRDD { rdd =>
        rdd.foreachPartition { partition =>
          partition.foreach { case house: HouseMeasurement =>
            val clients = new RedisClientPool("localhost", 6379)
            clients.withClient { client =>
              val timeSliceEnd = getTimeSliceFunc(house.timestamp, timeFrameSize).toInt
              val timeSliceInit = timeSliceEnd - 1

              var measures_sum:Double = 0.0
              for (timeSlice <- timeSliceInit to timeSliceEnd) {
                // Get averages for houses/timeSlices
                val keyGet = "house_measure:" + timeFrameSize + ":" + getTimeSliceFunc(house.timestamp, timeFrameSize) + ":" + house.house_id
                val arraySize: Option[Long] = client.zcard(keyGet)
                if (arraySize.headOption.nonEmpty) {
                  val listAverages = client.zrange(keyGet, 0, -1).get
                  if (listAverages.nonEmpty) {
                    measures_sum = listAverages.map(x => x.toDouble).reduce((x,y) => x.toDouble + y.toDouble)
                    measures_sum = measures_sum / listAverages.length

                    // Set averages for houses/timeSlices
                    val keySet = "house_averages:" + timeFrameSize + ":" + getTimeSliceFunc(house.timestamp, timeFrameSize) + ":" + house.house_id
                    client.zadd(keySet,measures_sum,measures_sum)
                  }
                }
              }
            }
          }
        }
      }
    }
    setAverageHouses(houseMeasurements, 1)

    // Calculate plug averages based on measures, time frame and nearby time slices
    def setAveragePlugs(plugs: DStream[PlugMeasurement], timeFrameSize: Int) = {
      plugs.foreachRDD { rdd =>
        rdd.foreachPartition { partition =>
          partition.foreach { case plug: PlugMeasurement =>
            val clients = new RedisClientPool("localhost", 6379)
            clients.withClient { client =>
              val timeSliceEnd = getTimeSliceFunc(plug.timestamp, timeFrameSize).toInt
              val timeSliceInit = timeSliceEnd - 1

              var measures_sum:Double = 0.0
              for (timeSlice <- timeSliceInit to timeSliceEnd) {
                // Get averages for plugs/timeSlices
                val keyGet = "plug_measure:" + timeFrameSize + ":" + timeSlice +
                  ":" + plug.house_id + ":" +
                  plug.household_id.toString + ":" +
                  plug.plug_id.toString

                val arraySize: Option[Long] = client.zcard(keyGet)
                if (arraySize.headOption.nonEmpty) {
                  val listAverages = client.zrange(keyGet, 0, -1).get
                  if (listAverages.nonEmpty) {
                    measures_sum = listAverages.map(x => x.toDouble).reduce((x,y) => x.toDouble + y.toDouble)
                    measures_sum = measures_sum / listAverages.length

                    // Set averages for plugs/timeSlices
                    val keySet = "plug_averages:" + timeFrameSize + ":" + timeSlice +
                      ":" + plug.house_id + ":" +
                      plug.household_id.toString + ":" +
                      plug.plug_id.toString

                    client.zadd(keySet,measures_sum,measures_sum)
                  }
                }
              }
            }
          }
        }
      }
    }
    setAveragePlugs(plugMeasurements, 1)

    //##################################################################

    // Prediction using rolling median per plug
    //val predictionPlugs = plugMeasurements.map(p => (p.hhp_id, p.value)).updateStateByKey(loadPredictionRollingMedianFunc)
    //predictionPlugs.print()


    // Prediction using rolling median per house
    //val predictionHouses = houseMeasurements.map(h => (h.house_id, h.value)).updateStateByKey(loadPredictionRollingMedianFunc)
    //predictionHouses.print()

    //##################################################################

    // Get median per house per slice from ordered measurements (from Redis)
    def getMedianHouses(houses: DStream[HouseMeasurement], timeFrameSize: Int): HashMap[String,Double] = {
      val medianHouses = new HashMap[String,Double]
      houses.foreachRDD { rdd =>
        rdd.foreachPartition { partition =>
          partition.foreach { case house: HouseMeasurement =>
            val clients = new RedisClientPool("localhost", 6379)
            clients.withClient { client =>
              val timeSlice = getTimeSliceFunc(house.timestamp, timeFrameSize)
              //val key = "house_measure:" + timeFrameSize + ":" + timeSlice.toString + ":" + house.house_id
              val key = "house_averages:" + timeFrameSize + ":" + timeSlice.toString + ":" + house.house_id
              val arraySize: Option[Long] = client.zcard(key)

              var median = 0.0
              if (arraySize.get.toInt % 2 == 0) {
                val listAveragesLeft = client.zrange(key, arraySize.get.toInt/2, arraySize.get.toInt/2).get
                val listAveragesRight = client.zrange(key, arraySize.get.toInt/2+1, arraySize.get.toInt/2+1).get
                if (!listAveragesLeft.isEmpty && !listAveragesRight.isEmpty)
                  median = (listAveragesLeft.head.toDouble + listAveragesRight.head.toDouble) / 2
              }
              else {
                val listAverages = client.zrange(key, (arraySize.get.toInt+1)/2, (arraySize.get.toInt+1)/2).get
                if (!listAverages.isEmpty)
                  median = listAverages.head.toDouble
              }

              //println("key: "+key+" median: "+median)
              val keyMedian = "house_median:" + timeFrameSize + ":" + timeSlice.toString + ":" + house.house_id
              medianHouses.put(keyMedian, median)
            }
          }
        }
      }

      medianHouses
    }
    val housesMedians = getMedianHouses(houseMeasurements, 1)

    // Set median per slice into data stream (also save on Redis)
    def setMedianHouses(houses: DStream[HouseMeasurement], medians: HashMap[String,Double], timeFrameSize: Int): DStream[HouseMeasurement] = {
      houses.map { house =>
        val timeSlice = getTimeSliceFunc(house.timestamp, timeFrameSize)
        val key = "house_median:" + timeFrameSize + ":" + timeSlice.toString + ":" + house.house_id
        house.copy(median = medians.get(key).getOrElse(house.value))
      }
    }
    val houseMeasurementsWithMedian = setMedianHouses(houseMeasurements, housesMedians, 1)


    // Get median per plug per slice from ordered measurements (from Redis)
    def getMedianPlugs(plugs: DStream[PlugMeasurement], timeFrameSize: Int): HashMap[String,Double] = {
      val medianPlugs = new HashMap[String,Double]
      plugs.foreachRDD { rdd =>
        rdd.foreachPartition { partition =>
          partition.foreach { case plug: PlugMeasurement =>
            val clients = new RedisClientPool("localhost", 6379)
            clients.withClient { client =>
              val timeSlice = getTimeSliceFunc(plug.timestamp, timeFrameSize)
//              val key = "plug_measure:" + timeFrameSize + ":" + timeSlice.toString + ":" + plug.house_id + ":" +
//                                                                                plug.household_id.toString + ":" +
//                                                                                plug.plug_id.toString
              val key = "plug_averages:" + timeFrameSize + ":" + timeSlice.toString + ":" + plug.house_id + ":" +
                plug.household_id.toString + ":" +
                plug.plug_id.toString
              val arraySize: Option[Long] = client.zcard(key)

              var median = 0.0
              if (arraySize.get.toInt % 2 == 0) {
                val listAveragesLeft = client.zrange(key, arraySize.get.toInt/2, arraySize.get.toInt/2).get
                val listAveragesRight = client.zrange(key, arraySize.get.toInt/2+1, arraySize.get.toInt/2+1).get
                if (!listAveragesLeft.isEmpty && !listAveragesRight.isEmpty)
                  median = (listAveragesLeft.head.toDouble + listAveragesRight.head.toDouble) / 2
              }
              else {
                val listAverages = client.zrange(key, (arraySize.get.toInt+1)/2, (arraySize.get.toInt+1)/2).get
                if (!listAverages.isEmpty)
                  median = listAverages.head.toDouble
              }

              val keyMedian = "plug_median:" + timeFrameSize + ":" + timeSlice.toString + ":" + plug.house_id + ":" +
                plug.household_id.toString + ":" +
                plug.plug_id.toString
              medianPlugs.put(keyMedian, median)
            }
          }
        }
      }

      medianPlugs
    }
    val plugMedians = getMedianPlugs(plugMeasurements, 1)

    // Set median per slice into data stream (also save on Redis)
    def setMedianPlugs(plugs: DStream[PlugMeasurement], medians: HashMap[String,Double], timeFrameSize: Int): DStream[PlugMeasurement] = {
      plugs.map { plug =>
        val timeSlice = getTimeSliceFunc(plug.timestamp, timeFrameSize)
        val key = "plug_median:" + timeFrameSize + ":" + timeSlice.toString + ":" + plug.house_id + ":" +
                                                                        plug.household_id.toString + ":" +
                                                                        plug.plug_id.toString
        plug.copy(median = medians.get(key).getOrElse(plug.value))
      }
    }
    val plugMeasurementsWithMedian = setMedianPlugs(plugMeasurements, plugMedians, 1)

    //##################################################################

    // Prediction using Redis median per house
    val predictionHouses = houseMeasurementsWithMedian.map(h => (h.house_id.toString, h)).updateStateByKey(loadPredictionHouseStaticMedianFunc)
    //predictionHouses.print()

    // Prediction using Redis median per plug
    val predictionPlugs = plugMeasurementsWithMedian.map(h => (h.hhp_id, h)).updateStateByKey(loadPredictionPlugStaticMedianFunc)
    //predictionPlugs.print()

    // Insert house predictions into Redis based on time slice
    predictionHouses.foreachRDD { rdd =>
      rdd.foreachPartition { partition =>
        val clients = new RedisClientPool("localhost", 6379)
        partition.foreach { case prediction: (String, (HousePrediction, Double, Int)) =>
          clients.withClient { client =>
            val house_id = prediction._1
            val (housePrediction, _, _) = prediction._2

            // Defines an implicit schema ("house_averages":window:time_slice:house_id => value)
            val keyPred1Min = "house_prediction:1:" + (getTimeSliceFunc(housePrediction.timestamp,1).toInt + 1).toString +
                                                  ":" + house_id

            // Insert prediction into Redis tables
            client.zadd(keyPred1Min, housePrediction.predicted_load, housePrediction.predicted_load)
          }
        }
      }
    }
    // Insert plug predictions into Redis based on time slice
    predictionPlugs.foreachRDD { rdd =>
      rdd.foreachPartition { partition =>
        val clients = new RedisClientPool("localhost", 6379)
        partition.foreach { case prediction: (String, (PlugPrediction, Double, Int)) =>
          clients.withClient { client =>
            val hhp_id = prediction._1
            val (plugPrediction, _, _) = prediction._2

            // Defines an implicit schema ("plug_prediction":window:time_slice:house_id => value)
            val keyPred1Min = "plug_prediction:1:" + (getTimeSliceFunc(plugPrediction.timestamp,1).toInt + 1).toString +
                                                ":" + hhp_id

            // Insert prediction into Redis tables
            client.zadd(keyPred1Min, plugPrediction.predicted_load, plugPrediction.predicted_load)
          }
        }
      }
    }

    //##################################################################
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
    unifiedKafkaStream.foreachRDD((rdd: RDD[_], time: Time) => {
        val count = rdd.count()
        println("\n-------------------")
        println("Time: " + time)
        println("-------------------")
        println("Received " + count + " events\n")
        println("-------------------")
        totalCount += count
    })

//    ssc.start()
//    Thread.sleep(20 * 1000)
//    ssc.stop()
//    if (totalCount > 0) {
//        println("PASSED")
//    } else {
//        println("FAILED")
//    }

    ssc.start()
    ssc.awaitTermination()
  }
}

class CustomListener extends StreamingListener {
  override def onReceiverError(receiverError: StreamingListenerReceiverError): Unit ={
    //println(receiverError.toString)
  }
  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit ={
    //println(batchCompleted.batchInfo.processingDelay.get)
    //println(batchCompleted.batchInfo.schedulingDelay.get)
    //println(batchCompleted.batchInfo.totalDelay.get)
  }
}
