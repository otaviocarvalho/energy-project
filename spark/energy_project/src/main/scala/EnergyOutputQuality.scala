package energy_stream

import com.redis.RedisClientPool

import scala.collection.mutable.HashMap
import scala.io.Source

object EnergyOutputQuality {
  final val initialTimestamp = 1377986401
  final val fullDayTimestamp = 86400

  // Get time slice based on timestamp (seconds) and timeframesize (minutes)
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

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: EnergyOutputQuality <original data> <prediction data>")
      System.exit(1)
    }

    // Open CSV for reading
    val origSource = Source.fromFile(args(0))
    //val predictSource = Source.fromFile(args(1))
    var timeframeGiven = 1

    // Calculate averages
    val houses = new HashMap[String,List[HouseMeasurement]]
    val plugs = new HashMap[String,List[PlugMeasurement]]

    for (line <- origSource.getLines){
      val lineSplit = line.split(",")
      //println(line)
      val measurement = Measurement(lineSplit(0).toInt,
        lineSplit(1).toInt,
        lineSplit(2).toFloat,
        lineSplit(3).toInt,
        lineSplit(4).toInt,
        lineSplit(5).toInt,
        lineSplit(6).toInt,
        lineSplit(6) + ":" + lineSplit(5) + ":" + lineSplit(4)
      )

      // Add houses to hashmap
      if (measurement.property == 1 && measurement.value > 0.0) {
        // schema: <timeframe>:<timeslice>:<house_id>
        //val key = "house:" + timeframeGiven*60 + ":" + getTimeSliceFunc(measurement.timestamp,timeframeGiven*60) + ":" + measurement.house_id.toString
        val key = "house:" + timeframeGiven + ":" + getTimeSliceFunc(measurement.timestamp,timeframeGiven) + ":" + measurement.house_id.toString

        val currentList: List[HouseMeasurement] = houses.get(key).getOrElse(List())
        var updatedListHouse = List(HouseMeasurement(house_id = measurement.house_id,
          timestamp = measurement.timestamp,
          value = measurement.value,
          0,
          hhp_id = measurement.hhp_id))

        if (!currentList.isEmpty) {
          updatedListHouse ++= currentList
        }
        houses.put(key, updatedListHouse)
      }

      // Add plugs to hashmap
      if (measurement.property == 1 && measurement.value > 0.0) {
        // schema: <timeframe>:<timeslice>:<house_id>:<household_id>:<plug_id>
        val key = "plug:" + timeframeGiven + ":" + getTimeSliceFunc(measurement.timestamp,timeframeGiven) + ":" +
          measurement.house_id.toString + ":" +
          measurement.household_id.toString + ":" +
          measurement.plug_id.toString

        val currentList: List[PlugMeasurement] = plugs.get(key).getOrElse(List())
        var updatedListPlug = List(PlugMeasurement(house_id = measurement.house_id,
          household_id = measurement.household_id,
          plug_id = measurement.plug_id,
          timestamp = measurement.timestamp,
          value = measurement.value,
          0,
          hhp_id = measurement.hhp_id))

        if (!currentList.isEmpty) {
          updatedListPlug ++= currentList
        }
        plugs.put(key, updatedListPlug)
      }

    }

    // Averages from houses CSV input
    val houseAverages = new HashMap[String,Double]
    //for (house <- List(0)) {
    for (house <- 0 to 40) {
      for (timeframe <- List(timeframeGiven)) {
        val numTimeSlices = (initialTimestamp + fullDayTimestamp) / timeframe
        for (timeslice <- (0 to 1)) {
        //for (timeslice <- (0 to numTimeSlices)) {
          //val key = "house:" + timeframe*60 + ":" + timeslice + ":" + house
          val key = "house:" + timeframe + ":" + timeslice + ":" + house
          //println(key)

          val measureAux = houses.get(key)
          if (measureAux.nonEmpty) {
            val emptyMeas = HouseMeasurement(house_id = 0, timestamp = 0, value = 0, median = 0, hhp_id = "")

            val houseMeasuresListSum = measureAux.getOrElse(List(emptyMeas)).map(m => m.value).sum
            val houseMeasuresCounter = measureAux.getOrElse(List(emptyMeas)).size
            val average = (houseMeasuresListSum / houseMeasuresCounter)

            println(key + " | count# " + houseMeasuresCounter + " | avg# " + average)
            houseAverages.put(key, average)
          }
        }
      }
    }
    // Averages from plugs CSV input
    val plugAverages = new HashMap[String,Double]
    for (house <- 0 to 40) {
      for (household <- 0 to 40) {
        for (plug <- 0 to 40) {
        //for (plug <- 0 to 2125) {
          for (timeframe <- List(timeframeGiven)) {
            val numTimeSlices = (initialTimestamp + fullDayTimestamp) / timeframe
            for (timeslice <- (0 to 1)) {
              //for (timeslice <- (0 to numTimeSlices)) {
              //val key = "house:" + timeframe*60 + ":" + timeslice + ":" + house
              val key = "plug:" + timeframe + ":" + timeslice + ":" +
                house + ":" +
                household + ":" +
                plug

              val measureAux = plugs.get(key)
              if (measureAux.nonEmpty) {
                val emptyMeas = PlugMeasurement(house_id = 0, household_id = 0, plug_id = 0, timestamp = 0, value = 0, median = 0, hhp_id = "")

                val plugMeasuresListSum = measureAux.getOrElse(List(emptyMeas)).map(m => m.value).sum
                val plugMeasuresCounter = measureAux.getOrElse(List(emptyMeas)).size
                val average = (plugMeasuresListSum / plugMeasuresCounter)

                println(key + " | count# " + plugMeasuresCounter + " | avg# " + average)
                plugAverages.put(key, average)
              }
            }
          }
        }
      }
    }

    // House predictions from Redis
    val housePredictions = new HashMap[String,Double]
    val clients = new RedisClientPool("localhost", 6379)
    clients.withClient { client =>
      //for (house <- List(0)) {
      for (house <- 0 to 40) {
        for (timeframe <- List(timeframeGiven)) {
          val numTimeSlices = (initialTimestamp + fullDayTimestamp) / timeframe
          for (timeslice <- (0 to 0)) {
            //for (timeslice <- (0 to numTimeSlices)) {
            //val key = "house_prediction:" + timeframeGiven * 60 + ":" + timeslice + ":" + house
            val key = "house_prediction:" + timeframeGiven + ":" + (timeslice.toInt+1) + ":" + house
            val listPredictions = client.zrange(key, 0, -1).get
            if (listPredictions.nonEmpty) {
              println(key + " | house prediction# " + listPredictions.head.toDouble)
              housePredictions.put(key, listPredictions.head.toDouble)
            }
          }
        }
      }
    }

    // Plug predictions from Redis
    val plugPredictions = new HashMap[String,Double]
    clients.withClient { client =>
      //for (house <- List(0)) {
      for (house <- 0 to 40) {
        for (household <- 0 to 40) {
          for (plug <- 0 to 40) {
            for (timeframe <- List(timeframeGiven)) {
              val numTimeSlices = (initialTimestamp + fullDayTimestamp) / timeframe
              for (timeslice <- (0 to 0)) {
                //for (timeslice <- (0 to numTimeSlices)) {
                val key = "plug_prediction:" + timeframeGiven + ":" + (timeslice.toInt + 1) + ":" +
                  house + ":" +
                  household + ":" +
                  plug
                val listPredictions = client.zrange(key, 0, -1).get
                if (listPredictions.nonEmpty) {
                  println(key + " | plug prediction# " + listPredictions.head.toDouble)
                  plugPredictions.put(key, listPredictions.head.toDouble)
                }
              }
            }
          }
        }
      }
    }

    // House error measurement
    //for (house <- List(0)) {
    for (house <- 0 to 40) {
      for (timeframe <- List(timeframeGiven)) {
        val numTimeSlices = (initialTimestamp + fullDayTimestamp) / timeframe
        for (timeslice <- (0 to 0)) {
          val keyPrediction = "house_prediction:" + timeframeGiven + ":" + (timeslice.toInt+1) + ":" + house
          val keyAverages = "house:" + timeframe + ":" + timeslice + ":" + house
          val predictionVal = housePredictions.get(keyPrediction)
          val averageVal = houseAverages.get(keyAverages)
          if (predictionVal.nonEmpty && averageVal.nonEmpty) {
            println("ID: " +  keyAverages + " | House Prediction: " + predictionVal.get + " | Measurement: " + averageVal.get + " | Error: " + (predictionVal.get - averageVal.get).abs)
          }
        }
      }
    }
    // Plug error measurement
    //for (plug <- List(0)) {
    for (house <- 0 to 40) {
      for (household <- 0 to 40) {
        for (plug <- 0 to 40) {
          for (timeframe <- List(timeframeGiven)) {
            val numTimeSlices = (initialTimestamp + fullDayTimestamp) / timeframe
            for (timeslice <- (0 to 0)) {
              val keyPrediction = "plug_prediction:" + timeframeGiven + ":" + (timeslice.toInt + 1) + ":" +
                house + ":" +
                household + ":" +
                plug
              val keyAverages = "plug:" + timeframe + ":" + timeslice + ":" +
                house + ":" +
                household + ":" +
                plug
              val predictionVal = plugPredictions.get(keyPrediction)
              val averageVal = plugAverages.get(keyAverages)
              if (predictionVal.nonEmpty && averageVal.nonEmpty) {
                println("ID: " + keyAverages + " | Plug Prediction: " + predictionVal.get + " | Measurement: " + averageVal.get + " | Error: " + (predictionVal.get - averageVal.get).abs)
              }
            }
          }
        }
      }
    }

    origSource.close()
    //predictSource.close()
  }
}
