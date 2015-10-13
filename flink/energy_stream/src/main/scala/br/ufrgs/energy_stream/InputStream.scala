package br.ufrgs.energy_stream

import org.apache.flink.streaming.api.scala._
import java.lang.Math.copySign
//import org.apache.flink.api.scala._

object InputStream {

  case class Measurement(id: Int,
                         timestamp: Int,
                         value: Float,
                         property: Int,
                         plug_id: Int,
                         household_id: Int,
                         house_id: Int,
                         hhp_id: String
                         )


  case class HouseholdLoadMeasurement(household_id: Int,
                             value: Double)

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //val env = ExecutionEnvironment.getExecutionEnvironment

    //Create streams for names and ages by mapping the inputs to the corresponding objects
    val text = env.readFileStream("file:///home/omcarvalho/tcc/dataset/sample.csv")
    //val text = env.readTextFile("file:///home/omcarvalho/tcc/dataset/sample.csv")

    // Split text in lines
    val lines = text.flatMap( _.split("\n"))

    // Split text by commas and group in Measurements
    //val Line = """([0-9]*),([0-9]*),([0-9]*.[0-9]*),([0-9]*),([0-9]*),([0-9]*),([0-9]*)\n""".r
    val measurements = lines.map(l => {
      val item = l.split(",")
      Measurement(item(0).toInt,
        item(1).toInt,
        item(2).toFloat,
        item(3).toInt,
        item(4).toInt,
        item(5).toInt,
        item(6).toInt,
        item(6).toString+item(5).toString+item(4).toString
      )
    })

    // Filter load values from stream
    //val load_measurements = measurements.filter(_.property == 1).filter(_.value > 0)
    val load_measurements = measurements.filter(_.property == 1)

    // Calculate average consumption
    val averageComsumption = load_measurements.keyBy("house_id").mapWithState {
        (in: Measurement, state: Option[(Double, Long, Double)]) =>
        {
            // Option[value, counter, median]
            val current = state.getOrElse((0.0, 0L, 0.0))
            val updated = (current._1 + in.value, current._2 + 1, current._3)
            val avg = (updated._1 / updated._2).toFloat

            //val average = (average + ((in.value - average) * 0.1)))
            //val median = (median + copySign(avg * 0.01, current - median))
            val median: Double = (current._3 + copySign(avg * 0.01, current._1 - current._3))

            val loadPrediction = (avg + median) / 2
            val householdMeasurement = HouseholdLoadMeasurement(in.household_id, loadPrediction)

            (householdMeasurement, Some(updated))

        }
    }

    // GroupBy
    val group_by = load_measurements.keyBy("hhp_id").map {
        x => Tuple2(x.hhp_id, x.value)
    }
    .keyBy(0)

    load_measurements.writeAsText("file:///home/omcarvalho/tcc/dataset/out1")
    averageComsumption.writeAsText("file:///home/omcarvalho/tcc/dataset/out2")
    group_by.writeAsText("file:///home/omcarvalho/tcc/dataset/out2")

    //val test = load_measurements.filter(_.household_id == 0)
    //lines.writeAsText("file:///home/omcarvalho/tcc/dataset/out1")
    //lines print

    env.execute("Energy Stream - Scala InputStream")
  }

}
