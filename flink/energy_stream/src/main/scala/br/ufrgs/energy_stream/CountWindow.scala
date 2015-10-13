package br.ufrgs.energy_stream

import java.util.concurrent.TimeUnit

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.windowing.Time

/**
 * Created by omcarvalho on 05/10/15.
 */
object CountWindow {

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

    //Create streams for names and ages by mapping the inputs to the corresponding objects
    val text = env.readFileStream("file:///home/omcarvalho/tcc/dataset/sample.csv")

    // Split text in lines
    val lines = text.flatMap( _.split("\n"))

    // Split text by commas and group in Measurements
     val measurements: DataStream[Measurement] = lines.map(l => {
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

    val house_measurements = measurements.keyBy("house_id").map(l => (l.house_id,l.value))
    house_measurements.print()
    //val window_measurements = lines.map(l => (l(6), 1))
    //val window_measurements =
      //measurements.keyBy(0)
      //:.window(Time.of(100, TimeUnit.MILLISECONDS))
      //.sum(1)
      //.flatten()
      //.printToErr()

    //window_measurements.writeAsText("file:///home/omcarvalho/tcc/dataset/count")

    env.execute("Energy Stream - Scala InputStream")
  }


}
