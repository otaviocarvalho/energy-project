package br.ufrgs.energy_stream

import org.apache.flink.api.scala._

object Job {
  def main(args: Array[String]) {

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // get input data
    val text = env.fromElements("1,1377986401,68.451,0,11,0,0")

    val counts = text.flatMap { _.split("([0-9]*),([0-9]*),([0-9]*.[0-9]*),([0-9]*),([0-9]*),([0-9]*),([0-9]*)") }

    // execute and print result
    counts.print()

  }
}
