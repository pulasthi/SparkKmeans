package org.pulasthi.sparkkmeans.computeonly

import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

import com.google.common.base.Optional
import org.apache.commons.cli.{CommandLine, HelpFormatter, Options}
import org.apache.spark.{SparkConf, SparkContext}

object DataFlowTest {
  var programOptions: Options = new Options();

  def main(args: Array[String]): Unit = {
    var dateFormat: DateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss")

    programOptions.addOption("n", true, "Number of points")
    programOptions.addOption("m", true, "Max iteration count")
    programOptions.addOption("T", true, "Number of threads")
    programOptions.addOption("W", true, "Number of worker")

    val parserResult: Optional[CommandLine] = UtilsCustom.parseCommandLineArguments(args, programOptions)
    if (!parserResult.isPresent) {
      println(UtilsCustom.ERR_PROGRAM_ARGUMENTS_PARSING_FAILED)
      new HelpFormatter().printHelp(UtilsCustom.PROGRAM_NAME, programOptions)
      return
    }

    val cmd: CommandLine = parserResult.get
    if (!(cmd.hasOption("n")  && cmd.hasOption("m") && cmd.hasOption("T"))) {
      println(UtilsCustom.ERR_INVALID_PROGRAM_ARGUMENTS)
      new HelpFormatter().printHelp(UtilsCustom.PROGRAM_NAME, programOptions)
      return
    }

    val numPoints: Int = cmd.getOptionValue("n").toInt
    val maxIterations: Int = cmd.getOptionValue("m").toInt
    val numberOfThreads: Int = if (cmd.hasOption("T")) cmd.getOptionValue("T").toInt else 1
    val numberOfWorkers: Int = if (cmd.hasOption("W")) cmd.getOptionValue("W").toInt else 1
    val parallism = numberOfThreads*numberOfWorkers

    val conf = new SparkConf().setAppName("sparkKMeans")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    println("=== Program Started on " + dateFormat.format(new Date) + " ===")


    val startTime = System.currentTimeMillis();
    for ( k <- 0 until maxIterations ) {

      val runRDD = sc.parallelize(1 to parallism, parallism);
      val mappedRDD = runRDD.map { index => {
        //init data points
        val data: Array[Double] = Array.ofDim(numPoints);
        for (i <- 0 until numPoints) {
          data(i) = index*k;
        }
        data
      }
      }

      val result = mappedRDD.reduce(reduceFun)
      sc.broadcast(result)
    }
    println("Total Time for 1" + maxIterations + " : " + (System.currentTimeMillis() - startTime));
  }

  def reduceFun(a: Array[Double], b: Array[Double]): Array[Double] = {
    a.zip(b).map { case (x, y) => x + y }
  }
}
