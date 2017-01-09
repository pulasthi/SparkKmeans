package org.pulasthi.sparkkmeans.computeonly

import java.io.{File, PrintWriter}
import java.text.{SimpleDateFormat, DateFormat}
import java.util.Date
import java.util.concurrent.TimeUnit

import com.google.common.base.{Stopwatch, Optional}
import org.apache.commons.cli.{Options, CommandLine, HelpFormatter}
import org.apache.spark.mllib.clustering.{KMeansModel, KMeans}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by pulasthi on 7/2/16.
  */
object Driver {
  var programOptions: Options = new Options();

  def main(args: Array[String]): Unit = {
    var dateFormat: DateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss")

    programOptions.addOption("n", true, "Number of points")
    programOptions.addOption("d", true, "Dimensionality")
    programOptions.addOption("k", true, "Number of centers")
    programOptions.addOption("t", true, "Error threshold")
    programOptions.addOption("m", true, "Max iteration count")
    programOptions.addOption("b", true, "Is big-endian?")
    programOptions.addOption("T", true, "Number of threads")
    programOptions.addOption("W", true, "Number of worker")
    programOptions.addOption("c", true, "Initial center file")
    programOptions.addOption("p", true, "Points file")
    programOptions.addOption("o", true, "Cluster assignment output file")
    programOptions.addOption("mmpn", true, "mmaps per node")
    programOptions.addOption("mmdir", true, "mmaps dir")
    programOptions.addOption("bind", true, "Bind threads [true/false]")

    val parserResult: Optional[CommandLine] = UtilsCustom.parseCommandLineArguments(args, programOptions)
    if (!parserResult.isPresent) {
      println(UtilsCustom.ERR_PROGRAM_ARGUMENTS_PARSING_FAILED)
      new HelpFormatter().printHelp(UtilsCustom.PROGRAM_NAME, programOptions)
      return
    }

    val cmd: CommandLine = parserResult.get
    if (!(cmd.hasOption("n") && cmd.hasOption("d") && cmd.hasOption("k") && cmd.hasOption("t") &&
      cmd.hasOption("m") && cmd.hasOption("b") && cmd.hasOption("c") && cmd.hasOption("p") && cmd.hasOption("T"))) {
      println(UtilsCustom.ERR_INVALID_PROGRAM_ARGUMENTS)
      new HelpFormatter().printHelp(UtilsCustom.PROGRAM_NAME, programOptions)
      return
    }

    val numPoints: Int = cmd.getOptionValue("n").toInt
    val dimension: Int = cmd.getOptionValue("d").toInt
    val epsilon: Double = cmd.getOptionValue("t").toDouble
    val numCenters: Int = cmd.getOptionValue("k").toInt
    val maxIterations: Int = cmd.getOptionValue("m").toInt
    val outputFile: String = if (cmd.hasOption("o")) cmd.getOptionValue("o") else ""
    val centersFile: String = if (cmd.hasOption("c")) cmd.getOptionValue("c") else ""
    val pointsFile: String = if (cmd.hasOption("p")) cmd.getOptionValue("p") else ""
    val numberOfThreads: Int = if (cmd.hasOption("T")) cmd.getOptionValue("T").toInt else 1
    val numberOfWorkers: Int = if (cmd.hasOption("W")) cmd.getOptionValue("W").toInt else 1


    val conf = new SparkConf().setAppName("sparkKMeans")
    val sc = new SparkContext(conf)


    //val mainTimer: Stopwatch = Stopwatch.createStarted

    println("=== Program Started on " + dateFormat.format(new Date) + " ===")
    println("  Reading Points ... ")

    val timerstart = System.currentTimeMillis();

    val parallelism = numberOfThreads*numberOfWorkers;
    val data = sc.textFile(pointsFile).repartition(parallelism);
    println("Number of partitions : " + data.getNumPartitions);
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

    println("  Reading Centers ... ")
    val centers = sc.textFile(centersFile);
    val parsedCenters = centers.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()
    val parsedCentersArray = parsedCenters.collect()

    println("  Initializing KMeansModel with centroids ... ")
    // Setting initial centroids for K Means
    val kMeansModel = new KMeansModel(parsedCentersArray);
    println(" Performing KMeans calculations ")

    val model = new KMeans()
      .setK(numCenters)
      .setInitialModel(kMeansModel)
      .setMaxIterations(maxIterations)
      .setEpsilon(epsilon)
      .run(parsedData);

    val timeend = System.currentTimeMillis();

    println("=== Program terminated successfully on " + dateFormat.format(new Date) + " took " + (timeend-timerstart) + " ms ===")

    val pw = new PrintWriter(new File(outputFile.replace(".txt","_" + epsilon + ".txt")))
    var predicted = model.clusterCenters;
    predicted.map(center => pw.println(center(0) + "\t" + center(1)));

    pw.flush()
    pw.close

  }
}
