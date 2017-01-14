package org.pulasthi.sparkkmeans.computeonly

import com.google.common.base.Optional
import org.apache.commons.cli.{HelpFormatter, CommandLine, Options}
import org.apache.spark.{TaskContext, SparkContext, SparkConf}

/**
  * Created by pulasthi on 1/14/17.
  */
object Reduce {
  var programOptions: Options = new Options();

  def main(args: Array[String]): Unit = {

    programOptions.addOption("T", true, "Number of threads")
    programOptions.addOption("W", true, "Number of worker")

    val parserResult: Optional[CommandLine] = UtilsCustom.parseCommandLineArguments(args, programOptions)
    if (!parserResult.isPresent) {
      println(UtilsCustom.ERR_PROGRAM_ARGUMENTS_PARSING_FAILED)
      new HelpFormatter().printHelp(UtilsCustom.PROGRAM_NAME, programOptions)
      return
    }
    val cmd: CommandLine = parserResult.get
    val parallelism = cmd.getOptionValue("T").toInt*cmd.getOptionValue("W").toInt

    val conf = new SparkConf().setAppName("SimpleReduce")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val tempArray = 0 to (parallelism-1) toArray;
    val distData = sc.parallelize(tempArray,parallelism).map(_ => {
      val currntTime = System.currentTimeMillis();
      currntTime;
    }).reduce((x,y) => x+y);
    val timeafterReduce = System.currentTimeMillis();
    val averageTime = distData/parallelism;

    println("============= Reduce Time to Master +++++++++ :" + (timeafterReduce - averageTime));

    val redbyKey = sc.parallelize(tempArray,parallelism).map(_ => {
      val localMachine = java.net.InetAddress.getLocalHost();
      (0,(localMachine.getHostName(),System.currentTimeMillis()))
    }).reduceByKey((x,y) => {
      val localMachine = java.net.InetAddress.getLocalHost().getHostName();
      var result = ("-1",-1l);
      if(localMachine == x._1){
        result = ("-1", (System.currentTimeMillis() - x._2))
      }
      if(localMachine == y._1){
        result = ("-1", (System.currentTimeMillis() - y._2))
      }
      result
    }).collect();

    val hosts = sc.parallelize(tempArray,parallelism).map(_ => {
      val localMachine = java.net.InetAddress.getLocalHost();
      (0,localMachine + ":" + System.currentTimeMillis())
    }).reduceByKey((x,y) => {
      val localMachine = java.net.InetAddress.getLocalHost().getHostName();
      var split1 = x.split(":");
      var split2 = y.split(":");
      if(split1.length > 2){
        return x + "::::" + "( " + localMachine + "||" + split2(0) + ")"
      }else{
        split1(0) + "::::" + "( " + localMachine + "||" + split2(0) + ")"
      }

    }).collect();

    for ( x <- hosts ) {
      println( "============= Reduce By Key +++++++++ :" + x._1 );
    }
    for ( x <- redbyKey ) {
      println( "============= Reduce By Key +++++++++ :" + x._2 );
    }
    val localMachine = java.net.InetAddress.getLocalHost();

    println( " Driver" + localMachine.getHostName() )

  }


}
