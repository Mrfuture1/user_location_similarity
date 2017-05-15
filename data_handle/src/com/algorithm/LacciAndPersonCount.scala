package com.algorithm

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mercury on 17-4-22
  * project:location_opendsg
  * Package Name:com.algorithm
  * Description:
  */
object LacciAndPersonCount {
    def main(args: Array[String]): Unit = {
        if (args.length < 3) {
            println("Usage:KMeansClustering trainingDataFilePath testDataFilePath numClusters numIterations runTimes")
            sys.exit(1)
        }

        val Array(inputFilePath,  lacciCountOutput, personCountOutput) = args
        val conf = new SparkConf().setAppName("location_opendsg_LacciAndPersonCount")
                .setMaster("local")
        val sc = new SparkContext(conf)
        val inputData=sc.textFile(inputFilePath).map(x=>x.split("\t")).map(x=>(x(0),x(1),x(2)))

        val lacciCount=inputData.map(x=>(x._2.toInt,x._3.toInt)).distinct().map(x=>(1,1))
                .reduceByKey(_+_)
        lacciCount.map {
            case (nouse, count) => {
                s"$count"
            }
        }.saveAsTextFile(lacciCountOutput)


        val personCount=inputData.map(x=>(1,1)).reduceByKey(_+_)

        personCount.map {
            case (nouse, count) => {
                s"$count"
            }
        }.saveAsTextFile(personCountOutput)
    }
}
