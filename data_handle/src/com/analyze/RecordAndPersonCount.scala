package com.analyze

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mercury on 17-5-3
  * project:location_opendsg
  * Package Name:com.analyze
  * Description:（统计记录条数、总人数、基站数）、每天的人数
  */
object RecordAndPersonCount {
    /**
      * inputFilePath:UserStayTime输出locationStayTime：(phoneNum, (lac, ci, arrive_time, stay_time))
      * countOutput:记录的条数、人的数目、基站数目
      * dayPersonCount：每天的人数分布
      */
    def main(args: Array[String]): Unit = {
        val Array(inputFilePath, countOutput, dayPersonCountOutput) = args
        val conf = new SparkConf().setAppName("RecordAndPersonCount")
        //                .setMaster("local")
        val sc = new SparkContext(conf)

        //input
        val inputData = sc.textFile(inputFilePath)
                .map(x => x.split("\t"))
                .map(x => (x(0), x(1), x(2), x(5)))


        //record count
        val recordCount = inputData.count()
        var countArray = Array("recordCount" + recordCount)

        //personCount
        val personCount = inputData.map(x => x._1).distinct().count()
        countArray = countArray.+:("personCount" + personCount)

        //stationCount
        val stationCount = inputData.map(x => (x._2, x._3)).distinct().count()
        countArray = countArray.+:("stationCount" + stationCount)

        //every day person count
        val dayPersonCount = inputData.map(x => (x._4, 1)).reduceByKey(_ + _)

        //save
        sc.parallelize(countArray).repartition(1).saveAsTextFile(countOutput)
        dayPersonCount.repartition(1).sortBy(x=>x._1.toLong).map {
            case (day, personCount) => {
                s"$day\t$personCount"
            }
        }.saveAsTextFile(dayPersonCountOutput)


    }
}
