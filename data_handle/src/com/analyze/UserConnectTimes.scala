package com.analyze

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mercury on 17-4-19
  * project:location_opendsg
  * Package Name:com.com.analyze
  * Description:用户连接基站总次数
  */
object UserConnectTimes {
    def main(args: Array[String]): Unit = {
        /**
          * inputFilePath:输入路径，FilterConnectDays输出
          * personCountOutput：输出路径
          */
        val Array(inputFilePath, personCountOutput) = args
        val conf = new SparkConf().setAppName("location_opendsg_UserConnectTimes")
        //                .setMaster("local")
        val sc = new SparkContext(conf)

        //输入数据
        val inputData = sc.textFile(inputFilePath)
                .map(x => x.split("\t"))

        //获取用户基站总连接数
        val connectTatalTimes = inputData
                .map(x => (x(1).toLong, 1))
                .reduceByKey(_ + _)
                .sortBy(x => x._2, false)

        connectTatalTimes.map {
            case (phoneNum, connectTimes) => {
                s"$phoneNum\t$connectTimes"
            }
        }.saveAsTextFile(personCountOutput)
    }

}
