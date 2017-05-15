package com.prehandle

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mercury on 17-4-20
  * project:location_opendsg
  * Package Name:com.prehandle
  * Description:
  */
object SampleData {
    def main(args: Array[String]): Unit = {
        val Array(inputFilePath,outputFilepath) = args
        val conf = new SparkConf().setAppName("location_opendsg_SampleData")
        //                .setMaster("local")
        val sc = new SparkContext(conf)

        //输入数据
        val inputData = sc.textFile(inputFilePath)
                      .take(10)

        sc.parallelize(inputData).saveAsTextFile(outputFilepath)

    }
}
