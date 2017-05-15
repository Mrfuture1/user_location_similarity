package com.util

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mercury on 17-4-22
  * project:location_opendsg
  * Package Name:com.util
  * Description:
  */
object Repart {


    def main(args: Array[String]): Unit = {
        /**
          * inputFilePath:输入文件路径，ExtractData输出
          * ouputFilepath：输出文件路径
          */
        val Array(inputFilePath, partnum, outputFilepath) = args
        val conf = new SparkConf().setAppName("repart")
        //                .setMaster("local")
        val sc = new SparkContext(conf)

        //输入数据
        sc.textFile(inputFilePath)
                .repartition(partnum.toInt)
                .saveAsTextFile(outputFilepath)


    }

}
