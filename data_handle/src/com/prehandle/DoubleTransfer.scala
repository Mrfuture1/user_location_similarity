package com.prehandle

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mercury on 17-4-20
  * project:location_opendsg
  * Package Name:com.prehandle
  * Description:将科学计数法的数据转变为Long
  */
object DoubleTransfer {
    def main(args: Array[String]): Unit = {
        /**
          * inputFilePath:输入文件路径，ExtractData输出
          * ouputFilepath：输出文件路径
          */
        val Array(inputFilePath, outputFilepath) = args
        val conf = new SparkConf().setAppName("location_opendsg_DoubleTransfer")
        //                .setMaster("local")
        val sc = new SparkContext(conf)

        //输入数据
        val inputData = sc.textFile(inputFilePath)
                .map(x => x.split("\t"))
                .map(x => {
                    val a = new java.math.BigDecimal(x(0))
                    x(0) = a.longValue().toString
                    x.mkString("\t")
                })
                .repartition(8000)
                .saveAsTextFile(outputFilepath)
    }
}
