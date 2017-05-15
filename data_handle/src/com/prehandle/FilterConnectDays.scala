package com.prehandle

import com.util.ToolClass
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by mercury on 17-4-19
  * project:location_opendsg
  * Package Name:com.prehandle
  * Description:过滤用户，保留28天内均有连接基站记录的用户
  */
object FilterConnectDays {
    def main(args: Array[String]): Unit = {
        /**
          * inputFilePath:输入文件路径，ExtractData输出
          * connectDays:连接天数
          * ouputFilepath：输出文件路径
          */
        val Array(inputFilePath, connectDays, ouputFilepath) = args
        val conf = new SparkConf().setAppName("location_opendsg_FilterConnectDays")
//                .setMaster("local")
        val sc = new SparkContext(conf)

        //输入数据
        val inputData = sc.textFile(inputFilePath)
                .map(x => x.split("\t"))

        //用户连接基站的天数需要等于connectDays天
        val validUserArray = inputData
                .map(x => ((ToolClass.bjToDay(ToolClass.utcToBeiJing(x(0))), x(1).toLong)))
                .distinct()
                .map(x => (x._2, 1))
                .reduceByKey(_ + _)
                .filter(x => x._2 >= connectDays.toInt)
                .map(x => x._1)
                .collect()


        val validUserHashSet: mutable.HashSet[Long] = new mutable.HashSet()
        validUserHashSet.++=(validUserArray)

        //获取基站连接数前connectTimesCountTop的用户
        val connectTatalTimes = inputData
                .filter(x => validUserHashSet.contains(x(1).toLong))
                .map(x => (x(0), x(1), x(2), x(3), x(4), x(5)))
                .map {
                    case (time, phoneNum, lac, ci, lng, lat) => {
                        s"$time\t$phoneNum\t$lac\t$ci\t$lng\t$lat"
                    }
                }.saveAsTextFile(ouputFilepath)

    }
}
