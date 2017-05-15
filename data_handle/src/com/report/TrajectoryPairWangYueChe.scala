package com.report

import com.util.ToolClass
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mercury on 17-4-25
  * project:location_opendsg
  * Package Name:com.report
  * Description:
  */
object TrajectoryPairWangYueChe {
    def main(args: Array[String]): Unit = {
        val Array(inputFilePath, beginHour, endHour, topN, trajectoryPairTopOnlyOutput) = args
        val conf = new SparkConf().setAppName("TrajectoryPair")
                        .setMaster("local")
        val sc = new SparkContext(conf)

        val inputData=sc.textFile(inputFilePath)
                .map(x=>x.split("\t"))
                .map(x=>(ToolClass.strToDouble(x(0),2),ToolClass.strToDouble(x(1),2),ToolClass.strToDouble(x(2),2),ToolClass.strToDouble(x(3),2),ToolClass.utcToBeiJing(x(4)).substring(11, 13).toInt))

        var beginHourInt = beginHour.toInt
        var endHourInt = endHour.toInt
        var durationRdd: RDD[(Double, Double, Double, Double, Int)] = sc.emptyRDD

        //过滤符合相应时间段
        if (endHourInt > 24) {
            val endHourNew = endHourInt - 24
            durationRdd = inputData.filter(x => (x._5 >= beginHourInt && x._5 <= 24) || (x._5 >= 0 && x._5 <= endHourNew))
        } else {
            durationRdd = inputData.filter(x => (x._5 >= beginHourInt && x._5 <= endHourInt))
        }

        val pair=durationRdd.map(x=>((x._1,x._2,x._3,x._4),1))
                .reduceByKey(_+_)
                .sortBy(x=>x._2,false)
                .zipWithIndex()
                .filter(x=>x._2<20)
                .map(x=>(x._1._1,x._1._2))

        pair.map {
                    case ((lng1, lat1, lng2, lat2), count) => {
                        s"$lng1\t$lat1\t$lng2\t$lat2\t$count"
                    }
                }.saveAsTextFile(trajectoryPairTopOnlyOutput)
    }
}
