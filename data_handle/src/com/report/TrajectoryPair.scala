package com.report

import com.util.ToolClass
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by mercury on 17-4-24
  * project:location_opendsg
  * Package Name:com.report
  * Description:
  */
object TrajectoryPair {
    def main(args: Array[String]): Unit = {
        /**
          * inputFilePath:UserStayTime输出locationStayTime：(phoneNum, (lac, ci, arrive_time, stay_time))
          * beginHour:开始时间
          * endHour:结束时间,若为第二天时间则24+,如第二天上午8点，则为32
          * lacCiFilePath:lac ci=> lng lat 文件
          * personImportPlaceOutput:用户在该时间段停留时间最长的地点
          */
        val Array(inputFilePath, beginHour, endHour, lacCiFilePath, topN, trajectoryPairTopOutput, trajectoryPairTopOnlyOutput) = args
        val conf = new SparkConf().setAppName("TrajectoryPair")
        //                .setMaster("local")
        val sc = new SparkContext(conf)

        //输入数据
        val inputData = sc.textFile(inputFilePath)
                .map(x => x.split("\t"))
                .map(x => (x(0).toLong, x(1).toInt, x(2).toInt, x(3).toLong, x(4).toDouble, ToolClass.utcToBeiJing(x(3)).substring(11, 13).toInt))


        var beginHourInt = beginHour.toInt
        var endHourInt = endHour.toInt
        var durationRdd: RDD[(Long, Int, Int, Long, Double, Int)] = sc.emptyRDD

        //过滤符合相应时间段
        if (endHourInt > 24) {
            val endHourNew = endHourInt - 24
            durationRdd = inputData.filter(x => (x._6 >= beginHourInt && x._6 <= 24) || (x._6 >= 0 && x._6 <= endHourNew))
        } else {
            durationRdd = inputData.filter(x => (x._6 >= beginHourInt && x._6 <= endHourInt))
        }

        val lacCi_lnglat_hashmap = ToolClass.lacCiToLngLatHDFS(lacCiFilePath, 0, 1, 7, 8, 2, sc, "\t")

        //统计用户轨迹对
        val trajectoryPair = durationRdd.map(x => (x._1, (lacCi_lnglat_hashmap.get(x._2.toLong, x._3.toLong).get, x._4)))
                .groupByKey()
                .flatMapValues(x => {
                    val stationArray = x.toArray.sortBy(x=>x._2)
                    var finalArray = ArrayBuffer.empty[((Double, Double), (Double, Double))]
                    var j = 0
                    for (i <- 0 to stationArray.length - 2) {
                        if (stationArray(i)._1._1 == stationArray(i + 1)._1._1 && stationArray(i)._1._2 == stationArray(i + 1)._1._2){
                        }else{
                            finalArray.+=((stationArray(i)._1,stationArray(i+1)._1))
                        }
                    }
                    finalArray
                })
                .map(x => (x._2, 1))
                .reduceByKey(_ + _)
                .sortBy(x => x._2, false)
                .zipWithIndex()
                .filter(x => x._2 < topN.toInt)
                .map(x => x._1)
                .repartition(1)
//        trajectoryPair.map {
//            case ((((lac1, ci1), (lac2, ci2)), count), (lng1, lat1), (lng2, lat2)) => {
//                s"$lac1\t$ci1\t$lac2\t$ci2\t$count\t$lng1\t$lat1\t$lng2\t$lat2"
//            }
//        }.saveAsTextFile(trajectoryPairTopOutput)

        trajectoryPair.map {
            case (((lng1, lat1), (lng2, lat2)), count) => {
                s"$lng1\t$lat1\t$lng2\t$lat2\t$count"
            }
        }.saveAsTextFile(trajectoryPairTopOnlyOutput)

    }
}
