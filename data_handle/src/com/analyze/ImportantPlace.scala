package com.analyze

import com.util.ToolClass
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mercury on 17-4-19
  * project:location_opendsg
  * Package Name:com.analyze
  * Description:获得用户在指定时间段，停留时间最长的基站
  */
object ImportantPlace {
    def main(args: Array[String]): Unit = {
        /**
          * inputFilePath:UserStayTime输出locationStayTime：(phoneNum, (lac, ci, arrive_time, stay_time))
          * beginHour:开始时间
          * endHour:结束时间,若为第二天时间则24+,如第二天上午8点，则为32
          * lacCiFilePath:lac ci=> lng lat 文件
          * personImportPlaceOutput:用户在该时间段停留时间最长的地点
          */
        val Array(inputFilePath, beginHour, endHour, lacCiFilePath, personImportPlaceOutput) = args
        val days = 28
        val conf = new SparkConf().setAppName("location_opendsg_ImportantPlace")
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
        val lacCi_lnglat_hashmap = ToolClass.lacCiToLngLatHDFS(lacCiFilePath, 0, 1, 7, 8, 6, sc, "\t")
        //计算用户在每个停留基站的停留时间,取停留时间最长的基站
        val importantUserPlace = durationRdd
                .map(x => ((x._1, x._2, x._3), x._5)) //以（Phone，Lac，Ci)为key做聚合
                .reduceByKey(_ + _) //对StationStayTime做reduce操作
                .map(x => (x._1._1, (x._1._2, x._1._3, x._2)))
                .groupByKey() //以Phone为key，做groupByKey操作，得到的(Phone,Array((Lac,Ci,StationStayTime)))
                .mapValues(x => {
                    //将Array((Lac,Ci,StationStayTime))按StationStayTime升序排列，取最后一个元素
                    x.toArray.sortBy(x => x._3).last
                })
                .map(x => (x._1, x._2, lacCi_lnglat_hashmap.get((x._2._1, x._2._2)).get, x._2._3 / days))

        importantUserPlace.map {
            case (phoneNum, (lac, ci, stay_time_most), (lng, lat), day_stay_time) => {
                s"$phoneNum\t$lac\t$ci\t$lng\t$lat\t$stay_time_most\t$day_stay_time"
            }
        }.saveAsTextFile(personImportPlaceOutput)


    }
}
