package com.analyze

import com.util.ToolClass
import org.apache.hadoop.hdfs.server.common.Storage
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mercury on 17-4-19
  * project:location_opendsg
  * Package Name:com.com.analyze
  * Description:获取用户在连续每个基站停留的时间：如用户依次去过地点 A->B->A,将会获得两个在A地点的停留时间
  */
object UserStayTime {
    def main(args: Array[String]): Unit = {
        /**
          * inputFilePath:输入文件路径，FilterConnectDays输出
          * locationStayTimeOutput：用户在连续位置的停留时间
          * locationTatalStayTimeOutput：用户在每个位置的全部停留时间
          */
        val Array(inputFilePath, locationStayTimeOutput, locationTatalStayTimeOutput) = args
        val conf = new SparkConf().setAppName("location_opendsg_UserStayTime")
        //                .setMaster("local")
        val sc = new SparkContext(conf)

        //每个用户在连续每个地点停留时间
        val locationStayTime = sc.textFile(inputFilePath)
                .map(x => x.split("\t"))
                .map(x => ((x(1).toLong,ToolClass.bjToDay(ToolClass.utcToBeiJing(x(0)))), (x(2).toInt, x(3).toInt, x(0).toLong))) //phoneNum, lac, ci, time
                .groupByKey()
                .flatMapValues(x => {
                    val locationStayTime = ToolClass.getStayTime(x)
                    locationStayTime
                })
                .persist(StorageLevel.DISK_ONLY)
        //以上每个用户每天的在每个基站停留时间,


//        //每个用户在每个地点的停留总时间
//        val locationTatalStayTime = locationStayTime.map(x => ((x._1._1, x._2._1, x._2._2), x._2._4))
//                .reduceByKey(_ + _)


        //保存数据
        locationStayTime.repartition(30).map {
            case ((phoneNum,day), (lac, ci, arrive_time, stay_time)) => {
                s"$phoneNum\t$lac\t$ci\t$arrive_time\t$stay_time\t$day"
            }
        }.saveAsTextFile(locationStayTimeOutput)

//        locationTatalStayTime.map {
//            case ((phoneNum, lac, ci), stay_time) => {
//                s"$phoneNum\t$lac\t$ci\t$stay_time"
//            }
//        }.saveAsTextFile(locationTatalStayTimeOutput)


    }
}
