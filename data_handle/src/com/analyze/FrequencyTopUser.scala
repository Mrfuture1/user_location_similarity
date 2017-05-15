package com.analyze

import com.util.ToolClass
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


/**
  * Created by mercury on 17-4-19
  * project:location_opendsg
  * Package Name:com.analyze
  * Description:获取接入基站次数最多的用户话单记录
  *
  */
object FrequencyTopUser {
    def main(args: Array[String]): Unit = {
        /** inputFilePath:数据文件路径，FilterConnectDays输出
          * userConnectInput:用户连接基站的总次数，UserConnectTimes输出
          * connectTimesCountTopUser:top连接次数的用户数
          * dataOutput：前connectTimesCountTopUser个用户的话单记录
          */
        val Array(inputFilePath, userConnectInput, connectTimesCountTopUser, dataOutput) = args
        val conf = new SparkConf().setAppName("location_opendsg_FrequencyMost")
        //                .setMaster("local")
        val sc = new SparkContext(conf)

        //输入数据
        val inputData = sc.textFile(inputFilePath)
                .map(x => x.split("\t"))

        val userConnectTimes = sc.textFile(userConnectInput)
                .map(x => x.split("\t"))
                .map(x => (x(0).toLong, x(1).toLong))
                .sortBy(x => x._2, false)


        //连接数前connectTimesCountTopUser用户
        val topUserHashset = new mutable.HashSet[Long]()
        topUserHashset.++=(userConnectTimes.collectAsMap().take(connectTimesCountTopUser.toInt).keySet)

        //获取top用户的话单记录
        val validRecord = inputData.filter(x => topUserHashset.contains(x(1).toLong))


        //保存结果
        validRecord.map(x => (x(0), x(1), x(2), x(3), x(4), x(5)))
                .map {
                    case (time, phoneNum, lac, ci, lng, lat) => {
                        s"$time\t$phoneNum\t$lac\t$ci\t$lng\t$lat"
                    }
                }.saveAsTextFile(dataOutput)


    }

}
