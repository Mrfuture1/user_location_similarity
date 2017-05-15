package com.report

import com.util.ToolClass
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mercury on 17-4-25
  * project:location_opendsg
  * Package Name:com.report
  * Description:
  */
object DiDiLocation {
    def main(args: Array[String]): Unit = {
        val Array(inputFilePath, lng_lat_dis_output, avg_dis_output) = args
        val conf = new SparkConf().setAppName("DiDiLocation")
                .setMaster("local")
        val sc = new SparkContext(conf)


        //        bubble_type = 1 & lat = 44.596421 & tlat = 44.513722686921 & flat = 44.596476713286 & lng = 129.607006 & tlng = 129.57218124115 & flng = 129.60686566717

        val didiPattern =""".*bubble_type.*lat=(\d+)\.(\d+).*tlat=(\d+)\.(\d+).*flat=(\d+)\.(\d+).*lng=(\d+)\.(\d+).*tlng=(\d+)\.(\d+).*flng=(\d+)\.(\d+).*""".r

        //        //数字和字母的组合正则表达式
        //        val numitemPattern="""([0-9]+) ([a-z]+)""".r
        //        val line="93459 spark"
        //        line match{
        //            case numitemPattern(num,blog)=> println(num+"\t"+blog)
        //            case _=>println("hahaha...")
        //        }
        //输入数据
        val lng_lat_dis = sc.textFile(inputFilePath)
                .map(x => x.split(","))
                .map(x => {
                    x(1) match {
                        case didiPattern(lat_1,lat_2, tlat_1,tlat_2 ,flat_1,flat_2, lng_1,lng_2, tlng_1, tlng_2,flng_1,flng_2) =>
                            ((flng_1.toString+"."+flng_2.toString).toDouble,(flat_1.toString+"."+flat_2.toString).toDouble,(tlng_1.toString+"."+tlng_2.toString).toDouble,
                                    (tlat_1.toString+"."+tlat_2.toString).toDouble,(lng_1.toString+"."+lng_2.toString).toDouble,(lat_1.toString+"."+lat_2.toString).toDouble,x(0).toDouble.toLong)
                        case _ => (0.0, 0.0,0.0,0.0,0.0,0.0,0)
                    }
                })
                .filter(x => x._6 != 0)
                .map(x => (x, ToolClass.distance((x._3, x._4), (x._5, x._6))))

        val avgDis = lng_lat_dis.map(x => x._2).sum() / lng_lat_dis.count()


        // 每次乘车距离保存
        lng_lat_dis.map {
            case ((flng,flat,tlng,tlat,lng,lat,time),dis) => {
                s"$flng\t$flat\t$tlng\t$tlat\t$time\t$dis\t$lng\t$lat"
            }
        }.saveAsTextFile(lng_lat_dis_output)

        sc.parallelize(Array(avgDis)).map {
            case (avgDis) => {
                s"$avgDis"
            }
        }.saveAsTextFile(avg_dis_output)


    }
}
