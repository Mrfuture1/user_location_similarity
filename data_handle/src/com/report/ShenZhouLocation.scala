package com.report

import com.util.ToolClass
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mercury on 17-4-25
  * project:location_opendsg
  * Package Name:com.report
  * Description:
  */
object ShenZhouLocation {
    def main(args: Array[String]): Unit = {
        val Array(inputFilePath, lng_lat_dis_output, avg_dis_output) = args
        val conf = new SparkConf().setAppName("DiDiLocation")
                .setMaster("local")
        val sc = new SparkContext(conf)


        //        origin=126.703408%2C45.763044&destination=126.707044%2C45.749460
        val shenZhouPattern1 =
        """.*origin=(\d+)\.(\d+)%2C(\d+)\.(\d+)[&]destination=(\d+)\.(\d+)%2C(\d+)\.(\d+).*""".r

        //&destination=126.607681%2C45.749237&originId=&strategy=5&origin=126.683411%2C45.791817

        val shenZhouPattern2=
            """.*[&]destination=(\d+)\.(\d+)%2C(\d+)\.(\d+)[&]origin=(\d+)\.(\d+)%2C(\d+)\.(\d+).*""".r
        //输入数据
        val lng_lat_dis = sc.textFile(inputFilePath)
                .map(x => x.split(","))
                .map(x => {
                    x(1) match {
                        case shenZhouPattern1(flng_1, flng_2, flat_1, flat_2, tlng_1, tlng_2, tlat_1, tlat_2) =>
                            ((flng_1.toString + "." + flng_2.toString).toDouble, (flat_1.toString + "." + flat_2.toString).toDouble,
                                    (tlng_1.toString + "." + tlng_2.toString).toDouble, (tlat_1.toString + "." + tlat_2.toString).toDouble,x(0).toDouble.toLong)
                        case shenZhouPattern2( tlng_1, tlng_2, tlat_1, tlat_2,flng_1, flng_2, flat_1, flat_2)=>
                            ((flng_1.toString + "." + flng_2.toString).toDouble, (flat_1.toString + "." + flat_2.toString).toDouble,
                                    (tlng_1.toString + "." + tlng_2.toString).toDouble, (tlat_1.toString + "." + tlat_2.toString).toDouble,x(0).toDouble.toLong)
                        case _ => (0.0, 0.0, 0.0, 0.0,0)
                    }
                })
                .filter(x => x._4 != 0)
                .map(x => (x, ToolClass.distance((x._3, x._4), (x._1, x._2))))

        val avgDis = lng_lat_dis.map(x => x._2).sum() / lng_lat_dis.count()


        // 每次乘车距离保存
        lng_lat_dis.map {
            case ((flng, flat, tlng, tlat,time), dis) => {
                s"$flng\t$flat\t$tlng\t$tlat\t$time\t$dis"
            }
        }.saveAsTextFile(lng_lat_dis_output)

        sc.parallelize(Array(avgDis)).map {
            case (avgDis) => {
                s"$avgDis"
            }
        }.saveAsTextFile(avg_dis_output)


    }
}
