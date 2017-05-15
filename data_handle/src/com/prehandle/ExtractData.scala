package com.prehandle

import com.util.ToolClass
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mercury on 17-4-18
  *
  * project:location_opendsg
  * Package Name:com.prehandle
  * Description:
  */
class ExtractData {

}

object ExtractData {
    def main(args: Array[String]): Unit = {
        val Array(filePathRoot, beginTime, endTime, lacCiFilePath, filePathOutput, dataOutput) = args
        val conf = new SparkConf().setAppName("location_opendsg_extractdata")
        //                .setMaster("local")
        val sc = new SparkContext(conf)
        val lacCi_lnglat_hashmap = ToolClass.lacCiToLngLatHDFS(lacCiFilePath, 0, 1, 7, 8, 6, sc, "\t")
        //构造正则匹配路径
        val filePathRegular = ToolClass.readDateFilesRegular(filePathRoot, beginTime, endTime)
        val filePathRegularArray = Array(filePathRegular)
        sc.parallelize(filePathRegularArray).saveAsTextFile(filePathOutput)


        sc.textFile(filePathRegular)
                .map(x => x.split("\t"))
                .filter(x => x.length > 15 && ToolClass.safeStringToDouble(x(0)).nonEmpty && ToolClass.safeStringToLong(x(2)).nonEmpty && ToolClass.safeStringToLong(x(14)).nonEmpty && ToolClass.safeStringToLong(x(15)).nonEmpty && lacCi_lnglat_hashmap.get(x(14).toLong, x(15).toLong).nonEmpty)
                .map(x => (x(0).toDouble.toLong, x(2).toLong, x(14).toLong, x(15).toLong, lacCi_lnglat_hashmap.get(x(14).toLong, x(15).toLong).get))
                //x(0).toDouble会成为科学计数法形式...
                .filter(x => x._5._1 > 126.4 && x._5._1 < 126.9 && x._5._2 > 45.6 && x._5._2 < 45.9)
                .map {
                    case (time, phoneNum, lac, ci, (lng, lat)) => {
                        s"$time\t$phoneNum\t$lac\t$ci\t$lng\t$lat"
                    }
                }.saveAsTextFile(dataOutput)
    }

}
