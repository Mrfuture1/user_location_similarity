package com.test.staytime

import com.util.ToolClass
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mercury on 17-5-11
  * project:location_opendsg
  * Package Name:com.test.staytime
  * Description:
  */
object ExtractData {
    def main(args: Array[String]): Unit = {
        val Array(userStaytime, pathOutput) = args

        val conf = new SparkConf().setAppName("TreeUserSimilar")
        //                .setMaster("local")
        val sc = new SparkContext(conf)
        sc.textFile(userStaytime).map(x => x.split("\t"))
                .filter(x => ToolClass.bjToDay(ToolClass.utcToBeiJing(x(0))) == 20160619)
                .repartition(1)
                .sortBy(x => x(0).toLong)
                .map(x => x.mkString("\t"))
                .saveAsTextFile(pathOutput)
    }
}
