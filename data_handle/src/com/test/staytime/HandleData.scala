package com.test.staytime

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mercury on 17-5-15
  * project:location_opendsg
  * Package Name:com.test.staytime
  * Description:
  */
object HandleData {
    def main(args: Array[String]): Unit = {
        val Array(logInput,homePath, pathOutput) = args

        val conf = new SparkConf().setAppName("HandleData")
                .setMaster("local")
        val sc = new SparkContext(conf)
        val sourceData = sc.textFile(logInput)
                .map(x => x.split("\t"))
                .map(x => ((x(0).toLong, x(1).toDouble, x(2).toDouble, x(3).toLong, x(4).toDouble), x(5).toDouble))

        val homeData = sc.textFile(homePath)
                .map(x => x.split("\t"))
                .map(x => (x(0).toLong, (x(1).toLong, x(2).toLong)))
        .collectAsMap()
        val count=sourceData.countByKey()

        sourceData
                .filter(x=>count.get(x._1).get==1)
                        .map(x=>(homeData.get(x._1._1).get,x._1,x._2))
                .map {
                    case (((homelng,homelat), (phone, lng,lat,arrive_time,stay_time), score)) => {
                        s"$homelng\t$homelat\t$phone\t$lng\t$lat\t$arrive_time\t$stay_time\t$score"
                    }
                }.saveAsTextFile(pathOutput)


    }
}
