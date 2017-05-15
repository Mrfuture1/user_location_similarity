package com.analyze

import com.util.ToolClass
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mercury on 17-4-25
  * project:location_opendsg
  * Package Name:com.analyze
  * Description:
  */
object HomeAndWorkDistance {


    def main(args: Array[String]): Unit = {
        val Array(homeInputFilePath, workInputFilePath, personDisOutput, avgOutput) = args
        val conf = new SparkConf().setAppName("location_opendsg_FrequencyMost")
                        .setMaster("local")
        val sc = new SparkContext(conf)

        //输入数据
        val home = sc.textFile(homeInputFilePath)
                .map(x => x.split("\t"))
                .map(x => (x(0), (x(3).toDouble, x(4).toDouble)))
        val work = sc.textFile(workInputFilePath)
                .map(x => x.split("\t"))
                .map(x => (x(0), (x(3).toDouble, x(4).toDouble)))

        val personDis = home.join(work).mapValues(x => ToolClass.distance(x._1, x._2))
        personDis.map {
            case (phoneNum,dis) => {
                s"$phoneNum\t$dis"
            }
        }.saveAsTextFile(personDisOutput)

        val disSum=personDis.map(x=>x._2).sum()
        val personCount=personDis.count()
        val avg=disSum/personCount
        val avgArray=Array(avg)
        sc.parallelize(avgArray).map {
            case (avgDis) => {
                s"$avgDis"
            }
        }.saveAsTextFile(avgOutput)


    }

}
