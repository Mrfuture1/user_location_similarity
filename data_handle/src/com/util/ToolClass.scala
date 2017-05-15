package com.util

import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date}
import javax.swing.tree.TreeNode

import breeze.numerics.{acos, cos, sin}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.immutable.HashMap.HashMap1
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.control.Breaks._

/**
  * Created by mercury on 17-4-17.
  */
object ToolClass {

    /**
      * function:构造连续时间段正则形式的文件路径
      * !!!如果是非连续时间段，因为按连续时间构造正则，所以可能会出现有些正则匹配不到实际文件，在读取数据的时候就会报错
      * input parameter:[filePath:正则形式的文件路径, sc]
      * return type:String
      * output:正则文件路径
      *
      * 正则路径example：
      * 2016.05.05-2016.06.06的数据：2016050[5-9],201605[1-2][0-9],2016053[0-1],2016060[1-6]
      * 2016.05.05-2016.07.07的数据：2016050[5-9],201605[1-2][0-9],2016053[0-1],201606[0-9]{2},2016070[1-7]
      * spark 正则读取文件：http://stackoverflow.com/questions/31782763/how-to-use-regex-to-include-exclude-some-input-files-in-sc-textfile
      * 类似正则表达式,正则讲解参考：http://deerchao.net/tutorials/regex/regex.htm
      *
      * 相比正则的不同：
      * 1. \d不能识别，需要[0-9]
      * 2. {4}不能识别，而{2},* 可以
      * 3. 2016050[5-5]只会对一次文件，而20160505,20160505会读取两次文件
      */
    def readDateFilesRegular(rootFilePath: String, beginTime: String, endTime: String): String = {
        val beginYear = beginTime.substring(0, 4).toInt
        val beginMonth = beginTime.substring(4, 6).toInt
        val beginDay = beginTime.substring(6, 8).toInt
        var beginDayTen = beginTime.substring(6, 7).toInt
        var beginDayGe = beginTime.substring(7, 8).toInt

        val endYear = endTime.substring(0, 4).toInt
        val endMonth = endTime.substring(4, 6).toInt
        val endDay = endTime.substring(6, 8).toInt
        var endDayTen = endTime.substring(6, 7).toInt
        var endDayGe = endTime.substring(7, 8).toInt


        //构建路径
        val inputArray = ArrayBuffer[(String)]() //存储所有日期
        var dateRegular: String = null

        //三种情况：不是起始终止年、不是起始终止年月、是起始终止年月
        //说明：breakable{}在for循环体外，则类似Java break；在循环体内，则类似Java continue
        for (year <- beginYear to endYear) {
            breakable {
                if (year != beginYear && year != endYear) {
                    //不是起始终止年
                    inputArray.+=(year.toString + "[0-9]*") //"[0-9]4" 不能匹配，奇怪
                    break()
                }
                //是起始年或者终止年
                var beginMonthFor = 1
                var endMonthFor = 12
                if (year == beginYear && year == endYear) {
                    beginMonthFor = beginMonth
                    endMonthFor = endMonth
                } else if (year == beginYear) {
                    beginMonthFor = beginMonth
                    endMonthFor = 12
                } else if (year == endYear) {
                    beginMonthFor = 1
                    endMonthFor = endMonth
                }
                for (month <- beginMonthFor to endMonthFor) {
                    if (!((year == beginYear && month == beginMonth) || (year == endYear && month == endMonth))) {
                        //不是起始终止年月
                        inputArray.+=(year.toString + monthScale2(month) + "[0-9]{2}")
                    }
                    else if (((year == beginYear && month == beginMonth) && (year == endYear && month == endMonth))) {
                        //起止年月相同，即读取一个月内的数据
                        val arrayTmp = oneMonthRegular(year, month, beginDay, endDay)
                        inputArray.++=(arrayTmp)
                    }
                    else if (year == beginYear && month == beginMonth) {
                        //为起始年月
                        val arrayTmp = oneMonthRegular(year, month, beginDay)
                        inputArray.++=(arrayTmp)
                    } else if (year == endYear && month == endMonth) {
                        //为终止年月
                        val arrayTmp = oneMonthRegular(year, month, 1, endDay)
                        inputArray.++=(arrayTmp)
                    }
                }

            }

        }
        val inputString = inputArray.map(x => rootFilePath + "/" + x).mkString(",")
        inputString
    }

    /**
      * function:依据一个月内的起止天做正则构造
      * input parameter:[year:年份如2016, month：月份如01, beginDay：开始天数如01, endDay：结束天数如23]
      * return type:ArrayBuffer[(String)]
      * output:天数正则构造
      */
    def oneMonthRegular(year: Int, month: Int, beginDay: Int, endDay: Int = 0): ArrayBuffer[String] = {
        val inputArray = ArrayBuffer[(String)]() //存储所有日期
        //该月份的总天数
        val c = Calendar.getInstance()
        c.set(Calendar.YEAR, year)
        c.set(Calendar.MONTH, month - 1)
        val daysOfMonth = c.getActualMaximum(Calendar.DAY_OF_MONTH)

        val beginDayTen = beginDay / 10
        val beginDayGe = beginDay % 10

        var endDayNew = endDay
        //如果endDay没有指定，则为该月的天数值
        if (endDayNew == 0) {
            endDayNew = daysOfMonth
        }
        val endDayTen = endDayNew / 10
        val endDayGe = endDayNew % 10

        var beginDayTenTmp = beginDayTen
        while (beginDayTenTmp != endDayTen) {
            if (beginDayTenTmp == beginDayTen) {
                inputArray.+=(year.toString + monthScale2(month) + beginDayTenTmp.toString + "[" + beginDayGe.toString + "-" + "9" + "]")
            } else {
                inputArray.+=(year.toString + monthScale2(month) + beginDayTenTmp.toString + "[0-9]")
            }
            beginDayTenTmp = beginDayTenTmp + 1
        }
        inputArray.+=(year.toString + monthScale2(month) + endDayTen.toString + "[" + "0" + "-" + endDayGe.toString + "]")
        inputArray
    }

    /**
      * function:将月份转变为String,如5：Int=>05：String、11:Int=>11:String
      * input parameter:[month]
      * return type:java.lang.String
      * output:
      */
    def monthScale2(month: Int): String = {

        var month2: String = null
        if (month < 10) {
            month2 = "0" + month.toString
        } else {
            month2 = month.toString
        }
        month2
    }

    //    /**
    //      × ！！！这种读取数据的方式，比较低效，不如readDateFilesRegular方法正则匹配读取快
    //      * function:依据上一级目录文件名及时间，读取数据
    //      * input parameter:[rootFilePath:上级目录, beginTime：开始时间如20160505, endTime：结束时间如20160606, sc]
    //      * return type:org.apache.spark.rdd.RDD<java.lang.String>
    //      * output:所有文件数据
    //      */
    //    def readDateFilesFor(rootFilePath: String, beginTime: String, endTime: String, sc: SparkContext): RDD[String] = {
    //        val filePathArray = buildInputPath(beginTime, endTime)
    //        var rddArray = new ArrayBuffer[RDD[String]]
    //        for (filePath <- filePathArray) {
    //            val input = sc.textFile(rootFilePath + "/" + filePath)
    //            rddArray.+=(input)
    //        }
    //        sc.union(rddArray)
    //    }
    //
    //    /**
    //      * function:依据开始时间和结束时间，生成天数Array
    //      * parameter:[beginTime, endTime]
    //      * beginTime:开始时间，如20160101
    //      * endTime:结束时间，如20160303
    //      */
    //    def buildInputPath(beginTime: String, endTime: String): Array[String] = {
    //        val beginYear = beginTime.substring(0, 4).toInt
    //        val beginMonth = beginTime.substring(4, 6).toInt
    //        val beginDay = beginTime.substring(6, 8).toInt
    //        var beginDayTmp = beginDay //for循环
    //
    //        val endYear = endTime.substring(0, 4).toInt
    //        val endMonth = endTime.substring(4, 6).toInt
    //        val endDay = endTime.substring(6, 8).toInt
    //        var endDayTmp = endDay //for循环
    //
    //        val inputArray = ArrayBuffer[(String)]() //存储所有日期
    //
    //        for (year <- beginYear to endYear) {
    //            for (month <- beginMonth to endMonth) {
    //                val c = Calendar.getInstance()
    //                c.set(Calendar.YEAR, year)
    //                c.set(Calendar.MONTH, month)
    //                val daysOfMonth = c.getActualMaximum(Calendar.DAY_OF_MONTH)
    //                //三种情况：开始年月、中间年月、终止年月
    //                if (year == beginYear && month == beginMonth) {
    //                    beginDayTmp = beginDay
    //                    endDayTmp = daysOfMonth
    //                } else if (year == endYear && month == endMonth) {
    //                    beginDayTmp = 1
    //                    endDayTmp = endDay
    //                } else {
    //                    beginDayTmp = 1
    //                    endDayTmp = daysOfMonth
    //                }
    //                for (day <- beginDayTmp to endDayTmp) {
    //                    inputArray.+=(year.toString + monthScale2(month) + day.toString)
    //                }
    //            }
    //        }
    //        inputArray.toArray
    //    }

    /**
      * !!!!注意只能读取本地文件，不能读取HDFS文件
      * function:构建lac ci =》经纬度 的hashmap,
      * input parameter:[filePath：文件路径, lacIndex：lac索引, CiIndex：ci索引, lngIndex：精度索引,latIndex:维度索引,scale:经纬度精度,小数点后几位]
      * return type:java.util.HashMap<scala.Tuple2<java.lang.Object,java.lang.Object>,scala.Tuple2<java.lang.Object,java.lang.Object>>
      * output:Hashmap[(lac,ci),(lng,lat)]
      */
    def lacCiToLngLatHDFS(filePath: String, lacIndex: Int, CiIndex: Int, lngIndex: Int, LatIndex: Int, scale: Int, sc: SparkContext, regex: String): mutable.HashMap[(Long, Long), (Double, Double)] = {
        val inputRdd = sc.textFile(filePath)
                .map(x => x.split(regex))
                .filter(x => safeStringToLong(x(lacIndex)).nonEmpty && safeStringToLong(x(CiIndex)).nonEmpty && safeStringToDouble(x(lngIndex)).nonEmpty && safeStringToDouble(x(LatIndex)).nonEmpty)
                .map(x => ((x(lacIndex).toLong, x(CiIndex).toLong), (strToDouble(x(lngIndex), scale), strToDouble(x(LatIndex), scale))))
        val mapTable = inputRdd.collectAsMap()
        val newHashMap = mapTable.asInstanceOf[mutable.HashMap[(Long, Long), (Double, Double)]]
        newHashMap
    }

    /**
      * !!!!注意只能读取本地文件，不能读取HDFS文件
      * function:构建lac ci =》经纬度 的hashmap,
      * input parameter:[filePath：文件路径, lacIndex：lac索引, CiIndex：ci索引, lngIndex：精度索引,latIndex:维度索引,scale:经纬度精度,小数点后几位]
      * return type:java.util.HashMap<scala.Tuple2<java.lang.Object,java.lang.Object>,scala.Tuple2<java.lang.Object,java.lang.Object>>
      * output:Hashmap[(lac,ci),(lng,lat)]
      */
    def lacCiToLngLatLocal(filePath: String, lacIndex: Int, CiIndex: Int, lngIndex: Int, LatIndex: Int, scale: Int): mutable.HashMap[(Long, Long), (Double, Double)] = {
        var mapTable = scala.collection.mutable.HashMap.empty[(Long, Long), (Double, Double)]
        for (line <- Source.fromFile(filePath).getLines()) {
            //只能读取本地文件
            val x = line.split("\t")
            if (x(lngIndex).isEmpty() || x(LatIndex).isEmpty()) {
            } else {
                mapTable += ((x(lacIndex).toLong, x(CiIndex).toLong) -> (strToDouble(x(lngIndex), scale), strToDouble(x(LatIndex), scale)))
            }
        }
        mapTable
    }

    //计算距离
    def distance(home: (Double, Double), shop: (Double, Double)): Double = {
        //C = sin(LatA)*sin(LatB) + cos(LatA)*cos(LatB)*cos(MLonA-MLonB)
        //Distance = R*Arccos(C)*Pi/180          //R=6371.004千米
        val homelat = home._2
        val homelng = home._1
        val shoplat = shop._2
        val shoplng = shop._1
        val xi = 0.017453292 //Pi/180
        val R = 6371004 //m
        var angleLong = sin(homelat * xi) * sin(shoplat * xi) + cos(homelat * xi) * cos(shoplat * xi) * cos(homelng * xi - shoplng * xi)
        if (angleLong > 1) {
            angleLong = 1
        }
        val myDistance = R * acos(angleLong)
        val bigDecimal: BigDecimal = BigDecimal(myDistance)
        bigDecimal.setScale(1, BigDecimal.RoundingMode.HALF_UP).doubleValue()
    }


    //string to long
    def safeStringToLong(str: String): Option[Long] = try {
        Some(str.toLong)
    } catch {
        case e: NumberFormatException => None
    }

    //string to double
    def safeStringToDouble(str: String) = try {
        Some(str.toDouble)
    } catch {
        case e: NumberFormatException => None
    }

    //经纬度保留n位小数
    def strToDouble(str: String, n: Int): Double = {
        val bigDecimal: BigDecimal = BigDecimal(str.toDouble);
        bigDecimal.setScale(n, BigDecimal.RoundingMode.HALF_UP).doubleValue()
    }


    //utc to beijing
    //input:utcTime 为毫秒  实验室话单时间戳为秒
    //df.format(new Date(utcTimeStamp))  utcTimeStamp为秒单位
    def utcToBeiJing(utcTime: String): String = {
        val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val utcTimeStamp = (utcTime.toLong * 1000).toLong
        df.format(new Date(utcTimeStamp))
    }


    //  beijing to utc
    //  output： 单位毫秒
    //  Date.getTime  返回值以秒为单位
    def bjToUtc(bjTime: String) = try {
        val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val sdate: Date = df.parse(bjTime);
        Some(sdate.getTime() / 1000)
    } catch {
        case e: NumberFormatException => None
    }

    /**
      * function:由北京时间"yyyy-MM-dd HH:mm:ss",获得天yyyyMMdd
      * input parameter:[bjTime]
      * return type:int
      * output:天yyyyMMdd
      */
    def bjToDay(bjTime: String): Int = {
        (bjTime.substring(0, 4) + bjTime.substring(5, 7) + bjTime.substring(8, 10)).toInt
    }

    /**
      * function:
      * input parameter:[lac_ci_utctime:Iterable[(Int:lac, Int:ci, Long:utctime)]]
      * return type:scala.Tuple2<scala.Tuple3<java.lang.Object,java.lang.Object,java.lang.Object>,java.lang.Object>[]
      * output:获取用户在连续每个地点停留的时间：如用户依次去过地点 A->B->A. 将会获得两个在A地点的停留时间
      * 停留时间计算方式：在某地的连续记录中：最后一条记录时间-第一条记录的时间
      */
    def getStayTime(lac_ci_timestamp: Iterable[(Int, Int, Long)]): Array[(Int, Int, Long, Long)] = {
        val sortedArray = lac_ci_timestamp.toArray.sortBy(x => x._3)
        var finalArray = ArrayBuffer.empty[((Int, Int, Long), Long)]
        finalArray.+=((sortedArray(0), sortedArray(0)._3))
        var j = 0
        val sortedSize = sortedArray.size
        if (sortedSize == 1) {

        } else {
            for (i <- 1 to (sortedSize - 1)) {
                val sortedEle = sortedArray(i)
                //判断基站是否相同
                if (sortedEle._1 == finalArray(j)._1._1 && sortedEle._2 == finalArray(j)._1._2) {
                } else {
                    //再次判断避免小区震荡的情况（中间突然多出一条其他基站）
                    if (i != (sortedSize - 1) && sortedArray(i + 1)._1 == finalArray(j)._1._1 && sortedArray(i + 1)._2 == finalArray(j)._1._2) {
                    } else {
                        //!=时，即转到了新的基站，做两个操作：更新上一个基站的离开时间、向finalArray加入下一个基站
                        val finalLastEle = finalArray.last
                        finalArray.trimEnd(1)
                        finalArray.+=((finalLastEle._1, sortedArray(i - 1)._3))
                        finalArray.+=((sortedArray(i), sortedArray(i)._3))
                        j = j + 1
                    }
                }
            }
            //处理用户最后一个基站
            val finalLastEle = finalArray.last
            finalArray.trimEnd(1)
            finalArray.+=((finalLastEle._1, sortedArray.last._3))
        }
        //再次处理小区震荡
        //通过停留时间过滤掉震荡
        val tmpArray = finalArray.map(x => (x._1._1, x._1._2, x._1._3, x._2 - x._1._3)).filter(x => x._4 >= 8)
        //将过滤掉震荡之后的统计再次聚合
        var realFinalArray = ArrayBuffer.empty[(Int, Int, Long, Long)]

        if (tmpArray.size > 0) {
            realFinalArray.+=(tmpArray(0))
            j = 0

            val tmpArraySize = tmpArray.size
            for (i <- 1 to tmpArraySize - 1) {
                val tmpEle = tmpArray(i)
                //判断基站是否相同
                if (tmpEle._1 == realFinalArray(j)._1 && tmpEle._2 == realFinalArray(j)._2) {
                    val finalLastEle = realFinalArray.last
                    realFinalArray(j) = (finalLastEle._1, finalLastEle._2, finalLastEle._3, tmpEle._3 - finalLastEle._3 + tmpEle._4)
                } else {
                    realFinalArray.+=(tmpEle)
                    j = j + 1
                }
            }
        }

        realFinalArray.toArray
    }

    //再次聚合方法2，貌似有问题
    //        if (tmpArray.size >= 2) {
    //            var ifCircular = true
    //            var i = 1
    //            while (ifCircular) {
    //                val tmpArrayi2 = tmpArray(i)
    //                val tmpArrayi1 = tmpArray(i - 1)
    //                if (tmpArrayi2._1 == tmpArrayi1._1 && tmpArrayi2._2 == tmpArrayi1._2) {
    //                    tmpArray(i - 1) = (tmpArrayi1._1, tmpArrayi1._2, tmpArrayi1._3, tmpArrayi2._4 + (tmpArrayi2._3 - tmpArrayi1._3))
    //                    tmpArray.remove(i)
    //                } else {
    //                    i = i + 1
    //                }
    //
    //                if (i < tmpArray.size) {
    //                    ifCircular=true
    //                }else{
    //                    ifCircular=false
    //                }
    //            }
    //        }
    //        tmpArray.toArray
    //}

    //    def userLocationSim(locListA:List[Double,Double,Double],locListB:List[Double,Double,Double],ifTime:Boolean): Unit ={
    //      TreeNode
    //    }


}
