package com.algorithm

import com.util.BuildTree
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by mercury on 17-5-8
  * project:location_opendsg
  * Package Name:com.test
  * Description:
  * 控制变量：
  * 1. 家的精度
  * 2. 源数据的天数 date
  * 3. 相似轨迹的array长度topN的轨迹 topN
  */
object TreeUserSimilar {
    def main(args: Array[String]): Unit = {
        val Array(trajectoryPath, homePath, date, topN, scoreOutput, scoreTopOutput) = args
        val trajectoryLengthMax = 15
        val personCount = 3000
        val taskCount = 300
        val stayTimeMin = 100
        //        val perLocUserCount=30
        //        val topN = 1

        val conf = new SparkConf().setAppName("TreeUserSimilar")
        //                .setMaster("local")
        val sc = new SparkContext(conf)

        // 实际应用时,应该用家或者工作的位置做一些限定,以减少计算量
        // 读取home数据。   可以通过更改家的精度来适当减少计算量
        val homeRdd = sc.textFile(homePath).map(x => x.split("\t")).map(x => (x(0).toLong, (x(1).toDouble, x(2).toDouble)))

        // 轨迹数据
        val trajectoryRdd = sc.textFile(trajectoryPath).map(x => x.split("\t"))
                .filter(x => x(5).toLong == date.toInt && x(4).toDouble > stayTimeMin) //一天的轨迹进行比较
                .map(x => (x(0).toLong, ((x(1).toDouble, x(2).toDouble), x(3).toLong, x(4).toDouble)))

        val trajectoryArrayData = trajectoryRdd.groupByKey()
                .map(x => (x._1, x._2.toList))
                .filter(x => x._2.size < trajectoryLengthMax && x._2.size > 1) //仅比较位置数>1的用户


        //每个位置重要度 log(n/N)
        val userTotalNum = trajectoryRdd.map(x => x._1).distinct().count() //总用户数
        val locUsersNumMapTmp = trajectoryRdd.map(x => (x._1, x._2._1)).distinct().map(x => (x._2, 1)).countByKey()

        val locUsersNumMap = collection.mutable.HashMap(locUsersNumMapTmp.toSeq: _*)
        val locScoreTmp = locUsersNumMap.map(x => (x._1, math.log10(userTotalNum.toDouble / x._2)))
        val locScore = sc.broadcast(locScoreTmp)


        //用户轨迹和家的位置结合
        val tmpAllData = homeRdd.join(trajectoryArrayData)
                .sortBy(x => x._2._2.size, false)
                .take(personCount)
        val allData = sc.parallelize(tmpAllData)

        //保存全部实验数据
        allData.repartition(1).map(x => ((x._2._1, x._1), x._2._2)).flatMapValues(x => x)
                .map {
                    case (((jing, wei), phone), ((lng, lat), arrive_time, stay_time)) => {
                        s"$jing\t$wei\t$phone\t$lng\t$lat\t$arrive_time\t$stay_time"
                    }
                }.saveAsTextFile(scoreOutput + "_allData")

        //按地区,考虑用户相似性
        val userSimilarSocre = allData.map(x => ((x._2._1, (x._1, (x._2._2, x._2._2.map(y => locScore.value.get(y._1).get).sum)))))
                .groupByKey(taskCount)
                .flatMapValues(x => {
                    //改用for循环实现,不用flatmap
                    var scoreArray = ArrayBuffer.empty[(Long, Long, Double)]
                    val tmp = x.toArray
                    //                            .take(perLocUserCount)
                    val size = tmp.size
                    for (i <- 0 to size - 1) {
                        for (j <- i + 1 to size - 1) {
                            val y = tmp(i)
                            val z = tmp(j)
                            val buildTree = new BuildTree[Double](y._2._1, z._2._1)
                            val tree = buildTree.buildTreeFromLocation(1)
                            //查看树结构
                            val allBranchNodeIndex = tree.getAllBranchNodeIndex()
                            val score = buildTree.similarScore(topN.toInt, allBranchNodeIndex, tree, locScore.value, y._2._2, z._2._2)
                            scoreArray.+=((y._1, z._1, score))
                        }
                    }
                    scoreArray.filter(z => z._3 > 0)
                })
                .persist(StorageLevel.DISK_ONLY)
                .sortBy(x => (x._2._1, x._2._3), false, 50)

        userSimilarSocre.map {
            case ((jing, wei), (phoneA, phoneB, score)) => {
                s"$jing\t$wei\t$phoneA\t$phoneB\t$score"
            }
        }.saveAsTextFile(scoreOutput)

        userSimilarSocre.filter(x => x._2._3 > 0.7).map {
            case ((jing, wei), (phoneA, phoneB, score)) => {
                s"$jing\t$wei\t$phoneA\t$phoneB\t$score"
            }
        }.saveAsTextFile(scoreTopOutput)

        userSimilarSocre.filter(x => x._2._3 > 0.7)
                .flatMap(x => Array(x._2._1, x._2._2))
                .distinct()
                .map(x => (x, 1))
                .join(trajectoryRdd)
                .map(x => (x._1, x._2._2))
                .sortBy(x => (x._1, x._2._2))
                .map {
                    case (phone, ((lac, ci), arrive_time, stay_time)) => {
                        s"$phone\t$lac\t$ci\t$arrive_time\t$stay_time"
                    }
                }.saveAsTextFile(scoreTopOutput+"_data")

    }

}
