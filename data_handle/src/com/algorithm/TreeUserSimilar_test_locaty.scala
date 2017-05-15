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
object TreeUserSimilar_test_locaty {
    def main(args: Array[String]): Unit = {
        val Array(trajectoryPath, homePath, date, topN, scoreOutput, scoreTopOutput, trouble) = args
        //        val date = 20160530
        //        val topN = 1

        val conf = new SparkConf().setAppName("TreeUserSimilar")
                .setMaster("local")
        val sc = new SparkContext(conf)

        // 实际应用时,应该用家或者工作的位置做一些限定,以减少计算量
        // 读取home数据。   可以通过更改家的精度来适当减少计算量
        val homeRdd = sc.textFile(homePath).map(x => x.split("\t")).map(x => (x(0).toLong, (x(1).toDouble, x(2).toDouble)))

        // 轨迹数据
        val trajectoryRdd = sc.textFile(trajectoryPath).map(x => x.split("\t"))
                //                .filter(x => x(5).toLong == date.toInt) //一天的轨迹进行比较
                .map(x => (x(0).toLong, ((x(1).toDouble, x(2).toDouble), x(3).toLong, x(4).toDouble)))

        val trajectoryArrayData = trajectoryRdd.groupByKey()
                .map(x => (x._1, x._2.toList.sortBy(x => x._2)))
                .filter(x => x._2.size > 1) //仅比较位置数>1的用户

        //每个位置重要度 log(n/N)
        val userTotalNum = trajectoryRdd.map(x => x._1).distinct().count() //总用户数
        val locUsersNumMapTmp = trajectoryRdd.map(x => (x._1, x._2._1)).distinct().map(x => (x._2, 1)).countByKey()

        val locUsersNumMap = collection.mutable.HashMap(locUsersNumMapTmp.toSeq: _*)
        val locScoreTmp = locUsersNumMap.map(x => (x._1, math.log10(userTotalNum.toDouble / x._2)))
        val locScore = sc.broadcast(locScoreTmp)


        //比较用户位置相似性
        //        val allData = homeRdd.join(trajectoryArrayData)
        //        allData.map(x => ((x._2._1, x._1), x._2._2)).flatMapValues(x => x)
        //                .map {
        //                    case (((jing, wei), phone), ((lng, lat), arrive_time, stay_time)) => {
        //                        s"$jing\t$wei\t$phone\t$lng\t$lat\t$arrive_time\t$stay_time"
        //                    }
        //                }.saveAsTextFile(scoreOutput + "_allData")
        val allData = sc.textFile(trouble)
                .map(x => x.split("\t"))
                .map(x => (((x(0).toLong, x(1).toLong), x(2).toLong, x(7).toDouble), ((x(3).toDouble, x(4).toDouble), x(5).toLong, x(6).toDouble)))
                .groupByKey()
                .map(x => (x._1._1, (x._1._2, (x._2.toList, x._1._3))))

        allData
                //                .map(x => ((x._2._1, (x._1, (x._2._2, x._2._2.map(y => locScore.value.get(y._1).get).sum)))))
                .groupByKey()
                .map(x => (x._1))
                .foreach(x => println("zy::::" + x._1 + "..." + x._2))

        //按地区,考虑用户相似性
        val userSimilarSocre = allData
                //                .map(x => ((x._2._1, (x._1, (x._2._2, x._2._2.map(y => locScore.value.get(y._1).get).sum)))))
                .groupByKey(1)
                .flatMapValues(x => {
                    //                                        val i1 = x
                    //                                        val i2 = x.toArray
                    //                                        i1.flatMap(y => {
                    //                                            i2.filter(z => z._1 > y._1)
                    //                                                    .map(z => {
                    //                                                        val buildTree = new BuildTree[Double](y._2._1, z._2._1)
                    //                                                        val tree = buildTree.buildTreeFromLocation(1)
                    //                                                        //查看树结构
                    //                                                        //                                    val allBranchNodeIndex = tree.getAllBranchNodeIndex()
                    //                                                        //                                    val score = buildTree.similarScore(topN.toInt, allBranchNodeIndex, tree, locScore.value, y._2._2, z._2._2)
                    //                                                        (y._1, z._1, 3)
                    //                                                    })
                    //                                                    .filter(z => z._3 != 0)
                    //                                        })
                    //改用for循环实现
                    var scoreArray = ArrayBuffer.empty[(Long, Long, Double)]

                    val tmp = x.toArray
                    println("zy.................")
                                        for(i <-0 to tmp.size-1){
                                            for(ele<- tmp(i)._2._1){
                                                println(tmp(i)._1+"\t"+ele._1._1+"\t"+ele._1._2+"\t"+ele._2+"\t"+ele._3+"\t"+tmp(i)._2._2+"\t"+"   :zy")
                                            }
                                        }
                    val size = tmp.size
                                        for (i <- 0 to size - 1) {
                                            for (j <- i + 1 to size - 1) {
                                                val y = tmp(i)
                                                val z = tmp(j)
                                                val buildTree = new BuildTree[Double](y._2._1, z._2._1)
                                                val tree = buildTree.buildTreeFromLocation(1)
                                                //查看树结构
                                                val allBranchNodeIndex = tree.getAllBranchNodeIndex()
                                                val allNodes = tree.nodes

                                                for (x <- 0 to allNodes.length - 1) {
                                                    System.out.println(allNodes(x))
                                                }
                                                for (x <- 0 to allBranchNodeIndex.length - 1) {
                                                    System.out.println(allBranchNodeIndex(x).mkString(","))
                                                }
                                                val score = buildTree.similarScore(topN.toInt, allBranchNodeIndex, tree, locScore.value, y._2._2, z._2._2)
                                                scoreArray.+=((y._1, z._1, score))
                                            }
                                        }
//                    scoreArray.filter(z => z._3 > 0)
                    scoreArray
                })
                .persist(StorageLevel.DISK_ONLY)
                .sortBy(x => (x._2._1, x._2._3), false, 50)




        userSimilarSocre.map {
            case ((jing, wei), (phoneA, phoneB, score)) => {
                s"$jing\t$wei\t$phoneA\t$phoneB\t$score"
            }
        }.saveAsTextFile(scoreOutput)

        //        userSimilarSocre.filter(x => x._2._3 > 0.7).map {
        //            case ((jing, wei), (phoneA, phoneB, score)) => {
        //                s"$jing\t$wei\t$phoneA\t$phoneB\t$score"
        //            }
        //        }.saveAsTextFile(scoreTopOutput)
    }

}
