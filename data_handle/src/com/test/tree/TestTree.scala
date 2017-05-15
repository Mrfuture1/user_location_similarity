package com.test.tree

import com.util.BuildTree
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mercury on 17-5-8
  * project:location_opendsg
  * Package Name:com.test
  * Description:
  */
object TestTree {
    def main(args: Array[String]): Unit = {
        val Array(inputFilePath, dataOutput) = args

        //        val myTreeParent=new MyTreeParent((-1,-1),12)
        //        val root=myTreeParent.rootNode
        //        myTreeParent.addNode((1,1),myTreeParent.getNode((-1,-1)))
        //        myTreeParent.addNode((2,1),myTreeParent.getNode((-1,-1)))
        //        System.out.println(myTreeParent.rootNode)
        //        val allBranchNodeIndex=myTreeParent.getAllBranchNodeIndex()
        //        for(i<- 0 to allBranchNodeIndex.length-1){
        //            System.out.println(allBranchNodeIndex(i).mkString(","))
        //        }

        // ((lng,lat),startTime,stayTime)
        val locA = ((11.0, 12.0), 1L, 3.0)
        val locB = ((12.0, 13.0), 5L, 2.0)
        val locC = ((13.0, 14.0), 8L, 1.0)
        val locD = ((12.0, 13.0), 10L, 3.0)
        val locE = ((14.0, 15.0), 20L, 2.0)
        val locF = ((11.0, 12.0), 30L, 3.0)


        val listA = List(locA, locB, locC, locD, locE, locF)
        //        val listA = List(locA, locB, locC, locE)
        val listB = List(locA, locB, locC, locE)
        val listC = List(locA)
        val buildTree = new BuildTree[Double](listA, listB)
        val tree = buildTree.buildTreeFromLocation(1)
        //查看树结构
        val allNodes = tree.nodes
        for (i <- 0 to allNodes.length - 1) {
            System.out.println(allNodes(i))
        }
        val allBranchNodeIndex = tree.getAllBranchNodeIndex()
        for (i <- 0 to allBranchNodeIndex.length - 1) {
            System.out.println(allBranchNodeIndex(i).mkString(","))
        }


        /**
          * RDD测试
          */
        var dataSource = listA.map(x => (188L, x)) ++: (listB.map(x => (189L, x))).++:(listC.map(x => (190L, x))).toArray
        val conf = new SparkConf().setAppName("location_opendsg_FrequencyMost")
                .setMaster("local")
        val sc = new SparkContext(conf)

        // 实际应用时,应该用家或者工作的位置做一些限定,以减少计算量
        // 构造home数据
        val homeRdd = sc.parallelize(Array((188L, (11.0, 12.0)), (189L, (11.0, 12.0)), (190L, (12.0, 12.0))))
        // 轨迹数据
        val dataRdd = sc.parallelize(dataSource).groupByKey().map(x => (x._1, x._2.toList.sortBy(x => x._2)))
        //每个位置重要度 log(n/N)
        val userTotalNum = sc.parallelize(dataSource).map(x => x._1).distinct().count() //总用户数
        val locUsersNumMapTmp = sc.parallelize(dataSource).map(x => (x._1, x._2._1)).distinct().map(x => (x._2, 1)).countByKey()

        val locUsersNumMap = collection.mutable.HashMap(locUsersNumMapTmp.toSeq: _*)
        val locScore = locUsersNumMap.map(x => (x._1, math.log10(userTotalNum.toDouble / x._2)))

        //比较用户位置相似性
        val homeMap = collection.mutable.HashMap(homeRdd.collectAsMap().toSeq: _*)
        //        dataRdd.map(x => (homeMap.get(x._1).get, x))  注意join改变数据顺序 ， 改用map
        homeRdd.join(dataRdd)
                //按地区,考虑用户相似性
                .map(x => ((x._2._1, (x._1, (x._2._2, x._2._2.map(y => locScore.get(y._1).get).sum)))))
                .groupByKey()
                .flatMapValues(x => {
                    val i1 = x
                    val i2 = x.toArray
                    i1.flatMap(y => {
                        i2.filter(z => z._1 > y._1).map(z => {
                            val buildTree = new BuildTree[Double](y._2._1, z._2._1)
                            val tree = buildTree.buildTreeFromLocation(1)
                            System.out.println(y._2._1.mkString(",") + "zy," + z._2._1.mkString(","))
                            //查看树结构
                            val allNodes = tree.nodes
                            for (i <- 0 to allNodes.length - 1) {
                                System.out.println(allNodes(i))
                            }
                            val allBranchNodeIndex = tree.getAllBranchNodeIndex()
                            for (i <- 0 to allBranchNodeIndex.length - 1) {
                                System.out.println(allBranchNodeIndex(i).mkString(","))
                            }
                            val score = buildTree.similarScore(1, allBranchNodeIndex, tree, locScore, y._2._2, z._2._2)
                            (y._1, z._1, score, y._2._1.mkString(","), z._2._1.mkString(","))
                        })
                    })
                })
                .sortBy(x => (x._2))
                .map {
                    case ((jing, wei), (phoneA, phoneB, score, listLocA, listLocB)) => {
                        s"$jing\t$wei\t$phoneA\t$phoneB\t$score\t$listLocA;$listLocB"
                    }
                }.saveAsTextFile(dataOutput)


    }


}
