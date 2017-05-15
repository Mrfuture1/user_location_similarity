package com.util

import scala.collection.mutable.ArrayBuffer
import util.control.Breaks._
import java.util

import scala.collection.mutable

/**
  * Created by mercury on 17-5-8
  * project:location_opendsg
  * Package Name:com.util
  * Description:
  */
class BuildTree[E](listLocA: List[((E, E), Long, Double)], listLocB: List[((E, E), Long, Double)]) {
    val listLocationA = listLocA
    val listLocationB = listLocB

    def buildTreeFromLocation[E](timeP: Double): MyTreeParent[(Int, Int)] = {
        //找到两个用户共同去过的地点
        val commonLocationArray = findCommonLocation()
        //树的根节点为((0,0),-1)
        val tp = new MyTreeParent[(Int, Int)]((-1, -1), commonLocationArray.length + 1)


        if (commonLocationArray.length > 1) {
            //新加入的元素和目前树的叶子节点及其父节点作比较
            for (i <- 1 to commonLocationArray.length - 1) {
                // 维护已经比较过的树中的节点
                var comparedNodes = new ArrayBuffer[Int]()
                //维护叶子节点列表
                var leafNodes = tp.getLeafNodesIndex()

                for (j <- 0 to leafNodes.length - 1) {
                    breakable {
                        //对每个叶子节点依次向上查找
                        var nodeIndex = leafNodes(j)
                        var leafNode = tp.nodes(nodeIndex)
                        var parentTmp = leafNode.parent
                        //比较至根节点,但不包括根节点，因为根节点没有时间。并且该节点之前没有比较过
                        while (parentTmp != -1 && !comparedNodes.contains(nodeIndex)) {

                            // 判断是否符合索引要求
                            if (commonLocationArray(i)._1 > leafNode.data._1 && commonLocationArray(i)._2 > leafNode.data._2) {
                                // 判断时间是否符合要求
                                val leaveJTimeA = listLocationA(leafNode.data._1)._2 + listLocationA(leafNode.data._1)._3
                                val arriveITimeA = listLocationA(commonLocationArray(i)._1)._2
                                val leaveJTimeB = listLocationB(leafNode.data._2)._2 + listLocationB(leafNode.data._2)._3
                                val arriveITimeB = listLocationB(commonLocationArray(i)._2)._2
                                val timePercent = math.abs((arriveITimeA - leaveJTimeA) - (arriveITimeB - leaveJTimeB)) / math.max((arriveITimeA - leaveJTimeA), (arriveITimeB - leaveJTimeB))
                                if (timePercent < timeP) {
                                    tp.addNode(commonLocationArray(i), leafNode)
                                    comparedNodes.+=(nodeIndex)
                                    break()
                                }
                            }
                            comparedNodes.+=(nodeIndex)
                            nodeIndex = parentTmp
                            leafNode = tp.nodes(nodeIndex)
                            parentTmp = leafNode.parent
                        }
                        //如果比较到了根节点，则在根节点添加子节点
                        if (parentTmp == -1 && !comparedNodes.contains(nodeIndex)) {
                            tp.addNode(commonLocationArray(i), leafNode)
                            comparedNodes.+=(0)
                        }
                    }
                }
            }


            //下面这种直接比较数组中元素的方法，不能判别多父节点的情况
            //            //保证Array中的元素数多于1
            //            for (i <- 1 to commonLocationArray.length - 1) {
            //                var j = i
            //                // 索引(即到达地点的先后顺序)限制  &&  到达时间限制
            //                breakable {
            //                    var ifBreak = false
            //                    while (true) {
            //                        j = j - 1
            //                        // 先确定索引的先后位置
            //                        if (commonLocationArray(i)._1 > commonLocationArray(j)._1 && commonLocationArray(i)._2 > commonLocationArray(j)._2) {
            //                            if (j != 0) {
            //                                // 判断时间是否符合要求
            //                                val leaveJTimeA = listLocationA(commonLocationArray(j)._1)._2 + listLocationA(commonLocationArray(j)._1)._3
            //                                val arriveITimeA = listLocationA(commonLocationArray(i)._1)._2
            //                                val leaveJTimeB = listLocationB(commonLocationArray(j)._2)._2 + listLocationB(commonLocationArray(j)._2)._3
            //                                val arriveITimeB = listLocationB(commonLocationArray(i)._2)._2
            //                                val timePercent = math.abs((arriveITimeA - leaveJTimeA) - (arriveITimeB - leaveJTimeB)) / math.max((arriveITimeA - leaveJTimeA), (arriveITimeB - leaveJTimeB))
            //                                if (timePercent < timeP) {
            //                                    tp.addNode(commonLocationArray(i), tp.getNode((commonLocationArray(j))))
            //                                    break()
            //                                }
            //                            }else{
            //                                tp.addNode(commonLocationArray(i), tp.getNode((commonLocationArray(j))))
            //                                break()
            //                            }
            //                        }
            //                    }
            //                }
            //            }

        }
        tp
    }

    /**
      * function:两人共同去过的地点
      * input parameter:[listLocationA, listLocationB]
      * return type:scala.Tuple2<java.lang.Object,java.lang.Object>[]
      * output:
      */
    def findCommonLocation[E](): Array[(Int, Int)] = {

        var commonLocationList = new ArrayBuffer[(Int, Int)]
        //加入首元素（-1，,1），便于后续构建树
        commonLocationList.+=((-1, -1))
        for (i <- 0 to listLocationA.length - 1) {
            for (j <- 0 to listLocationB.length - 1) {
                if (listLocationA(i)._1._1 == listLocationB(j)._1._1 && listLocationA(i)._1._2 == listLocationB(j)._1._2) {
                    commonLocationList.+=((i, j))
                }
            }
        }
        commonLocationList.toArray
    }


    def similarScore(topN:Int,branchNodes: Array[Array[Int]], tree: MyTreeParent[(Int,Int)], locScore: mutable.HashMap[(E, E), Double], listScoreA: Double, listScoreB: Double): Double = {
        //取相似序列长度top1的序列，因为一些短序列也会被包含其中
        val topPath = branchNodes.map(x => (x, x.length)).sortBy(x => x._2).takeRight(topN)
        val scoreSum = topPath.map(x => {
            val vpSum = x._1.dropRight(1).map(y => {
                val loc = tree.nodes(y).data
//                val locUserNum = locUsersNumMap.get(listLocationA(loc._1)._1).get
//                math.log10(userTotalNum.toDouble / locUserNum)
                locScore.get(listLocationA(loc._1)._1).get
            }).sum
            vpSum / ((listScoreA + listScoreB) / 2)
        }).sum / topN
        scoreSum
    }
}
