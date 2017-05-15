package com.util

import java.util

import scala.collection.mutable.ArrayBuffer

/**
  * Created by mercury on 17-5-8
  * project:location_opendsg
  * Package Name:com.util
  * Description:
  *
  * root 节点 (data,-1)
  * 若node.parent==-2说明不存在该节点
  */
class MyTreeParent[T](root: T, size: Int) {

    //定义节点类
    case class Node[T](data: T, parent: Int) {
        val this.data = data
        val this.parent = parent

        override def toString: String = "TreeParent$Node[data=" + data + ", parent=" + parent + "]"
    }

    //树的节点数目
    private var treeSize: Int = size
    //使用一个Node[]数组来记录该树里的所有节点
//    private
    var nodes = new ArrayBuffer[Node[T]](size)
    //添加根节点
    nodes.+=(new Node[T](root, -1))
    //记录节点数
     var nodeNums: Int = 1


    //为指定节点添加子节点
    def addNode(data: T, parent: Node[T]) {
        if (pos(parent) == (-2)) {
            System.out.println("the tree has not this node")
        } else {
            nodes.+=(new Node[T](data, pos(parent)))
            nodeNums += 1
        }
    }

    //判断树是否为空。
    def empty: Boolean = {
        //根节点是否为null
        nodes(0) == null
    }

    //返回根节点
    def rootNode: Node[T] = {
        //返回根节点
        nodes(0)
    }

    //返回指定节点（非根节点）的父节点。
    def parent(node: Node[T]): Node[T] = {
        //每个节点的parent记录了其父节点的位置
        nodes(node.parent)
    }

    //返回指定节点（非叶子节点）的所有子节点。
    def children(parent: Node[T]): Array[Node[T]] = {
        val list = new ArrayBuffer[Node[T]]
        var i: Int = 0
        while (i < nodeNums) {
            //如果当前节点的父节点的位置等于parent节点的位置
            if (nodes(i) != null && nodes(i).parent == pos(parent)) list.+=(nodes(i))
            i += 1
        }
        list.toArray
    }

    //返回包含指定值的节点。
    def pos(node: Node[T]): Int = {
        var i: Int = 0
        while (i < nodeNums) {
            //找到指定节点
            if (nodes(i).equals(node)) {
                return i
            }
            i += 1

        }
        return -2
    }

//    //get node through the node.data
//    def getNode(data: T): Node[T] = {
//        for (i <- 0 to nodeNums - 1) {
//            if (nodes(i).data == data) {
//                return nodes(i)
//            }
//        }
//        return new Node(root, -2)
//    }

    //get all leaf nodes index
    def getLeafNodesIndex(): Array[Int] = {
        var parentIndexArray=new ArrayBuffer[Int]()
        for (i <- 0 to nodeNums - 1) {
          parentIndexArray.+=(nodes(i).parent)
        }
        (0 to nodeNums-1).toArray.filterNot(parentIndexArray.toSet)
    }

    //get path node index from one leaf to root
    def getBranchNodeIndex(leafIndex:Int):Array[Int] ={
        var pathNodeIndexArray=new ArrayBuffer[Int]()
        //将叶子节点的索引加入
        pathNodeIndexArray.+=(leafIndex)
        var parentIndex=nodes(leafIndex).parent
        // 当不是根节点时，继续循环
        while (parentIndex != -1){
            pathNodeIndexArray.+=(parentIndex)
            parentIndex=nodes(parentIndex).parent
        }
        pathNodeIndexArray.toArray
    }

    //get all leafs paths
    def getAllBranchNodeIndex(): Array[Array[Int]] ={
        val leafNodesIndex=getLeafNodesIndex()
        var allBranchNodeIndex=new ArrayBuffer[Array[Int]]()
        for(i<- 0 to leafNodesIndex.length-1){
            allBranchNodeIndex.+=(getBranchNodeIndex(leafNodesIndex(i)))
        }
        allBranchNodeIndex.toArray
    }
}
