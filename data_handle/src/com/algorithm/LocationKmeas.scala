package com.algorithm

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mercury on 17-4-22
  * project:location_opendsg
  * Package Name:com.algorithm
  * Description:
  */
object LocationKmeas {
    def main(args: Array[String]) {
        if (args.length < 5) {
            println("Usage:KMeansClustering trainingDataFilePath testDataFilePath numClusters numIterations runTimes")
            sys.exit(1)
        }

        val Array(inputFilePath, numClustersString, numIterationsString, runTimesString, clusterCenterOuput) = args
        val conf = new SparkConf().setAppName("location_opendsg_LocationKmeas")
                .setMaster("local")
        val sc = new SparkContext(conf)

        //输入数据
        val trainingData = sc.textFile(inputFilePath)
                .map(x => x.split("\t"))
        val parsedTrainingData =
            trainingData.map(line => {
                Vectors.dense(Array(line(3).toDouble, line(4).toDouble))
            }).cache()

        // Cluster the data into two classes using KMeans

        val numClusters = numClustersString.toInt
        val numIterations = numIterationsString.toInt
        val runTimes = runTimesString.toInt
        var clusterIndex: Int = 0
        val clusters: KMeansModel =
            KMeans.train(parsedTrainingData, numClusters, numIterations, runTimes)


        //begin to check which cluster each test data belongs to based on the clustering result
        val result = sc.textFile(inputFilePath)
                .map(x => x.split("\t"))
                .map(x => {
                    val index = clusters.predict(Vectors.dense(Array(x(3).toDouble, x(4).toDouble)))
                    (index, 1)
                })
                .reduceByKey(_ + _)
                .map(x => (clusters.clusterCenters(x._1).toArray.mkString("\t"), x._2))
                .repartition(1)

        result.map {
            case (lng_lat, day_stay_time) => {
                s"$lng_lat\t$day_stay_time\t#de4c4f"
            }
        }.saveAsTextFile(clusterCenterOuput)
        println("Spark MLlib K-means clustering test finished.")
    }

}