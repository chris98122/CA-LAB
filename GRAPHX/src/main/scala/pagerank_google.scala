import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

object pagerank_google {
  def main(args: Array[String]) {
    /**
     * SparkContext 的初始化需要一个SparkConf对象
     * SparkConf包含了Spark集群的配置的各种参数
     */
    val conf = new SparkConf()
      .setAppName("testRdd") //设置本程序名称

    //jar包的位置是本地开发环境的地址
    //Spark程序的编写都是从SparkContext开始的
    val sc = new SparkContext(conf)
    // Load the edges as a graph
    val graph = GraphLoader.edgeListFile(sc, ".//file//web-Google.txt")
    // Run PageRank
    val ranks = graph.pageRank(0.0001).vertices
    // Print the result
    println(ranks.collect().mkString("\n"))
  }
}
