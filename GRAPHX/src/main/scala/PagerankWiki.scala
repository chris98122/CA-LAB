import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
object PagerankWiki {
  def main(args: Array[String]) {
    /**
     * SparkContext 的初始化需要一个SparkConf对象
     * SparkConf包含了Spark集群的配置的各种参数
     */
    val conf = new SparkConf()
      .setMaster("local") //启动本地化计算
      .setAppName("testRdd") //设置本程序名称
    //Spark程序的编写都是从SparkContext开始的
    val sc = new SparkContext(conf)
    // Load the edges as a graph
    val graph = GraphLoader.edgeListFile(sc, ".//file//Wiki-Vote.txt" )

    // Run PageRank
    val ranks = graph.pageRank(0.0001).vertices
    // Print the result
    println(ranks.collect().mkString("\n"))
  }
}
