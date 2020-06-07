import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

object SSSP_wiki {
  def main(args: Array[String]) {
    /**
     * SparkContext 的初始化需要一个SparkConf对象
     * SparkConf包含了Spark集群的配置的各种参数
     */
    val conf = new SparkConf()
      .setAppName("testRdd") //设置本程序名称

    //Spark程序的编写都是从SparkContext开始的
    val sc = new SparkContext(conf)

    //以上的语句等价与val sc=new SparkContext("local","testRdd")
    val path = ".//file//Wiki-Vote.txt" // 数据集文件地址
    val loadedGraph = GraphLoader.edgeListFile(sc, path)

    // A graph with edge attributes containing distances
    val graph: Graph[Int, Double] =
      loadedGraph.mapEdges(e => e.attr.toDouble)
    //graph.edges.foreach(println)

    val sourceId: VertexId = 4 // The ultimate source

    // Initialize the graph such that all vertices except the root have distance infinity.
    //初始化各节点到原点的距离
    val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)

    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      // Vertex Program，节点处理消息的函数，dist为原节点属性（Double），newDist为消息类型（Double）
      (id, dist, newDist) => math.min(dist, newDist),

      // Send Message，发送消息函数，返回结果为（目标节点id，消息（即最短距离））
      triplet => {
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      //Merge Message，对消息进行合并的操作，类似于Hadoop中的combiner
      (a, b) => math.min(a, b)
    )
    println(sssp.vertices.collect.mkString("\n"))
  }
}
