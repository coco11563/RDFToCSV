package datastructure.Util
import java.io.{File, FileReader}

import datastructure.{Node, NodeTreeHandler}
import datastructure.Obj.TypeMap.TRIPLE
import datastructure.Util.NodeUtils.buildCSVPerNode
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.eclipse.rdf4j.model.{BNode, IRI, Statement}
import org.eclipse.rdf4j.rio.{RDFFormat, Rio}

import scala.collection.mutable
object SparkUtils {

  def groupById(sc : SparkContext)(triples : List[Statement]):RDD[Iterable[Statement]] = sc.parallelize(triples).map(a => (a.getSubject , a)).groupByKey().values

  def filterByIsBNode(iter : Iterable[Statement]) : Boolean = iter.head.getSubject.isInstanceOf[BNode]

  def filterByIsNNode(iter : Iterable[Statement]) : Boolean = iter.head.getSubject.isInstanceOf[IRI]
  /**
    *
    * @param groupFun how to group triples
    * @param filter how to filter triples
    * @param triples the original triples
    * @return Triple RDD
    */
  def groupBuildTriples(groupFun : List[Statement] => RDD[Iterable[Statement]])
                    (filter : Iterable[Statement] => Boolean)
                    (triples : List[Statement]) : RDD[Iterable[Statement]] = {
    groupFun(triples).filter(filter)
  }

  def enQueue[T] (rdd : RDD[T]) : mutable.Queue[T] = {
    val q = new mutable.Queue[T]
    rdd.collect().foreach(q.enqueue(_))
    q
  }

  def generateSchema(nodes : RDD[Node]) : mutable.Map[String, Boolean] = {
    nodes.map(n => n.getPropSet).reduce((a,b) => {
      val set = a.keySet
      for (s <- set) {
        if (b.contains(s) && !b(s) && a(s)) b.update(s, true)
        else b.put(s, a(s))
      }
      b
    })
  }

  def buildCSV(nodeIter : RDD[Node], m :  mutable.Map[String, Boolean]) : RDD[String] = {
    nodeIter.map(n => buildCSVPerNode(n, m))
  }


  def process(path:String) :Unit = {
    val rdfParser = Rio.createParser(RDFFormat.N3)
    val handler = new NodeTreeHandler
    println("done init")
    rdfParser.setRDFHandler(handler)
    rdfParser.parse(new FileReader(new File(path)), "")
    println("done resolve")
    val sparkConf = new SparkConf()
      .setAppName("ProcessOnRDFFFFFFFFFFF")
      .set("spark.driver.maxResultSize", "4g")
    val sc : SparkContext = new SparkContext(sparkConf)
    val nodes = handler.fileStatementQueue
    import scala.collection.JavaConverters._
    val nodeArray = SparkUtils.groupBuildTriples(SparkUtils.groupById(sc))(SparkUtils.filterByIsNNode)(nodes.toList).map(i => NodeUtils.buildNodeByStatement(i))
    val schemaMap = SparkUtils.generateSchema(nodeArray)
    val csvStr = SparkUtils.buildCSV(nodeArray, schemaMap).collect()
    val csvHead = NodeUtils.stringSchema(schemaMap)
    NodeUtils.writeFile(csvHead +: csvStr , append = false, "/data2/test/" , path.split("/").reduce(_ + _).replace("n3", "csv"))

  }
}
