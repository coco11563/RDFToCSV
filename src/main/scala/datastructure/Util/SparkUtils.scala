package datastructure.Util
import java.io.{File, FileReader}

import datastructure.Obj.TypeMap.B
import datastructure.Util.NodeUtils.buildCSVPerNode
import datastructure.{Node, NodeTreeHandler}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.eclipse.rdf4j.model.{BNode, IRI, Statement}
import org.eclipse.rdf4j.rio.{RDFFormat, Rio}

import scala.collection.mutable
object SparkUtils {

  def groupById(sc : SparkContext)(triples : List[Statement]):RDD[Iterable[Statement]] =
    sc.parallelize(triples)
      .map(a => (a.getSubject , a))
      .groupByKey()
      .values

  def filterByIsBNode(iter : Iterable[Statement]) : Boolean = iter.head.getSubject.isInstanceOf[BNode]

  def filterByIsNNode(iter : Iterable[Statement]) : Boolean = !iter.head.getSubject.isInstanceOf[BNode]
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

  def buildBNodePair(nodeArray : RDD[Node]) : Array[(B, mutable.Set[String])] = {
    nodeArray
      .map(a => a.getBNodeMap.keySet.map((a.getId, _)))
      .flatMap(_.toList)
      .map(a => (a._2, mutable.Set(a._1)))
      .reduceByKey(_ ++ _)
      .collect()
  }

  def buildNodeMap(nodeArray : RDD[Node]) : Map[String, Node] = {
    nodeArray.map(n => n.getId -> n).collect().toMap
  }

  def process(path:String, sc : SparkContext, format : RDFFormat) :Unit = {
    val rdfParser = Rio.createParser(format)
    val handler = new NodeTreeHandler
    println("done init")
    rdfParser.setRDFHandler(handler)
    rdfParser.parse(new FileReader(new File(path)), "")
    println("done resolve")

    val nodes = handler.fileStatementQueue

    val nodeRDD = SparkUtils.groupBuildTriples(SparkUtils.groupById(sc))(SparkUtils.filterByIsNNode)(nodes.toList).map(i => NodeUtils.buildNodeByStatement(i))

    val bnodeMap = mutable.HashMap(buildBNodePair(nodeRDD) : _*)

    val nodeMap = buildNodeMap(nodeRDD)

    val bnodeArray = SparkUtils.groupBuildTriples(SparkUtils.groupById(sc))(SparkUtils.filterByIsBNode)(nodes.toList).flatMap(_.toList).collect()

    val (bub, tail) : (mutable.Queue[Statement], mutable.Queue[Statement]) = NodeUtils.bNodeBiFilter(bnodeArray)

    NodeUtils.processBUBNode(bub, nodeMap, bnodeMap)

    NodeUtils.processBNode(tail, nodeMap, bnodeMap)

    val finalNodeArray = sc.parallelize(nodeMap.values.toList)

    val labeledFinalNodeArray = finalNodeArray.map(n => (n.getLabel, n))
      .groupByKey()
      .collect()

    for (nodelist <- labeledFinalNodeArray) {
      val label = nodelist._1

      val labeledNodes = sc.parallelize(nodelist._2.toList)

      val schemaMap = SparkUtils.generateSchema(labeledNodes)

      val csvStr = SparkUtils.buildCSV(labeledNodes, schemaMap).collect()

      val csvHead = NodeUtils.stringSchema(schemaMap)

      println("now - " + label)

      println("schema is - " + csvHead)

      NodeUtils.writeFile(csvHead +: csvStr , append = false, "/data2/test/" , (path.split("/").reduce(_ + _) + label).replace("n3", "csv"))
    }
  }
}
