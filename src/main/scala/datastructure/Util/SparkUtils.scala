package datastructure.Util
import java.io.{File, FileReader}

import datastructure.Util.NodeUtils.buildCSVPerNode
import datastructure.{Node, NodeTreeHandler}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.eclipse.rdf4j.model.{BNode, Statement}
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

  def process(path: List[String], sc: SparkContext, format: RDFFormat, outpath: String, index: Int): Unit = {
    val rdfParser = Rio.createParser(format)

    val handler = new NodeTreeHandler
    println("done init")
    rdfParser.setRDFHandler(handler)

    for (p <- path) {
      println(s"now we are parsing $p")
      rdfParser.parse(new FileReader(new File(p)), "")
    }

    println("done resolve")

    val nodes = handler.fileStatementQueue

    val nodeRDD = SparkUtils
      .groupBuildTriples(SparkUtils.groupById(sc))(SparkUtils.filterByIsNNode)(nodes.toList)
      .map(i => NodeUtils.buildNodeByStatement(i))

//    val bnodeMap = mutable.HashMap(buildBNodePair(nodeRDD) : _*)
//
//    val nodeMap = buildNodeMap(nodeRDD)
//
//    val bnodeArray = SparkUtils.groupBuildTriples(SparkUtils.groupById(sc))(SparkUtils.filterByIsBNode)(nodes.toList).flatMap(_.toList).collect()

//    val (bub, tail) : (mutable.Queue[Statement], mutable.Queue[Statement]) = NodeUtils.bNodeBiFilter(bnodeArray)
//
//    NodeUtils.processBUBNode(bub, nodeMap, bnodeMap)
//
//    NodeUtils.processBNode(tail, nodeMap, bnodeMap)

//    val finalNodeArray = sc.parallelize(nodeMap.values.toList)

    val labeledFinalNodeArray = nodeRDD
      .map(n => (n.getLabel, n))
      .groupByKey()
      .collect()

    var count = 0

    for (nodelist <- labeledFinalNodeArray) {

      val label = sc.broadcast[String](nodelist._1)

      val labeledNodes: Iterable[Node] = nodelist._2

      val labeledNode = sc.parallelize(labeledNodes.toList)

      val schemaMap = sc.broadcast[mutable.Map[String, Boolean]](SparkUtils.generateSchema(labeledNode))

      println(schemaMap.value.keySet)

      val csvStr = SparkUtils.buildCSV(labeledNode, schemaMap.value).collect()

      val csvHead = NodeUtils.stringSchema(schemaMap.value)

      println("now - " + label.value)

      println("schema is - " + csvHead)

      NodeUtils
        .writeFile(csvHead +: csvStr,
          append = false,
          outpath + s"$index/",
          label.value + "_ent_.csv")

      val relationHead = ":START_ID,:END_ID,:TYPE"

      val relationship = SparkUtils.buildNodeRelationCSV(labeledNode).collect()

      NodeUtils
        .writeFile(relationHead +: relationship,
          append = false,
          outpath + s"$index/",
          label.value + "_rel_.csv")
    }
  }

  def buildCSV(nodeIter: RDD[Node], m: mutable.Map[String, Boolean]): RDD[String] = {
    nodeIter.map(n => buildCSVPerNode(n, m))
  }

  //  def buildBNodePair(nodeArray : RDD[Node]) : Array[(B, mutable.Set[String])] = {
  //    nodeArray
  //      .map(a => a.getBNodeMap.keySet.map((a.getId, _)))
  //      .flatMap(_.toList)
  //      .map(a => (a._2, mutable.Set(a._1)))
  //      .reduceByKey(_ ++ _)
  //      .collect()
  //  }

  def buildNodeMap(nodeArray: RDD[Node]): Map[String, Node] = {
    nodeArray.map(n => n.getId -> n).collect().toMap
  }

  def generateSchema(nodes: RDD[Node]): mutable.Map[String, Boolean] = {
    nodes.map(n => n.getPropSet).reduce((a, b) => {
      val set = a.keySet
      for (s <- set) {
        if (b.contains(s) && !b(s) && a(s)) b.update(s, true)
        else if (!b.contains(s)) b.put(s, a(s))
      }
      b
    })
  }

  def buildNodeRelationCSV(nodeArray: RDD[Node]): RDD[String] = nodeArray.map(n => n.getNodeRelation).flatMap(_.toList)
}
