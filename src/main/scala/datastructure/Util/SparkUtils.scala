package datastructure.Util
import datastructure.Node
import datastructure.Obj.TypeMap.TRIPLE
import datastructure.Util.NodeUtils.buildCSVPerNode
import org.apache.spark.{SparkContext, rdd}
import org.apache.spark.rdd.RDD
import org.eclipse.rdf4j.model.{BNode, IRI}

import scala.collection.mutable
object SparkUtils {
  def groupById(sc : SparkContext)(triples : List[TRIPLE]):RDD[Iterable[TRIPLE]] = sc.parallelize(triples).map(a => (a._1 , a)).groupByKey().values

  def filterByIsBNode(iter : Iterable[TRIPLE]) : Boolean = iter.head._1.isInstanceOf[BNode]

  def filterByIsNNode(iter : Iterable[TRIPLE]) : Boolean = iter.head._1.isInstanceOf[IRI]
  /**
    *
    * @param groupFun how to group triples
    * @param filter how to filter triples
    * @param triples the original triples
    * @return Triple RDD
    */
  def groupBuildTriples(groupFun : List[TRIPLE] => RDD[Iterable[TRIPLE]])
                    (filter : Iterable[TRIPLE] => Boolean)
                    (triples : List[TRIPLE]) : RDD[Iterable[TRIPLE]] = {
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
}
