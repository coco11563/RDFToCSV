package datastructure.Util

import java.io.FileWriter
import java.util

import datastructure.Node
import datastructure.Obj.TypeMap.TRIPLE
import org.eclipse.rdf4j.model.Statement
import org.eclipse.rdf4j.model.impl.{SimpleIRI, SimpleLiteral, SimpleValueFactory}

import scala.collection.mutable



object NodeUtils {
  def buildLabelNodeMap(nodeIter : Iterator[Node]) : mutable.HashMap[String, mutable.HashSet[Node]] = {
    val map = new mutable.HashMap[String, mutable.HashSet[Node]]()
    nodeIter.foreach(n => {
      val label = n.getLabel
      if (map.contains(label)) map(label).add(n) else map.put(label, mutable.HashSet[Node](n))
    })
    map
  }

  def buildCSV(nodeIter : Iterable[Node], m :  mutable.Map[String, Boolean]) : Iterable[String] = {
    nodeIter.map(n => buildCSVPerNode(n, m))
  }

  def generateSchema(nodeIter : Iterable[Node]) : mutable.Map[String, Boolean] = {
    val map = new mutable.HashMap[String, Boolean]()
    for (n <- nodeIter) {
      val schema = n.getPropSet
      for((k,v) <- schema) {
        if (map.contains(k) && !map(k) && v)
          map(k) = true
        else if (!map.contains(k)) map.put(k, v)
      }
    }
    map
  }

  def stringSchema(map : mutable.Map[String, Boolean]) : String = {
    val keySet = map.keySet
    var string = Array[String]("ENTITY_ID:ID")
    for (key <- keySet) {
      var str = key
      if (map(str)) str += ":String[]"
      string :+= str
    }
    string :+= "ENTITY_TYPE:LABEL"
    string.reduce(_ + "," + _)
  }

  def buildCSVPerNode(node: Node, schema : mutable.Map[String, Boolean]) : String = {
    val s = for (key <- schema.keySet) yield {
      val set = node.getProp(key).map(_.replace(",", " "))
      if (set.size > 1) set.reduce(_ + ";" + _)
      else set.head
    }
    val str = s.reduce(_ + "," + _)
    node.getId + s",$str," + node.getLabel
  }

  def buildNode(iter : Iterable[TRIPLE]) : Node = {
    var n : Node = null
    for (triple <- iter) {
      if (n == null)
        n = Node.instanceOf(triple._1, triple._2, triple._3)
      else n.handle(triple._1, triple._2, triple._3)
    }
    n
  }
  def buildNodeByStatement(iter : Iterable[Statement]) : Node = {
    var n : Node = null
    for (triple <- iter) {
      if (n == null)
        n = Node.build(triple)
      else n.handle(triple)
    }
    n
  }
  def main(args: Array[String]): Unit = {
    val vf = SimpleValueFactory.getInstance()
    var c = Array((vf.createIRI("http://gcm.wdcm.org/data/gcmAnnotation1/enzyme/1.5.1.17"), vf.createIRI("http://gcm.wdcm.org/ontology/gcmAnnotation/v1/substrate1"),  vf.createLiteral("lit1")),
        (vf.createIRI("http://gcm.wdcm.org/data/gcmAnnotation1/enzyme/1.5.1.17"), vf.createIRI("http://gcm.wdcm.org/ontology/gcmAnnotation/v1/substrate2"),  vf.createLiteral("lit2")),
        (vf.createIRI("http://gcm.wdcm.org/data/gcmAnnotation1/enzyme/1.5.1.17"), vf.createIRI("http://gcm.wdcm.org/ontology/gcmAnnotation/v1/substrate2"),  vf.createLiteral("lit4")),
        (vf.createIRI("http://gcm.wdcm.org/data/gcmAnnotation1/enzyme/1.5.1.17"), vf.createIRI("http://gcm.wdcm.org/ontology/gcmAnnotation/v1/substrate3"), vf.createLiteral("lit3")),
        (vf.createIRI("http://gcm.wdcm.org/data/gcmAnnotation1/enzyme/1.5.1.17"), vf.createIRI("http://gcm.wdcm.org/ontology/gcmAnnotation/v1/x-pathway"), vf.createIRI("http://gcm.wdcm.org/data/gcmAnnotation1/enzyme/1.5.1.18")),
        (vf.createIRI("http://gcm.wdcm.org/data/gcmAnnotation1/enzyme/1.5.1.17"), vf.createIRI("http://gcm.wdcm.org/ontology/gcmAnnotation/v1/x-pathway"), vf.createBNode("_:fuckyouannomynousnode")),
        (vf.createIRI("http://gcm.wdcm.org/data/gcmAnnotation1/enzyme/1.5.1.17"), vf.createIRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), vf.createIRI("http://gcm.wdcm.org/ontology/gcmAnnotation/v1/EnzymeNode"))
    )
    var n = buildNode(c.toIterable)
    print(n.getLabel)
  }
  def writeFile(ls : Array[String], append : Boolean, outputPath : String, outputName : String) : Unit = {
    val out = new FileWriter(outputPath + outputName,append)
    for (i <- ls) out.write(i + "\r\n")
    out.close()
  }
}
