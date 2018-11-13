package datastructure.Util

import java.util


import datastructure.Node

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
      val set = node.getProp(key)
      if (set.size > 1) set.reduce(_ + ";" + _)
      else set.head
    }
    val str = s.reduce(_ + "," + _)
    node.getId + s",$str," + node.getLabel
  }
//  def writeFile(ls : Array[String], append : Boolean, outputPath : String, outputName : String) : Unit = ???
}
