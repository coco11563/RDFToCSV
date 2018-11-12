package datastructure.Util

import datastructure.Node

import scala.collection.mutable

object NodeUtils {
  def buildLabelNodeMap(nodeIter : Iterable[Node]) : mutable.HashMap[String, Node] = ???
  def buildCSV(nodeIter : Iterable[Node]) : Array[String] = ???
  def generateSchema(nodeIter : Iterable[Node]) : mutable.Map[String, Boolean] = ???
  def buildCSVPerNode(node: Node, schema : mutable.Map[String, Boolean]) : String = ???
  def writeFile(ls : Array[String], append : Boolean, outputPath : String, outputName : String) : Unit = ???
}
