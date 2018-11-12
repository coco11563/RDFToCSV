package datastructure

import java.util.concurrent.Callable

import datastructure.Obj.Property
import datastructure.Obj.TypeMap.{B, L, U}
import org.eclipse.rdf4j.model._
import org.eclipse.rdf4j.rio.RDFHandler

import scala.collection.mutable

/**
  * we assuming that all node insert is in one label
  *
  * which means one file for one label
  *
  * the program will using rdf4j's implicit RDF parser
  *
  *
  */
class NodeTreeHandler extends RDFHandler with Callable[Int]{
  val NodeMap : mutable.HashMap[IRI, Node] = new mutable.HashMap[IRI, Node]()
  val BNodeMap : mutable.HashMap[BNode, mutable.Set[Node]] = new mutable.HashMap[BNode, mutable.Set[Node]]()
  val PropNames : mutable.HashSet[Property] = new mutable.HashSet[Property]()
  val relationQueue : mutable.Queue[Statement] = new mutable.Queue[Statement]()
  val unhandleBNodeTail : mutable.Queue[Statement] = new mutable.Queue[Statement]()

  override def handleStatement(statement: Statement) : Unit = {
    handleStatement(statement.getSubject, statement.getPredicate, statement.getObject, statement)
  }

  /**
    * handle all the statement
    *
    * @param subject the ori statement's subject (Value)
    * @param predicate the ori statement's predicate URI, in rdf4j it should be a IRI (inhere from Resource)
    * @param obj the ori statement's object value (Value)
    * @param statement the origin statement
    */

  def handleStatement(subject: Resource, predicate : IRI, obj : Value, statement: Statement) : Unit = (subject, predicate, obj) match {
    case (a:U, b:U, c:L) =>
      if (NodeMap.contains(a)) NodeMap(a).handle(statement)
      else NodeMap.put(a, Node.build(statement))
    case (a:U, b:U, c:U) =>
      if (NodeMap.contains(a)) NodeMap(a).handle(statement)
      else NodeMap.put(a, Node.build(statement))
    case (a:U, b:U, c:B) =>
      val node : Node =
        if (NodeMap.contains(a)) {
          val n = NodeMap(a)
          n.handle(statement)
          n
        }
      else {
        val n = Node.build(statement)
        NodeMap.put(a, n)
        n
      }
      if (BNodeMap.contains(c)) BNodeMap(c).add(node)
      else BNodeMap.put(c, mutable.Set(node))
    case (a:B, b:U, c:L) =>
      if (BNodeMap.contains(a)) BNodeMap(a).foreach(n => n.handle(statement))
      else unhandleBNodeTail.enqueue(statement)
    case (a:B, b:U, c:U) =>
      if (BNodeMap.contains(a)) BNodeMap(a).foreach(n => n.handle(statement))
      else unhandleBNodeTail.enqueue(statement)
    case (a:B, b:U, c:B) =>
      if (BNodeMap.contains(a)) BNodeMap(a).foreach(n => {
        n.handle(statement)
        if(BNodeMap.contains(c)) BNodeMap(c).add(n)
        else BNodeMap.put(c,mutable.Set(n))
      })
      else unhandleBNodeTail.enqueue(statement)
    case _ =>
  }

  override def startRDF(): Unit = {}

  override def endRDF(): Unit = {}

  override def handleNamespace(prefix: String, uri: String): Unit = {}

  override def handleComment(comment: String): Unit = {}

  /**
    * this call method will be called after all normal node processed
    *
    * this will bring un handle statement from the queue
    *
    * try to process it
    *
    * after once which no node has been process
    *
    * the whole program will be stop
    *
    * and all the statement left in the queue will be print
    *
    * @return the processed statements num
    */
  override def call(): Int = {

  }
}
