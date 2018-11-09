package datastructure

import datastructure.Obj.Property
import datastructure.Obj.TypeMap.{B, L, U}
import org.eclipse.rdf4j.model._

import scala.collection.mutable

/**
  * we assuming that all node insert is in one label
  *
  * which means one file for one label
  *
  */
class NodeTree {
  val NodeMap : mutable.HashMap[IRI, Node] = new mutable.HashMap[IRI, Node]()
  val BNodeMap : mutable.HashMap[BNode, mutable.Set[Node]] = new mutable.HashMap[BNode, mutable.Set[Node]]()
  val PropNames : mutable.HashSet[Property] = new mutable.HashSet[Property]()
  val relationQueue : mutable.Queue[Statement] = new mutable.Queue[Statement]()
  val unhandleBNodeTail : mutable.Queue[Statement] = new mutable.Queue[Statement]()
  def handleStatement(statement: Statement) : Unit = {
    handleStatement(statement.getSubject, statement.getPredicate, statement.getObject, statement)
  }

  def handleStatement(subject: Resource, predicate : IRI, obj : Value, statement: Statement) : Unit = (subject, predicate, obj) match {
    case (a:B, b:U, c:L) =>
    case (a:B, b:U, c:U) =>
    case (a:B, b:U, c:B) =>
    case _ =>
  }
}
