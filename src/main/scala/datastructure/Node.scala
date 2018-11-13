package datastructure

import datastructure.Obj.TypeMap.{B, L, U}
import org.eclipse.rdf4j.model._
import org.eclipse.rdf4j.model.vocabulary.RDF

import scala.collection.mutable
class Node(private val id : U, private var label : U) extends Serializable {
  /**
    * constructor for UUL and UUB or relation type's UUU
    * will build a node without the label
    * @param id you know what id means
    * @return
    */
  private def this(id : U) = this(id, null)

  private val properties = new mutable.HashMap[String, mutable.HashSet[String]]() // Name -> Value

  private val bNodePredicate = new mutable.HashMap[B, String]()

  private val bNodeRelation = new mutable.HashSet[(String, U, U)]()

  private val nodeRelation = new mutable.HashSet[(String, U, U)]()

  val propSet : mutable.HashMap[String, Boolean] = new mutable.HashMap[String, Boolean]()

  private def addProp(iri: String, lit : String) : Unit = {
    if (properties.contains(iri)) {
      val s = properties(iri)
      if (!s.contains(lit)){
        s.add(lit)
        propSet.update(iri, true)
      }
    }
    else {
      properties.put(iri, mutable.HashSet(lit))
      propSet.put(iri, false)
    }
  }

  /**
    * @param bNode inserted bNode
    * @param predicate the bNode statement's predicate
    */
  private def addBNode(bNode: B, predicate: String) : Unit =  if (!bNodePredicate.contains(bNode)) bNodePredicate.put(bNode, predicate)

  /**
    *
    * @param predicate the relation name
    * @param id point id
    */
  private def addBNodeRelation (predicate : String, id : U): Boolean = bNodeRelation.add((predicate, this.id, id))

  private def addNodeRelation (predicate : String, id : U) : Boolean = nodeRelation.add((predicate, this.id, id))
  /**
    * bNode adding method for insert bNode which have a bNode precedent(form of BUB)
    * the predicate of this bnode will be APPENDED by their precedent
    * @param precedent preceding Bnode id which HAS TO BE added before
    * @param succession succeeding Bnode id
    * @param sucPredicate succeeding Bnode predicate
    */
  private def addSucceedBNodeAppend(precedent : B, succession : B, sucPredicate : String) : Unit = addBNode(succession, bNodePredicate(precedent) + sucPredicate)

  private def getBNodePredicate(B : B) : String = bNodePredicate(B)

  private def hasLabel : Boolean = label != null

  private def addLabel(label : U) : Unit = if (!hasLabel) this.label = label

  def getProp(predicate : String) : mutable.HashSet[String] = {
    if (properties.contains(predicate))
      properties(predicate)
    else mutable.HashSet[String]("")
  }

  /**
    * handle all input statement, with the structure of the statement , another private function will handle the tuple
    * @param statement input statement
    */
  def handle(statement: Statement) : Unit = {
//    println(statement)
    handle(statement.getSubject, statement.getPredicate, statement.getObject)
  }

  def handle(subject: Resource, predicate : IRI, obj : Value) : Unit = (subject, predicate, obj) match {
    case (a:U, b:U, c:L) => addProp(b.getLocalName, c.stringValue()) // handle literal
    case (a:U, b:U, c:U) => if (b.equals(RDF.TYPE)) addLabel(c) else addNodeRelation(b.getLocalName, c)//handle label
    case (a:U, b:U, c:B) => addBNode(c, b.getLocalName) // handle this by adding
    case (a:B, b:U, c:L) => addProp(getBNodePredicate(a) + ":"+ b.getLocalName, c.stringValue()) // attach this
    case (a:B, b:U, c:U) => if (!b.equals(RDF.TYPE)) addBNodeRelation(getBNodePredicate(a) + ":"+ b.getLocalName, c) // discard the blank node label
    case (a:B, b:U, c:B) => addBNode(c, getBNodePredicate(a) + ":" + b.getLocalName)
    case _ =>
      throw new IllegalArgumentException(s"the input statement with ($subject, $predicate, $obj) is not any form of (UUU, UUL, UUB), please check the handle program")
  }

  def getLabel : String = if (hasLabel) label.getLocalName else "NONE"
  def getPropSet : mutable.HashMap[String, Boolean] = this.propSet
  def getId : String = this.id.stringValue()
}

object Node {

  def build(statement: Statement) : Node = {
//    println(statement)
    instanceOf(statement.getSubject, statement.getPredicate, statement.getObject)
  }
  // only UUL UUU UUB can create or add new node
  def instanceOf(subject: Resource, predicate : IRI, obj : Value) : Node = (subject, predicate, obj) match {
    case (a:U, b:U, c:L) => val node = new Node(a); node.addProp(b.getLocalName, c.stringValue()); node
    case (a:U, b:U, c:U) =>
      if (b.equals(RDF.TYPE)) //type label
        new Node(a, c)
      else new Node(a)
    case (a:U, b:U, c:B) => val node = new Node(a); node.addBNode(c, b.getLocalName); node
    case _ => throw new IllegalArgumentException(s"the input statement with ($subject, $predicate, $obj) is not any form of (UUU, UUL, UUB), please check the handle program")
  }
}
