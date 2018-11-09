package datastructure

import org.eclipse.rdf4j.model._
import org.eclipse.rdf4j.model.vocabulary.RDF

import scala.collection.mutable

class Node(private val id : IRI, private var label : IRI) {
  /**
    * constructor for UUL and UUB or relation type's UUU
    * will build a node without the label
    * @param id
    * @return
    */
  def this(id : IRI) = this(id, null)

  private val properties = new mutable.HashMap[IRI, mutable.HashSet[String]]() // Name -> Value

  private val bNodePredicate = new mutable.HashMap[BNode, String]()

  def addProp(iri: IRI, lit : String) : Unit = {
    if (properties.contains(iri)) properties(iri).add(lit)
    else properties.put(iri, mutable.HashSet(lit))
  }

  /**
    * @param bNode inserted bNode
    * @param predicate the bNode statement's predicate
    */
  def addBNode(bNode: BNode, predicate: String) : Unit =  if (!bNodePredicate.contains(bNode)) bNodePredicate.put(bNode, predicate)

  /**
    * bNode adding method for insert bNode which have a bNode precedent(form of BUB)
    * the predicate of this bnode will be APPENDED by their precedent
    * @param precedent preceding Bnode id which HAS TO BE added before
    * @param succession succeeding Bnode id
    * @param sucPredicate succeeding Bnode predicate
    */
  def addSucceedBNodeAppend(precedent : BNode, succession : BNode, sucPredicate : String) : Unit = addBNode(succession, bNodePredicate(precedent) + sucPredicate)

  def hasLabel : Boolean = label == null

  def addLabel(label : IRI) : Unit = if (!hasLabel) this.label = label
}

object Node {
  type U = IRI
  type B = BNode
  type L = Literal
  def instance(statement: Statement) : Node = {
    instance(statement.getSubject, statement.getPredicate, statement.getObject)
  }
  // only UUL UUU UUB can create or add new node
  def instance(subject: Resource, predicate : IRI, obj : Value) : Node = (subject, predicate, obj) match {
    case (a:U, b:U, c:L) => val node = new Node(a); node.addProp(b, c.stringValue()); node
    case (a:U, b:U, c:U) =>
      if (b.equals(RDF.TYPE)) //type label
        new Node(a, c)
      else new Node(a)
    case (a:U, b:U, c:B) => val node = new Node(a); node.addBNode(c, b.getLocalName); node
    case _ => throw new IllegalArgumentException(s"the input statement with ($subject, $predicate, $obj) is not any form of (UUU, UUL, UUB), please check the handle program")
  }
}
