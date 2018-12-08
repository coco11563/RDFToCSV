package datastructure

import java.util.regex.{Matcher, Pattern}

import datastructure.Obj.TypeMap.{L, U, V}
import org.eclipse.rdf4j.model._
import org.eclipse.rdf4j.model.vocabulary.RDF

import scala.collection.mutable

class Node(private val id: String) extends Serializable {
  /**
    * constructor for UUL and UUB or relation type's UUU
    * will build a node without the label
    * @param id you know what id means
    * @return
    */
  private def this(id: V) = this(id.stringValue())

  private var label: String = _

  private val properties = new mutable.HashMap[String, mutable.HashSet[String]]() // Name -> Value
  private val idLabelRegex: Pattern = Pattern.compile("(?<prefix>http:\\/\\/[^>]+\\/(?<label>[^\\/]+)\\/)")
  private val secondLabels : mutable.HashSet[String] = new mutable.HashSet[String]()
//  @Deprecated
//  private val bNodePredicate = new mutable.HashMap[B, String]()
//  @Deprecated
//  private val bNodeRelation = new mutable.HashSet[(String, U, U)]()

  private val nodeRelation = new mutable.HashSet[(String, String, String)]()

  val propSet : mutable.HashMap[String, Boolean] = new mutable.HashMap[String, Boolean]()
  private def addProp(iri: String, lit : String) : Unit = {
    val literal = lit.replaceAll("\"", " ").replaceAll("\'", " ")
    if (properties.contains(iri)) {
      val s = properties(iri)
      if (!s.contains(literal)) {
        s.add(literal)
        propSet.update(iri, true)
      }
    }
    else {
      properties.put(iri, mutable.HashSet(literal))
      propSet.put(iri, false)
    }
  }

  override def toString: String = {
    s"$id : $label , $properties"
  }

//  /**
//    * @param bNode inserted bNode
//    * @param predicate the bNode statement's predicate
//    */
//  @Deprecated
//  private def addBNode(bNode: B, predicate: String) : Unit =  if (!bNodePredicate.contains(bNode)) bNodePredicate.put(bNode, predicate)
//
//  /**
//    *
//    * @param predicate the relation name
//    * @param id point id
//    */
//  @Deprecated
//  private def addBNodeRelation (predicate : String, id : U): Boolean = bNodeRelation.add((predicate, this.id, id))

  private def addNodeRelation (predicate : String, id : Value) : Boolean = nodeRelation.add((predicate, this.id, id.stringValue()))

  def getNodeRelation: List[String] = nodeRelation.map(a => a._2 + "," + a._3 + "," + a._1).toList
//  /**
//    * bNode adding method for insert bNode which have a bNode precedent(form of BUB)
//    * the predicate of this bnode will be APPENDED by their precedent
//    * @param precedent preceding Bnode id which HAS TO BE added before
//    * @param succession succeeding Bnode id
//    * @param sucPredicate succeeding Bnode predicate
//    */
//  private def addSucceedBNodeAppend(precedent : B, succession : B, sucPredicate : String) : Unit = addBNode(succession, bNodePredicate(precedent) + sucPredicate)
//
//  private def getBNodePredicate(B : B) : String = bNodePredicate(B)

  def hasLabel : Boolean = label != null

  private def addLabel(label : U) : Unit = {
    if (!hasLabel) {
      this.label = label.getLocalName
      secondLabels += label.getLocalName
    }
    else secondLabels += label.getLocalName
  }



//  def getBNodeMap : mutable.HashMap[B, String] = this.bNodePredicate

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
    case (a:V, b:U, c:L) => addProp(b.getLocalName, c.stringValue()) // handle literal
    case (a:V, b:U, c:U) => if (b.equals(RDF.TYPE)) addLabel(c) else addNodeRelation(b.getLocalName, c)//handle label
    case (a:V, b:U, c:V) => addNodeRelation(b.getLocalName, c) // handle this by adding
    case _ =>
      throw new IllegalArgumentException(s"the input statement with ($subject, $predicate, $obj) is not any form of (UUU, UUL, UUB), please check the handle program")
  }

  def getLabel: String = if (hasLabel) label else getLabelFromID

  def getAllLabel: String = {
    if (!hasLabel)
      getLabelFromID
    else
      this.secondLabels.reduce(_ + ";" + _)
  }

  def getLabelFromID: String = {
    val matcher: Matcher = idLabelRegex.matcher(id)
    if (matcher.find()) matcher.group("label")
    else "NONE"
  }
  def getPropSet : mutable.HashMap[String, Boolean] = this.propSet
  def getId : String = this.id
}

object Node {

  def build(statement: Statement) : Node = {
    instanceOf(statement.getSubject, statement.getPredicate, statement.getObject)
  }
  // only UUL UUU UUB can create or add new node
  def instanceOf(subject: Resource, predicate : IRI, obj : Value) : Node = (subject, predicate, obj) match {
    case (a:V, b:U, c:L) => val node = new Node(a); node.addProp(b.getLocalName, c.stringValue()); node
    case (a:V, b:U, c:U) =>
      if (b.equals(RDF.TYPE)) {
        //type label
        val n = new Node(a)
        n.addLabel(c)
        n
      }
      else {
        val n = new Node(a)
        n.addNodeRelation(b.getLocalName, c)
        n
      }
    case (a:V, b:U, c:V) => val node = new Node(a); node.addNodeRelation(b.getLocalName, c); node
    case _ => throw new IllegalArgumentException(s"the input statement with ($subject, $predicate, $obj) is not any form of (UUU, UUL, UUB), please check the handle program")
  }

}
