package datastructure

import java.util
import java.util.concurrent._

import datastructure.Obj.TypeMap.{B, L, U}
import datastructure.Util.ThreadUtils._
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
  val NodeMap : ConcurrentHashMap[IRI, Node] = new ConcurrentHashMap[IRI, Node]()
  val BNodeMap : ConcurrentHashMap[BNode, mutable.Set[Node]] = new ConcurrentHashMap[BNode, mutable.Set[Node]]()
  val relationQueue : mutable.Queue[Statement] = new mutable.Queue[Statement]()
  val unHandleBNodeTail : mutable.Queue[Statement] = new mutable.Queue[Statement]()
  lazy val DEFAULT: ThreadPoolExecutor = createDefaultPool

  override def handleStatement(statement: Statement) : Unit = {
//    val timer = System.currentTimeMillis()
    handleStatement(statement.getSubject, statement.getPredicate, statement.getObject, statement)
//    println(s"handle one statement in ${System.currentTimeMillis() - timer}")
  }

  /**
    * handle all the statement
    *
    * @param subject the ori statement's subject (Value)
    * @param predicate the ori statement's predicate URI, in rdf4j it should be a IRI (inhere from Resource)
    * @param obj the ori statement's object value (Value)
    * @param statement the origin statement
    */
  // Node map -> get   BNodeMap -> write and get
  def handleStatement(subject: Resource, predicate : IRI, obj : Value, statement: Statement) : Unit = (subject, predicate, obj) match {
    case (a:U, b:U, c:L) =>
      if (NodeMap.contains(a)) NodeMap.get(a).handle(statement)
      else NodeMap.put(a, Node.build(statement))
    case (a:U, b:U, c:U) =>
      if (NodeMap.contains(a)) NodeMap.get(a).handle(statement)
      else NodeMap.put(a, Node.build(statement))
    case (a:U, b:U, c:B) =>
      val node : Node =
        if (NodeMap.contains(a)) {
          val n = NodeMap.get(a)
          n.handle(statement)
          n
        }
      else {
        val n = Node.build(statement)
        NodeMap.put(a, n)
        n
      }
      if (BNodeMap.contains(c)) BNodeMap.get(c).add(node)
      else BNodeMap.put(c, mutable.Set(node))
    case (a:B, b:U, c:L) =>
      if (BNodeMap.contains(a)) BNodeMap.get(a).foreach(n => n.handle(statement))
      else unHandleBNodeTail.enqueue(statement)
    case (a:B, b:U, c:U) =>
      if (BNodeMap.contains(a)) BNodeMap.get(a).foreach(n => n.handle(statement))
      else unHandleBNodeTail.enqueue(statement)
    case (a:B, b:U, c:B) =>
      if (BNodeMap.contains(a))
        BNodeMap.get(a).foreach(n => {
          n.handle(statement)
          if(BNodeMap.contains(c)) BNodeMap.get(c).add(n)
          else BNodeMap.put(c,mutable.Set(n))
        })
      else unHandleBNodeTail.enqueue(statement)
    case _ =>
  }

  override def startRDF(): Unit = {
    println("start parser")
  }

  override def endRDF(): Unit = {
    println("start to clean the queue")
    val nums = intoFuture[Int](DEFAULT,this)
    println(nums.get())
  }

  override def handleNamespace(prefix: String, uri: String): Unit = {}

  override def handleComment(comment: String): Unit = {}

  def getAllNode: util.Collection[Node] = NodeMap.values()
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
    println("start to clean the queue")
    var count = 0 // processed statements count
    var perCount = 0 // this iterate's process statements count
    var done = false
    while (unHandleBNodeTail.nonEmpty && !done) {
      val statements : mutable.Seq[Statement] = unHandleBNodeTail.dequeueAll(_ => true) // dequeue all
      statements.foreach(
        statement => {
//          println(statement.getSubject.stringValue())
          val subject = statement.getSubject
          val obj = statement.getObject
          val predicate = statement.getPredicate
          (subject, obj, predicate) match {
            case (a:B, b:U, c:L) =>
              if (BNodeMap.contains(a)) {BNodeMap.get(a).foreach(n => n.handle(statement)) ; count += 1; perCount += 1}
              else unHandleBNodeTail.enqueue(statement)
            case (a:B, b:U, c:U) =>
              if (BNodeMap.contains(a)) {BNodeMap.get(a).foreach(n => n.handle(statement)) ; count += 1; perCount += 1}
              else unHandleBNodeTail.enqueue(statement)
            case (a:B, b:U, c:B) =>
              if (BNodeMap.contains(a)) BNodeMap.get(a).foreach(n => {
                n.handle(statement)
                if(BNodeMap.contains(c)) BNodeMap.get(c).add(n)
                else BNodeMap.put(c,mutable.Set(n))
                count += 1; perCount += 1
              })
              else unHandleBNodeTail.enqueue(statement)
            case _ => throw new IllegalStateException(s"you have a wrong input will we empty the queue")
          }
        })
      if (perCount == 0) done = true
      else perCount = 0
    }
    count
  }
}
