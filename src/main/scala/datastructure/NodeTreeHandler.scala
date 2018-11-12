package datastructure

import java.util.concurrent._
import java.util.concurrent.locks.LockSupport

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
  val unHandleBNodeTail : mutable.Queue[Statement] = new mutable.Queue[Statement]()
  lazy val DEFAULT: ThreadPoolExecutor = NodeTreeHandler.createDefaultPool
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
      else unHandleBNodeTail.enqueue(statement)
    case (a:B, b:U, c:U) =>
      if (BNodeMap.contains(a)) BNodeMap(a).foreach(n => n.handle(statement))
      else unHandleBNodeTail.enqueue(statement)
    case (a:B, b:U, c:B) =>
      if (BNodeMap.contains(a)) BNodeMap(a).foreach(n => {
        n.handle(statement)
        if(BNodeMap.contains(c)) BNodeMap(c).add(n)
        else BNodeMap.put(c,mutable.Set(n))
      })
      else unHandleBNodeTail.enqueue(statement)
    case _ =>
  }

  override def startRDF(): Unit = {}

  override def endRDF(): Unit = {
    val nums = NodeTreeHandler.intoFuture[Int](DEFAULT,this)
    while (!nums.isDone) {}
    println(nums.get())
  }

  override def handleNamespace(prefix: String, uri: String): Unit = {}

  override def handleComment(comment: String): Unit = {}

  def getAllNode: Iterable[Node] = NodeMap.values
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
    var count = 0 // processed statements count
    var perCount = 0 // this iterate's process statements count
    var done = false
    while (unHandleBNodeTail.nonEmpty && !done) {
      val statements : mutable.Seq[Statement] = unHandleBNodeTail.dequeueAll(_ => true) // dequeue all
      statements.foreach(
        statement => {
          val subject = statement.getSubject
          val obj = statement.getObject
          val predicate = statement.getPredicate
          (subject, obj, predicate) match {
            case (a:B, b:U, c:L) =>
              if (BNodeMap.contains(a)) {BNodeMap(a).foreach(n => n.handle(statement)) ; count += 1; perCount += 1}
              else unHandleBNodeTail.enqueue(statement)
            case (a:B, b:U, c:U) =>
              if (BNodeMap.contains(a)) {BNodeMap(a).foreach(n => n.handle(statement)) ; count += 1; perCount += 1}
              else unHandleBNodeTail.enqueue(statement)
            case (a:B, b:U, c:B) =>
              if (BNodeMap.contains(a)) BNodeMap(a).foreach(n => {
                n.handle(statement)
                if(BNodeMap.contains(c)) BNodeMap(c).add(n)
                else BNodeMap.put(c,mutable.Set(n))
                count += 1; perCount += 1
              })
              else unHandleBNodeTail.enqueue(statement)
          }
        })
      if (perCount == 0) done = true
      else perCount = 0
    }
    count
  }
}

object NodeTreeHandler {
  def intoFuture[T](pool: ExecutorService , callable: Callable[T]): Future[T] = {
      pool.submit(() => {
        def foo() = try {
          callable.call
        }
        foo()
      })
  }
    def createDefaultPool :ThreadPoolExecutor = {
      val threads = Runtime.getRuntime.availableProcessors * 2
      val queueSize = threads * 25
      new ThreadPoolExecutor(threads / 2, threads, 30L, TimeUnit.SECONDS, new ArrayBlockingQueue[Runnable](queueSize), new CallerBlocksPolicy)
      //                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    class CallerBlocksPolicy() extends RejectedExecutionHandler {
      override def rejectedExecution(r: Runnable, executor: ThreadPoolExecutor): Unit = {
        if (!executor.isShutdown) { // block caller for 100ns
          LockSupport.parkNanos(100)
          try // submit again
          executor.submit(r).get
          catch {
            case e@(_: InterruptedException | _: ExecutionException) =>
              throw new RuntimeException(e)
          }
        }
      }
    }
}