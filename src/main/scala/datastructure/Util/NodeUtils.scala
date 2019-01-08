package datastructure.Util

import java.io.{File, FileWriter, StringReader}

import datastructure.Obj.TypeMap.{B, TRIPLE, U}
import datastructure.Obj.{ArrayStringReader, FileSeparateIterator}
import datastructure.{Node, NodeTreeHandler, SimpleStatementHandler}
import org.apache.commons.io.input.ReaderInputStream
import org.eclipse.rdf4j.model.Statement
import org.eclipse.rdf4j.rio.{RDFFormat, RDFParseException, Rio}

import scala.collection.mutable


//0^A7cajl#Z
object NodeUtils {
  def main(args: Array[String]): Unit = {
    //    val vf = SimpleValueFactory.getInstance()

    //    val s = parseCleanQuota("<http://purl.obolibrary.org/obo/GO_0075135>              <http://www.w3.org/2000/01/rdf-schema#comment>                                                      \"Note that this term is used to annotate gene products of the symbiont. To annotate host gene products, consider the biological process term \"Negative regulation by host of symbiont calcium or calmodulin-mediated signal transduction ; GO:0075105\".\"^^<http://www.w3.org/2001/XMLSchema#string> .","1")
    //    println(s)
    //
    //    val array = Array("_:BX2D6caf4877X3A1588628cb1fX3A478f <http://www.geneontology.org/formats/oboInOwl#hasDbXref> \"GOC:TermGenie\"^^<http://www.w3.org/2001/XMLSchema#string> .","_:BX2D6caf4877X3A1588628cb1fX3A478f <http://www.w3.org/2002/07/owl#annotatedTarget> \"downregulation of centriole elongation\"^^<http://www.w3.org/2001/XMLSchema#string> .","_:BX2D6caf4877X3A1588628cb1fX3A478f <http://www.w3.org/2002/07/owl#annotatedProperty> <http://www.geneontology.org/formats/oboInOwl#hasExactSynonym> .", "_:BX2D6caf4877X3A1588628cb1fX3A478f <http://www.w3.org/2002/07/owl#annotatedSource> <http://purl.obolibrary.org/obo/GO_1903723> .","_:BX2D6caf4877X3A1588628cb1fX3A478f <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Axiom> .")
    ////    var c = Array((vf.createIRI("<http://gcm.wdcm.org/data/gcmAnnotation1/enzyme/1.5.1.17>"), vf.createIRI("http://gcm.wdcm.org/ontology/gcmAnnotation/v1/substrate1"), vf.createLiteral("lit1")),
    //      (vf.createIRI("<http://gcm.wdcm.org/data/gcmAnnotation/enzyme/1.5.1.17>"), vf.createIRI("http://gcm.wdcm.org/ontology/gcmAnnotation/v1/substrate2"), vf.createLiteral("lit2")),
    //      (vf.createIRI("<http://gcm.wdcm.org/data/gcmAnnotation/enzyme/1.5.1.17>"), vf.createIRI("http://gcm.wdcm.org/ontology/gcmAnnotation/v1/substrate2"), vf.createLiteral("lit4"))
    //      //        (vf.createIRI("http://gcm.wdcm.org/data/gcmAnnotation1/enzyme/1.5.1.17"), vf.createIRI("http://gcm.wdcm.org/ontology/gcmAnnotation/v1/substrate3"), vf.createLiteral("lit3")),
    //      //        (vf.createIRI("http://gcm.wdcm.org/data/gcmAnnotation1/enzyme/1.5.1.17"), vf.createIRI("http://gcm.wdcm.org/ontology/gcmAnnotation/v1/x-pathway"), vf.createIRI("http://gcm.wdcm.org/data/gcmAnnotation1/enzyme/1.5.1.18")),
    //      //        (vf.createIRI("http://gcm.wdcm.org/data/gcmAnnotation1/enzyme/1.5.1.17"), vf.createIRI("http://gcm.wdcm.org/ontology/gcmAnnotation/v1/x-pathway"), vf.createBNode("_:fuckyouannomynousnode")),
    //      //          (vf.createIRI("<http://gcm.wdcm.org/data/gcmAnnotation1/enzyme/1.5.1.17>"), vf.createIRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), vf.createIRI("http://gcm.wdcm.org/ontology/gcmAnnotation/v1/EnzymeNode")),
    //      //          (vf.createIRI("<http://gcm.wdcm.org/data/gcmAnnotation1/enzyme/1.5.1.17>"), vf.createIRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), vf.createIRI("http://gcm.wdcm.org/ontology/gcmAnnotation/v1/NameNode"))
    //    )
    //    var n = buildNode(c.toIterable)
    //
    //    val s = buildCSVPerNode(n, mutable.Map("substrate2" -> true, "test" -> false, "dopeArray" -> true, "dopeArray2" -> false))
    //    println(n.getAllLabel)
    //    println(n.toString)
    //    println(s)
    ////
    ////    val stmt = toStatementWithNoDirective(SparkUtils.parseCleanQuota("<http://gcm.wdcm.org/data/gcmAnnotation/protein/B9KXQ1_THERP> <http://gcm.wdcm.org/ontology/gcmAnnotation/v1/function> \"Required for rescue of stalled ribosomes mediated by trans-translation. Binds to transfer-messenger RNA (tmRNA), required for stable association of tmRNA with ribosomes. tmRNA and SmpB together mimic tRNA shape, replacing the anticodon stem-loop with SmpB. tmRNA is encoded by the ssrA gene; the 2 termini fold to resemble tRNA(Ala) and it encodes a \"tag peptide\", a short internal open reading frame. During trans-translation Ala- aminoacylated tmRNA acts like a tRNA, entering the A-site of stalled ribosomes, displacing the stalled mRNA. The ribosome then switches to translate the ORF on the tmRNA; the nascent peptide is terminated with the \"tag peptide\" encoded by the tmRNA and targeted for degradation. The ribosome is freed to recommence translation, which seems to be the essential function of trans- translation. {ECO:0000256|HAMAP-Rule:MF_00023}. Required for rescue of stalled ribosomes mediated by trans-translation. Binds to transfer-messenger RNA (tmRNA), required for stable association of tmRNA with ribosomes. tmRNA and SmpB together mimic tRNA shape, replacing the anticodon stem-loop with SmpB. tmRNA is encoded by the ssrA gene; the 2 termini fold to resemble tRNA(Ala) and it encodes a 'tag peptide', a short internal open reading frame. During trans-translation Ala- aminoacylated tmRNA acts like a tRNA, entering the A-site of stalled ribosomes, displacing the stalled mRNA. The ribosome then switches to translate the ORF on the tmRNA; the nascent peptide is terminated with the 'tag peptide' encoded by the tmRNA and targeted for degradation. The ribosome is freed to recommence translation, which seems to be the essential function of trans- translation. {ECO:0000256|SAAS:SAAS00308991}.\" .", "\'"), new ArrayBuffer[String]())
    ////    println(stmt)
    //    val temp = "http://gcm.wdcm.org/data/gcmAnnotation1/enzyme/1.5.1.17"
    //    print(nodeIdRepair(temp))
    val file = new FileSeparateIterator(List(new File("C:\\Users\\coco1\\IdeaProjects\\SeqN3CSVProcessor\\src\\main\\scala\\datastructure\\go.n3")), 1)
    val rdfParser = Rio.createParser(RDFFormat.N3)
    val handler = new NodeTreeHandler
    import scala.collection.JavaConversions._
    for (f <- file) {
      //p.map(parseCleanQuota(_, "\'")).map(NodeUtils.nodeIdRepair).iterator), ""
      val reader = new ArrayStringReader(f.map(SparkUtils.parseCleanQuota(_, "\'")).map(NodeUtils.nodeIdRepair).iterator)
      rdfParser.setRDFHandler(handler)
      try {
        rdfParser.parse(reader, "")
      }
      catch {
        case e: Exception => e.printStackTrace()
      }
      val q = handler.fileStatementQueue.toList
      val qs = q.map(f => (f.getSubject, f)).groupBy(_._1).values
      for (lines <- qs) yield {
        val node = NodeUtils.buildNodeByStatement(lines.map(_._2))
        node.getLabel
        if (node.isInitLabel) {
          println(node)
          println(node.getNodeRelation)
        }
      }
    }
  }


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

  //reduce left is empty
  def buildCSVPerNodeMayEmpty(node: Node, schema: mutable.Map[String, Boolean]): String = {
    if (schema.keySet.isEmpty) node.getId + "," + node.getAllLabel
    else buildCSVPerNode(node, schema)
  }

  def toStatementWithNoDirective(str: String, log: mutable.ArrayBuffer[String]): Statement = {
    val parser = Rio.createParser(RDFFormat.N3)
    val simpleStatementHandler = new SimpleStatementHandler
    parser.setRDFHandler(simpleStatementHandler)
    try {
      parser.parse(new ReaderInputStream(new StringReader(str), java.nio.charset.Charset.defaultCharset()), "")
    } catch {
      case e: RDFParseException => println(str + "got a wrong format"); log +: (str + "got a wrong format"); log :+ e.getMessage
    }
    simpleStatementHandler.statement
  }

  def buildNode(iter: Iterable[TRIPLE]): Node = {
    var n: Node = null
    for (triple <- iter) {
      if (n == null)
        n = Node.instanceOf(triple._1, triple._2, triple._3)
      else n.handle(triple._1, triple._2, triple._3)
    }
    n
  }

  def buildNodeByStatement(iter: Iterable[Statement]): Node = {
    var n: Node = null
    for (triple <- iter) {
      if (n == null)
        n = Node.build(triple)
      else n.handle(triple)
    }
    n
  }

  def nodeIdRepair(iri: String): String = {
    if (iri.contains("/gcmAnnotation/"))
      iri.replaceAll("/gcmAnnotation/", "/gcmAnnotation1/")
    else iri
  }
  def buildCSVPerNode(node: Node, schema : mutable.Map[String, Boolean]) : String = {
    val array = new mutable.ArrayBuffer[String]()
    for (key <- schema.keySet) {
      val set = node
        .getProp(key)
        .map(_.replace(",", " "))
      var str =
        if (set.size > 1) set.reduce(_ + ";" + _)
        else set.head
      array += str
    }
    val ret = try {
      array.reduce(_ + "," + _)
    } catch {
      case e: UnsupportedOperationException =>
        println(node.toString)
        throw e
      case e: Exception =>
        println(e.getMessage)
        throw e
    }
    node.getId + s",$ret," + node.getAllLabel
  }

  def bNodeBiFilter(nodeArray : Array[Statement]) : (mutable.Queue[Statement], mutable.Queue[Statement]) = {
    val bubQueue : mutable.Queue[Statement] = new mutable.Queue[Statement]()
    val btailQueue : mutable.Queue[Statement] = new mutable.Queue[Statement]()
    nodeArray.foreach(s => (s.getSubject, s.getPredicate, s.getObject) match {
      case (a : B ,b : U, c : B) => bubQueue.enqueue(s)
      case _ => btailQueue.enqueue(s)
    })
    (bubQueue, btailQueue)
  }

  def writeFile(ls : Array[String], append : Boolean, outputPath : String, outputName : String) : Unit = {
    val f: File = new File(outputPath)
    if (!f.exists()) f.mkdirs()
    val out = new FileWriter(outputPath + outputName,append)
    for (i <- ls) out.write(i + "\r\n")
    out.close()
  }

  /**
    * @SideEffect this one got serious side effect
    * @param bubQueue a queue only contain BUB formal statement
    * @param nodeMap str -> Node, a pair of id and node
    * @param bnodeMap B -> Set[String] a pair of BNode and id
    */
  def processBUBNode (bubQueue : mutable.Queue[Statement], nodeMap : Map[String, Node], bnodeMap : mutable.HashMap[B, mutable.Set[String]]) : Unit = {
    val count = bubQueue.size
    var counter = 0
    while (bubQueue.nonEmpty && counter * 2 == count) {
      val s = bubQueue.dequeue()
      val stat = s.getSubject.asInstanceOf[B]
      if (bnodeMap.contains(stat)) {
        val set = bnodeMap(stat)
        val obj = s.getObject.asInstanceOf[B]
        for (id <- set) {
          nodeMap(id).handle(s)
          if (bnodeMap.contains(obj)) bnodeMap(obj).add(id)
          else bnodeMap.put(obj, mutable.HashSet(id))
        }
      }
      else bubQueue.enqueue(s)
      counter += 1
    }
  }

  /**
    * @SideEffect this one got serious side effect
    * @param bnodeQueue a queue only contain *NOT* BUB formal statement
    * @param nodeMap str -> Node, a pair of id and node
    * @param bnodeMap B -> Set[String] a pair of BNode and id
    */
  def processBNode(bnodeQueue  : mutable.Queue[Statement], nodeMap : Map[String, Node], bnodeMap : mutable.HashMap[B, mutable.Set[String]]) : Unit = {
    val count = bnodeQueue.size
    var counter = 0
    while (bnodeQueue.nonEmpty && counter == count){
      val statement = bnodeQueue.dequeue()
      val bid = statement.getSubject.asInstanceOf[B]
      if (bnodeMap.contains(bid)) {
        val set = bnodeMap(bid)
        for (id <- set) {
          nodeMap(id).handle(statement)
        }
      }
      counter += 1
    }
  }

}
