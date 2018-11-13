
import java.io.{File, FileReader}

import datastructure.NodeTreeHandler
import datastructure.Util.NodeUtils
import org.eclipse.rdf4j.rio.{RDFFormat, Rio}

class Main {

}
object Main {
  def main(args: Array[String]): Unit = {
    val rdfParser = Rio.createParser(RDFFormat.N3)

    var handler = new NodeTreeHandler
    println("done init")
    rdfParser.setRDFHandler(handler)
    rdfParser.parse(new FileReader(new File(s"C:\\Users\\NickXin\\OneDrive\\Doc\\scala\\SeqN3CSVProcessor\\src\\main\\scala\\res\\testonomy.n3")), "")
    println("done resolve")

//    handler.NodeMap.keySet.forEach(iri => println(iri.getLocalName))
    val nodes = handler.getAllNode
    //1.buildLabelNodeMap
    import scala.collection.JavaConverters._
    val nodeMap = NodeUtils.buildLabelNodeMap(nodes.iterator().asScala)
    nodeMap.keySet.foreach(v => println(v))
    nodeMap.values
    //2.generateSchema

    //3.stringSchema

    //4.buildCSVPerNode

    //5.buildCSV

    handler.DEFAULT.shutdown()
    }

}
