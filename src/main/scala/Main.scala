import java.io.{File, FileReader}

import datastructure.NodeTreeHandler
import datastructure.Util.NodeUtils
import org.eclipse.rdf4j.rio.{RDFFormat, Rio}
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class Main {

}
object Main {
  def main(args: Array[String]): Unit = {
    val rdfParser = Rio.createParser(RDFFormat.N3)
    for (i <- 19 to 19) {
      var handler = new NodeTreeHandler
      println("done init")
      rdfParser.setRDFHandler(handler)
      rdfParser.parse(new FileReader(new File(s"C:\\Users\\NickXin\\OneDrive\\Doc\\scala\\SeqN3CSVProcessor\\src\\main\\scala\\res\\taxonomy$i.n3")), "")
      println("done resolve")

      handler.NodeMap.keySet.forEach(iri => println(iri.getLocalName))
      val nodes = handler.NodeMap.values()
      //1.buildLabelNodeMap
      val nodeMap = NodeUtils.buildLabelNodeMap(nodes)

      nodeMap.keySet.foreach(v => println(v))

      //2.generateSchema

      //3.stringSchema

      //4.buildCSVPerNode

      //5.buildCSV
    }
  }
}
