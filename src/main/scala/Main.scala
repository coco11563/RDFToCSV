import java.io.{File, FileReader}

import datastructure.NodeTreeHandler
import org.eclipse.rdf4j.rio.{RDFFormat, RDFParser, Rio}

class Main {

}
object Main {
  def main(args: Array[String]): Unit = {
    val rdfParser = Rio.createParser(RDFFormat.N3)
    for (i <- 19 to 22) {
      var handler = new NodeTreeHandler
      rdfParser.setRDFHandler(handler)
      rdfParser.parse(new FileReader(new File(s"C:\\Users\\NickXin\\OneDrive\\Doc\\scala\\SeqN3CSVProcessor\\src\\main\\scala\\res\\taxonomy$i.n3")), "")
      //    handler.NodeMap.keySet.foreach(iri => println(iri.getLocalName))
      handler.PropNames.keySet().forEach(
        i => println(i + "::" + handler.PropNames.get(i))
      )
    }
  }
}
