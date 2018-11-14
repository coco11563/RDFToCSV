import java.io.File

import datastructure.Util.{Neo4jUtils, NodeUtils, SparkUtils}
import org.apache.spark.{SparkConf, SparkContext}
import org.eclipse.rdf4j.rio.RDFFormat

import scala.collection.mutable

class Main {

}
object Main {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("ProcessOnRDFFFFFFF")
      .set("spark.driver.maxResultSize", "4g")
    val sc = new SparkContext(sparkConf)

    val fileList = Neo4jUtils.ls(new File(args(0)), new mutable.HashSet[String]()).filter(s => {
      s.contains(".n3")
    }).toList
    for (f <- fileList) {
      SparkUtils.process(f, sc, RDFFormat.N3, "/data2/test/")
    }

    val script = Neo4jUtils.buildImportScript(Neo4jUtils.ls(new File("/data2/test"), new mutable.HashSet[String]()).filter(_.contains("_ent_")).toList,
      Neo4jUtils.ls(new File("/data2/test"), new mutable.HashSet[String]()).filter(_.contains("_rel_")).toList,
      "test.db")

    println(script)

    NodeUtils.writeFile(Array[String](script), append = false, "/data2/test", "importScript")
  }
}
