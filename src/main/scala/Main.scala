import java.io.File

import datastructure.Util.{Neo4jUtils, NodeUtils, SparkUtils}
import org.apache.spark.{SparkConf, SparkContext}
import org.eclipse.rdf4j.rio.RDFFormat

import scala.collection.mutable

class Main {

}
object Main {
  def main(args: Array[String]): Unit = {
    if (args.length == 2) {
      val sparkConf = new SparkConf()
        .setAppName("ProcessOnRDFFFFFFF")
        .set("spark.driver.maxResultSize", "4g")
        .set("spark.default.parallelism", "32")
        .set("spark.shuffle.service.enabled", "true")
        .set("spark.shuffle.service.port", "7337")
        .set("spark.driver.extraJavaOptions", "-Xmx20480m -XX:MaxPermSize=2048m -XX:ReservedCodeCacheSize=2048m")
      //      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      //      .registerKryoClasses(new Class[]{CategorySortKey.class})
      val sc = new SparkContext(sparkConf)

      var fileList = Neo4jUtils.ls(new File(args(0)), new mutable.HashSet[String]()).filter(s => {
        s.contains(".n3")
      }).toList
      if (fileList.nonEmpty)
        SparkUtils.processParseBySpark(fileList, sc, RDFFormat.N3, args(1))
      //      println("  isData method should be carefully used\r\n  * this used generic linux shell command\n    *\n    * should be careful to use\n    *\n    * base on centOS7.5 test pass")
      //      var dataList = fileList.filter(a => FileUtils.isData(a)).toArray
      //      println("  isData method should be carefully used\r\n  * this used generic linux shell command\n    *\n    * should be careful to use\n    *\n    * base on centOS7.5 test pass")
      //      var validList = fileList.filter(a => !FileUtils.isData(a))
      //      println("uniprot contain some corrupt file, in the beta version, we use filter to ignore these files")
      //      var validList = fileList
      //      //      NodeUtils.writeFile(dataList, false, args(1), "binaryFile")
      //
      //
      //      val fileAmount = validList.size
      //      var index = 0
      //
      //      while (validList != Nil) {
      //        val filePers = Neo4jUtils.getRightPerCut(validList, 1000) //for exe 16g dri 32g -> 1000MB
      //        println(s"本次处理文件数 = $filePers")
      //        val c = validList.splitAt(filePers)
      //        validList = c._2
      //        println("*********************************************************")
      //        println(s"**************正在进行第${index + 1} 次处理*********************")
      //        println(s"**************目前处理情况: ${(index + 1) * filePers}/$fileAmount *********************")
      //        println(s"**************还需要时间: ${fileAmount / (fileAmount - (index + 1) * filePers)}/ *********************")
      //        println("*********************************************************")
      //        if (filePers > 0)
      //          SparkUtils.processParseBySpark(c._1, sc, RDFFormat.N3, args(1), index)
      //        println("*********************************************************")
      //        println(s"**************已经完成第${index + 1} 次处理*********************")
      //        println(s"**************目前剩余处理情况: ${validList.size}/$fileAmount *********************")
      //        println("*********************************************************")
      //        index += 1
      //      }


      val script = Neo4jUtils.buildImportScript(Neo4jUtils.ls(new File(args(1)), new mutable.HashSet[String]()).filter(_.contains("_ent_")).toList,
        Neo4jUtils.ls(new File(args(1)), new mutable.HashSet[String]()).filter(_.contains("_rel_")).toList,
        "graph.db", "/data/neo4j/db/databases")

      println(script)

      NodeUtils.writeFile(Array[String](script), append = false, args(1), "importScript.sh")
    }
    else {
      val script = Neo4jUtils.buildImportScript(Neo4jUtils.ls(new File(args(0)), new mutable.HashSet[String]()).filter(_.contains("_ent_")).toList,
        Neo4jUtils.ls(new File(args(0)), new mutable.HashSet[String]()).filter(_.contains("_rel_")).toList,
        "graph.db", "/data/neo4j/db/")

      println(script)

      NodeUtils.writeFile(Array[String](script), append = false, args(0), "importScript.sh")
    }
  }
}
