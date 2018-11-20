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
      .set("spark.default.parallelism", "32")
    //      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    //      .registerKryoClasses(new Class[]{CategorySortKey.class})



    val sc = new SparkContext(sparkConf)

    var fileList = Neo4jUtils.ls(new File(args(0)), new mutable.HashSet[String]()).filter(s => {
      s.contains(".n3")
    }).toList

    val fileAmount = fileList.size
    var index = 0

    while (fileList != Nil) {
      val filePers = Neo4jUtils.getRightPerCut(fileList, 1000) //for exe 16g dri 32g -> 1000MB
      println(s"本次处理文件数 = $filePers")
      val c = fileList.splitAt(filePers)
      fileList = c._2
      println("*********************************************************")
      println(s"**************正在进行第${index + 1} 次处理*********************")
      println(s"**************目前处理情况: ${(index + 1) * filePers}/$fileAmount *********************")
      println(s"**************还需要时间: ${fileAmount / (fileAmount - (index + 1) * filePers)}/ *********************")
      println("*********************************************************")
      SparkUtils.process(c._1, sc, RDFFormat.N3, args(1), index)
      println("*********************************************************")
      println(s"**************已经完成第${index + 1} 次处理*********************")
      println(s"**************目前剩余处理情况: ${fileList.size}/$fileAmount *********************")
      println("*********************************************************")
      index += 1
    }


    val script = Neo4jUtils.buildImportScript(Neo4jUtils.ls(new File("/data2/test/"), new mutable.HashSet[String]()).filter(_.contains("_ent_")).toList,
      Neo4jUtils.ls(new File("/data2/test/"), new mutable.HashSet[String]()).filter(_.contains("_rel_")).toList,
      "test.db")

    println(script)

    NodeUtils.writeFile(Array[String](script), append = false, "/data2/test/", "importScript")
  }
}
