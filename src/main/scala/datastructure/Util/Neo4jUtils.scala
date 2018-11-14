package datastructure.Util

import java.io.File

import scala.collection.mutable

object Neo4jUtils {
  def buildImportScript(csvEntityPath: List[String], csvRelPath: List[String], dbName: String): String = {
    val ent = csvEntityPath.map("--nodes " + _).reduce(_ + " " + _)
    val rel = "--relationships " + csvRelPath.reduce(_ + "," + _)
    s"neo4j-import --into $dbName $ent $rel --id-type string --ignore-empty-string true --bad-tolerance true --high-io --max-memory=100G "
  }

  def ls(file: File, resultFileName: mutable.HashSet[String]): mutable.HashSet[String] = {
    val files = file.listFiles
    if (files == null) return resultFileName // 判断目录下是不是空的
    for (f <- files) {
      if (f.isDirectory) { // 判断是否文件夹
        resultFileName.add(f.getPath)
        ls(f, resultFileName) // 调用自身,查找子目录
      }
      else resultFileName.add(f.getPath)
    }
    resultFileName
  }

  def main(args: Array[String]): Unit = {
    //    val li = ls("C:\\Users\\NickXin\\OneDrive\\Doc\\scala\\SeqN3CSVProcessor\\src\\main\\scala\\datastructure", recursive = true, "(\\w+)[4]{1}(\\w+).scala")
    //    println(buildImportScript(li,li,"test"))
  }
}
