package datastructure.Util

import java.io.File

import scala.collection.mutable

object Neo4jUtils {
  def buildImportScript(csvEntityPath: List[String], csvRelPath: List[String], dbName: String, outpath: String): String = {
    val ent = csvEntityPath.map("--nodes " + _).reduce(_ + " " + _)
    val rel = "--relationships " + csvRelPath.reduce(_ + "," + _)
    s"neo4j-import --into ${outpath + dbName} $ent $rel --id-type string --ignore-empty-string true --bad-tolerance true --high-io --max-memory=100G --skip-duplicate-nodes true --processors 64 "
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

  def getRightPerCut(list: List[String], amount: Long): Int = {
    var list_ = list
    var fileNum = 0l
    var count = 0
    var flag = true
    while (list_.nonEmpty && fileNum < amount) {
      fileNum += getFileSize(list_.head)
      count += 1
      list_ = list_.tail
    }
    println(s"本次处理文件大小 $fileNum MB, 共处理 $count 个文件")
    count
  }

  def getFileSize(fileName: String): Long = getFileSize(new File(fileName))
  def getFileSize(file: File): Long = {
    if (file.exists && file.isFile) {
      val fileName = file.getName
      System.out.println("文件" + fileName + "的大小是：" + file.length)
    }
    file.length() / (1024 * 1024)
  }
  def main(args: Array[String]): Unit = {
    //    val li = ls("C:\\Users\\NickXin\\OneDrive\\Doc\\scala\\SeqN3CSVProcessor\\src\\main\\scala\\datastructure", recursive = true, "(\\w+)[4]{1}(\\w+).scala")
    //    println(buildImportScript(li,li,"test"))
  }
}
