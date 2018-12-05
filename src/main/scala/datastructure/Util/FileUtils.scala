package datastructure.Util

import java.io.{BufferedReader, File, FileReader}

import scala.sys.process._

object FileUtils {
  def main(args: Array[String]): Unit = {
    val reader = new BufferedReader(new FileReader(new File("C:\\Users\\NickXin\\OneDrive\\Doc\\scala\\SeqN3CSVProcessor\\src\\main\\GeneNode_ent_.csv")))
    val first = reader.readLine()
    println(first)
  }

  /**
    * this used generic linux shell command
    *
    * should be careful to use
    *
    * base on centOS7.5 test pass
    *
    * @param s the file path
    * @return is this path is an binary data file
    */
  @Deprecated
  def isData(s: String): Boolean = {
    val ret: Stream[String] = s"file $s" lineStream_!
    val rep = ret.toList.head
    rep == s"$s: data"
  }
}
