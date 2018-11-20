package datastructure.Util

import java.io.{BufferedReader, File, FileReader}

object FileUtils {
  def main(args: Array[String]): Unit = {
    val reader = new BufferedReader(new FileReader(new File("C:\\Users\\NickXin\\OneDrive\\Doc\\scala\\SeqN3CSVProcessor\\src\\main\\GeneNode_ent_.csv")))
    val first = reader.readLine()
    println(first)
  }
}
