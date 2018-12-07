package datastructure.Obj

import java.io._
import java.util.Random

import scala.collection.mutable.ArrayBuffer

class FileSeparateIterator(file: List[File], split: Int) extends Iterator[Array[String]] {
  private var reader: BufferedReader = _
  private var lineNums: Int = _
  private var curr: Int = _
  private var fileSize: Long = _
  private var inited: Boolean = false
  private var eachTimeRead: Int = _
  private var curRead: Int = 0
  private var strBuffer: String = ""
  private var buffer: Boolean = false
  private var filelist = file
  private var currentFile: File = _

  def init(): Unit = {
    assert(filelist != Nil, "input file list is empty")
    currentFile = filelist.head
    filelist = filelist.tail
    reader = new BufferedReader(new FileReader(currentFile))
    fileSize = file.head.length()
    lineNums = FileSeparateIterator.getTotalLines(currentFile)
    curr = 0
    println(s"file size is $fileSize, line num is $lineNums")
    eachTimeRead = (split / (fileSize / lineNums)).toInt + 1
    println(eachTimeRead)
    inited = true
    println(s"iterator has init to new file $currentFile")
  }

  override def next(): Array[String] = {
    curRead = 0
    var currentArrayBuffer = new ArrayBuffer[String]()
    if (!inited) init()
    var lst: String = ""
    while (hasNext && curRead < eachTimeRead) {
      if (!thisFileHasNext) nextFile()
      if (buffer) {
        buffer = false
      }
      else {
        strBuffer = reader.readLine()
      }
      val str = strBuffer
      currentArrayBuffer += str
      curr += 1
      curRead += 1
      lst = str
    }
    if (hasNext) {
      while (hasNext && !buffer) {
        if (!thisFileHasNext) nextFile()
        strBuffer = reader.readLine()
        if (strBuffer.split(" ")(0) == lst.split(" ")(0)) {
          currentArrayBuffer += strBuffer
          curr += 1
          curRead += 1
        } // same
        else {
          buffer = true
        }
      }
    }
    currentArrayBuffer.toArray
  }

  def nextFile(): Unit = {
    currentFile = filelist.head
    filelist = filelist.tail
    reader = new BufferedReader(new FileReader(currentFile))
    fileSize = currentFile.length()
    lineNums = FileSeparateIterator.getTotalLines(currentFile)
    curr = 0
    println(s"iterator has switch to new file $currentFile")
    println(s"file size is $fileSize, line num is $lineNums")
    eachTimeRead = (split / (fileSize / lineNums)).toInt + 1
    println(s"each time we read $eachTimeRead lines")
  }

  override def hasNext: Boolean = {
    if (!inited) init()
    curr < lineNums || filelist != Nil
  }

  def thisFileHasNext: Boolean = {
    if (!inited) init()
    curr < lineNums
  }
}

object FileSeparateIterator {
  def getTotalLines(file: File): Int = {
    val in = new FileReader(file)
    val reader = new LineNumberReader(in)
    reader.skip(java.lang.Long.MAX_VALUE)
    val lines = reader.getLineNumber
    reader.close()
    lines
  }

  def main(args: Array[String]): Unit = {
    val f = new File("C:\\Users\\coco1\\IdeaProjects\\SeqN3CSVProcessor\\src\\main\\scala\\hosts")
    val h2 = new File("C:\\Users\\coco1\\IdeaProjects\\SeqN3CSVProcessor\\src\\main\\scala\\h2")
    val reader = new FileSeparateIterator(List(f, h2), 0)
    reader.init()
    var count = 0
    var ran = new Random
    while (reader.hasNext) {
      val r = reader.next()
      import scala.collection.JavaConversions._
      val arrayReader = new PushbackReader(new ArrayStringReader(r.iterator))
      var flag = true
      while (flag) {
        var e = arrayReader.read()
        if (e == -1)
          flag = false
        if (ran.nextBoolean() && flag) {
          arrayReader.unread(e)
          e = arrayReader.read()
        }
        print(e.toChar)
      }
      println()
      println("*********************************")
    }
  }
}
