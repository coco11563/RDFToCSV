package datastructure.COC

import scala.util.Random

class cocInit {

}

object cocInit {
  val ran: Random = new Random()

  /**
    *
    * 13272轮智妈 2018/12/9 20:03:30
    * 飞行员
    * 力量为: 45
    * 体质为: 80
    * 体型为: 65
    * 敏捷为: 45
    * 外貌为: 75
    * 智力为: 75
    * 意志为: 65
    * 教育为: 80
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    println(dice(100))
    println("力量为: " + initOne(3, 6, 5))
    println("体质为: " + initOne(3, 6, 5))
    println("体型为: " + (initOne(2, 6, 5) + 30))
    println("敏捷为: " + initOne(3, 6, 5))
    println("外貌为: " + initOne(3, 6, 5))
    println("智力为: " + (initOne(2, 6, 5) + 30))

    println("意志为: " + initOne(3, 6, 5))
    println("教育为: " + (initOne(2, 6, 5) + 30))
  }

  def initOne(time: Int, diceMount: Int, rate: Int): Int = {
    val st = for (i <- 0 until time) yield {
      ran.nextInt(diceMount) + 1
    }
    val st_Total = st.sum * rate
    st_Total
  }

  def dice(mount: Int): Int = {
    ran.nextInt(mount) + 1
  }
}

