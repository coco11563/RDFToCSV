import datastructure.Util.SparkUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.eclipse.rdf4j.rio.RDFFormat

class Main {

}
object Main {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("ProcessOnRDFFFFFFF")
      .set("spark.driver.maxResultSize", "4g")
    val sc = new SparkContext(sparkConf)
    SparkUtils.process(args(0), sc, RDFFormat.N3)
  }
}
