import java.io.File

import datastructure.Util.{Neo4jUtils, NodeUtils}

import scala.collection.mutable

class ScriptMain {

}

object ScriptMain {
  def main(args: Array[String]): Unit = {
    val script = Neo4jUtils.buildImportScript(Neo4jUtils.ls(new File(args(0)), new mutable.HashSet[String]()).filter(_.contains("_ent_")).toList,
      Neo4jUtils.ls(new File(args(0)), new mutable.HashSet[String]()).filter(_.contains("_rel_")).toList,
      "graph.db", "/data/neo4j/db/")

    println(script)

    NodeUtils.writeFile(Array[String](script), append = false, args(1), "importScript.sh")
  }
}