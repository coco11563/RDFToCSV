package datastructure

import org.eclipse.rdf4j.model.Statement
import org.eclipse.rdf4j.rio.RDFHandler

class SimpleStatementHandler extends RDFHandler {
  var statement: Statement = _

  override def startRDF(): Unit = {}

  override def endRDF(): Unit = {}

  override def handleNamespace(prefix: String, uri: String): Unit = {}

  override def handleStatement(st: Statement): Unit = {
    this.statement = st
  }

  override def handleComment(comment: String): Unit = {}
}
