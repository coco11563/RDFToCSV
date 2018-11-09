package datastructure.Obj

import org.eclipse.rdf4j.model.IRI

class Property(val name : IRI, var isArray : Boolean) {
  def this (name : IRI) = this(name, false)
  def setArray() : Unit = isArray = true
}
