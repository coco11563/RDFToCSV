package datastructure.Obj

import org.eclipse.rdf4j.model._

object TypeMap {
  type U = IRI
  type B = BNode
  type L = Literal
  type TRIPLE = (Resource, IRI, Value)
  type V = Value
}
