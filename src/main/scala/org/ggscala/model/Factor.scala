/*
 * Grammar of Graphics in Scala
 * Copyright (c) 2011, ggscala.org
 */
package org.ggscala.model

import scala.collection.mutable.HashMap

import org.ggscala.model.DataColumn.DataVector

object Factor 
{ 
  /** A FactorFactory manages a set of Factor flyweights */
  trait FactorFactory {
    def factorToString( id:Int ) = ""
  }
  
  /**
   * Compactly represents a vector of strings which are members of a set (will be efficient
   * if the set size << vector size)
   */
  class FactorVector( values : Array[String] ) 
    extends DataVector[Factor] 
    with FactorFactory
  {
    override type DataType = Factor
    // map strings to the underlying Factor flyweights
    private val str2factor = new HashMap[String,Factor]
    // map ids to the string labels
    private val id2str = new HashMap[Int,String]
    // maintains current ID for factor factory work
    private var currentId = 0
    val _values = values.map(toFactor)
    
    private def toFactor( s:String ) =
      if ( str2factor contains s )
        str2factor(s)
      else
      {
        val f = new Factor(currentId,this)
        str2factor( s ) = f
        id2str( currentId ) = s
        currentId += 1
        f
      }
    
    /**
     * When concatenating two vectors of factors we marshal back to string and 
     * then back to factors to normalize the factors (simple appending wouldn't work).
     */
    def cbind( data:DataVector[DataType] )( implicit ev:ClassManifest[DataType] ) =
      new FactorVector( (_values.iterator ++ data.iterator).toArray.map( _.toString ) )
    
    override def factorToString(id:Int) = id2str(id)
    override def iterator = _values.iterator
    def toArray = _values
  }
  
  class Factor( id:Int, parent:FactorFactory )
  {
    override def toString = parent.factorToString(id)
    override def hashCode = id
    override def equals(that:Any) = 
      that.isInstanceOf[Factor] && that.asInstanceOf[Factor].hashCode == hashCode
    def toInt = id
  }
}