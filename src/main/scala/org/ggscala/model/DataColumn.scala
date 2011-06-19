/*
 * Grammar of Graphics in Scala
 * Copyright (c) 2011, ggscala.org
 */
package org.ggscala.model

import org.ggscala.model.TypeCode._
import org.ggscala.model.Factor._

object DataColumn {

  /**
   * Associates a name (id) and type code with a DataVector.  Provides 
   * column metadata + data for MultiColumnSources.
   */
  trait DataColumn
  {
    var id : String
    val _type : TypeCode
    var data : DataVector[Any]
  }
  
  /**
   * An iterable sequence of arbitrary type backing each of the columns
   * provided by a MultiColumnSource
   */
  trait DataVector[+T <: Any] extends Iterable[T]
  {
    type DataType
    /** Concatenate a data vector with this data vector. */
    def cbind( data:DataVector[DataType] )( implicit ev:ClassManifest[DataType] ) : DataVector[DataType]
  }
  
  /** A DataVector which wraps an Iterable */
  class IterableDataVector[T]( protected val values:Iterable[T] ) extends DataVector[T] {
    protected def factory( vals:Iterable[DataType] ) : DataVector[DataType] = new IterableDataVector(vals)
    override type DataType = T
    override def iterator = values.iterator
    def cbind( data:DataVector[DataType] )( implicit ev:ClassManifest[DataType] ) = 
      factory( values.map(_.asInstanceOf[DataType]) ++ data.iterator )
  }
  
  /** A DataVector which is backed by a fully instantiated Array */
  class ArrayDataVector[T]( protected val values: Array[T] ) extends DataVector[T] {
    override type DataType = T
    protected def factory( vals:Array[DataType] ) : DataVector[DataType] = new ArrayDataVector(vals)
    override def iterator = values.iterator
    def toArray = values
    // I'm positive this horrific bit of casting can be avoided, but I haven't been patient
    // enough to work out the right type signatures
    // but what this means in the end is that a StringVector can cbind a StringVector and produce a StringVector
    // ...and no new code is written
    def cbind( data:DataVector[DataType] )( implicit ev:ClassManifest[DataType] ) = 
      factory( Array.concat( values.asInstanceOf[Array[DataType]], data.asInstanceOf[ArrayDataVector[DataType]].values ) )
  }
  
  /** A specialization of DataVector for String. */
  class StringVector( values: Array[String] ) extends ArrayDataVector[String](values)
  {
    override type DataType = String
    protected override def factory( vals:Array[DataType] ) : DataVector[DataType] = new StringVector(vals)
  }
  
  /** A specialization of DataVector for Double. */
  class DoubleVector( values: Array[Double] ) extends ArrayDataVector[Double](values)
  {
    override type DataType = Double
    protected override def factory( vals:Array[DataType] ) : DataVector[DataType] = new DoubleVector(vals)
  }
  
  /** Attempts to unmarshal a string array into a DataVector of the requested type */
  def stringArrayToDataVector( values:Array[String], _type:TypeCode ) = _type match {
    case StringTypeCode => new StringVector( values )
    case DoubleTypeCode => new DoubleVector( values.map( _.toDouble ) )
    case FactorTypeCode => new FactorVector( values ) 
  }
  
  def anyArrayToDataVector[_ <: Any]( values:Array[_], _type:TypeCode ) = _type match {
    case StringTypeCode => new StringVector( values.asInstanceOf[Array[String]] )
    case DoubleTypeCode => new DoubleVector( values.asInstanceOf[Array[Double]] )
    case FactorTypeCode => new FactorVector( values.asInstanceOf[Array[Factor]].map(_.toString) )
  }
}