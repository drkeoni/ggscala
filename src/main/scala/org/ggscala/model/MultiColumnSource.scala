/*
 * Grammar of Graphics in Scala
 * Copyright (c) 2011, ggscala.org
 */
package org.ggscala.model

import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import org.ggscala.model.Factor._
import org.ggscala.model.TypeCode._

object MultiColumnSource
{
  /** Represents a data source which provides typed columns of data.
   *  Type-specific selectors allow retrieval of correctly typed
   *  Iterables corresponding to a key.
   */
  trait MultiColumnSource {
    /** select a string column */
    def $s( key:String ) : DataVector[String]
    /** select a double column */
    def $d( key:String ) : DataVector[Double]
    /** select a factor column */
    def $f( key:String ) : DataVector[Factor]
    /** provide the list of keys known to this column source. */
    def names : Seq[String]
    /** Subclasses can override to provide the total number of columns */
    def ncol = 0
    
  }
  
  // TODO: still playing with the type signature here
  
  /** MultiColumnSources which can be row-concatenated with other MultiColumnSources */
  trait RowBindable[T <: MultiColumnSource] extends MultiColumnSource {
    def rbind( d:T ) : RowBindable[T]
  }
  
  /** MultiColumnSources which can be column-concatenated with other MultiColumnSources */
  trait ColumnBindable extends MultiColumnSource {
    def cbind( d:ColumnBindable ) : ColumnBindable
  }
  
  /** Convenient base class for MultiColumnSources that wrap other MultiColumnSources.
   *  All methods on MultiColumnSource are delegated to the underlying source.
   *  The underlying data source is lazily initialized.
   */
  abstract class MultiColumnSourceDelegate extends MultiColumnSource
  {
    /** Subclasses implement to provide an underlying column source. */
    protected def columnSource : MultiColumnSource
    lazy val _columnSource = columnSource
    def $s( key:String ) = _columnSource.$s(key)
    def $d( key:String ) = _columnSource.$d(key)
    def $f( key:String ) = _columnSource.$f(key)
    def names = _columnSource.names
    override def ncol = _columnSource.ncol
  }
  
  trait DataVector[+T <: Any] extends Iterable[T]
  
  /** A DataVector which is backed by a fully instantiated Array */
  class ArrayDataVector[T]( protected val values: Array[T] )
    extends DataVector[T] {
    override def iterator = values.iterator
    def toArray = values
  }
  
  class StringVector( values: Array[String] ) extends ArrayDataVector[String](values)
  
  class DoubleVector( values: Array[Double] ) extends ArrayDataVector[Double](values)
  
  def stringArrayToDataVector( values:Array[String], _type:TypeCode ) = {
    var data : DataVector[Any] = null
    _type match {
	  case StringTypeCode =>
	    data = new StringVector( values )
	  case DoubleTypeCode => 
	    data = new DoubleVector( values.map( _.toDouble ) )
	  case FactorTypeCode =>
	    data = new FactorVector( values )
    }
    data
  }
  
  def anyArrayToDataVector[_ <: Any]( values:Array[_], _type:TypeCode ) = {
    var data : DataVector[_] = null
    _type match {
	  case StringTypeCode =>
	    data = new StringVector( values.asInstanceOf[Array[String]] )
	  case DoubleTypeCode => 
	    data = new DoubleVector( values.asInstanceOf[Array[Double]] )
	  case FactorTypeCode =>
	    data = new FactorVector( values.asInstanceOf[Array[Factor]].map(_.toString) )
    }
    data
  }
  
  def value[T] : T = null.asInstanceOf[T]
}