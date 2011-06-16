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
    /** select a column with any type */
    def $a( key:String ) : DataVector[Any]
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
    def rbind( d:Option[T] ) : RowBindable[T] = if ( d.isEmpty ) this; else rbind( d.get )
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
    def $a( key:String ) = _columnSource.$a(key)
    def $s( key:String ) = _columnSource.$s(key)
    def $d( key:String ) = _columnSource.$d(key)
    def $f( key:String ) = _columnSource.$f(key)
    def names = _columnSource.names
    override def ncol = _columnSource.ncol
  }
  
  /**
   * Associates a name (id) and type code with a DataVector.  Useful in
   * implementing MultiColumnSources.
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
  
  class StringVector( values: Array[String] ) extends ArrayDataVector[String](values)
  {
    override type DataType = String
    protected override def factory( vals:Array[DataType] ) : DataVector[DataType] = new StringVector(vals)
  }
  
  class DoubleVector( values: Array[Double] ) extends ArrayDataVector[Double](values)
  {
    override type DataType = Double
    protected override def factory( vals:Array[DataType] ) : DataVector[DataType] = new DoubleVector(vals)
  }
  
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