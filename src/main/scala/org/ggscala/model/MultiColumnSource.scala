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
  /** Represents a data source which provides columns of data.
   *  The caller is responsible for supplying types for the column data.
   *  Sub-traits provide stronger type support.
   */
  trait NamedColumnSource
  {
    /** select a column with any type */
    def $a( key:String ) : DataVector[Any]
    /** provide the list of keys known to this column source. */
    def names : Seq[String]
    /** Subclasses can override to provide the total number of columns */
    def ncol = 0
    /** Iterate over rows of this column source.  Rows are represented by Seq[Any].
     *  The order of elements is defined by the order of columns as returned by 
     *  names.
     */
    def rowIterator : Iterator[IndexedSeq[Any]] = new Iterator[IndexedSeq[Any]] {
      val _iterators = names.map( $a(_).iterator )
      def hasNext = _iterators.forall(_.hasNext)
      def next = _iterators.foldLeft(new ArrayBuffer[Any]())( (l,i) => l += i.next )
    }
  }
  
  /** Represents a data source which provides typed columns of data.
   *  Type-specific selectors allow retrieval of correctly typed
   *  Iterables corresponding to a key.
   */
  trait MultiColumnSource extends NamedColumnSource
  {
    //
    // abstract methods
    //
    val columns : Seq[DataColumn]
    def idMap( id:String ) : Int
    /** select a string column */
    def $s( id:String ) : DataVector[String]
    /** select a double column */
    def $d( id:String ) : DataVector[Double]
    /** select a factor column */
    def $f( id:String ) : DataVector[Factor]
    
    //
    // concrete methods
    //
    def names = columns.map(_.id)
    override def ncol = columns.length
    /** select a column with any type */
    def $a( id:String ) = columns(idMap(id)).data
    
    /** Provides access to a DataColumn (type,id,data) for specified column. */
    def apply( id:String ) : DataColumn = columns( idMap(id) )
    
    protected def keyAs[T]( id:String ) = this( id ).data.asInstanceOf[T]
    protected def keyAs[T]( i:Int ) = columns(i).data.asInstanceOf[T]
  }
  
  // TODO: still playing with the type signature here
  
  /** MultiColumnSources which can be row-concatenated with other MultiColumnSources */
  trait RowBindable[+D <: MultiColumnSource] {
    this: MultiColumnSource =>
    def rbind( d:RowBindable[MultiColumnSource] ) : RowBindable[D]
    def rbind( d:Option[RowBindable[MultiColumnSource]] ) : RowBindable[D] = if ( d.isEmpty ) this; else rbind( d.get )
  }
  
  /** MultiColumnSources which can be column-concatenated with other MultiColumnSources */
  trait ColumnBindable {
    this: MultiColumnSource =>
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
    lazy val columns = _columnSource.columns
    def idMap( id:String ) = _columnSource.idMap(id)
    override def $a( key:String ) = _columnSource.$a(key)
    def $s( key:String ) = _columnSource.$s(key)
    def $d( key:String ) = _columnSource.$d(key)
    def $f( key:String ) = _columnSource.$f(key)
    override def names = _columnSource.names
    override def ncol = _columnSource.ncol
  }
  
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