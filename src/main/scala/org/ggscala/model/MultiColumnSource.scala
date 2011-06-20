/*
 * Grammar of Graphics in Scala
 * Copyright (c) 2011, ggscala.org
 */
package org.ggscala.model

import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import org.ggscala.model.Factor.Factor
import org.ggscala.model.TypeCode._
import org.ggscala.model.DataColumn._

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
    def types = columns.map( _._type )
    override def ncol = columns.length
    /** select a column with any type */
    def $a( id:String ) = this( id ).data
    
    /** Provides access to a DataColumn (type,id,data) for specified column. */
    def apply( id:String ) : DataColumn = columns( idMap(id) )
    /** Provides access to a DataColumn (type,id,data) for specified column. */
    def apply( i:Int ) : DataColumn = columns( i )
    
    protected def keyAs[T]( id:String ) = this( id ).data.asInstanceOf[T]
    protected def keyAs[T]( i:Int ) = columns(i).data.asInstanceOf[T]
  }
  
  // TODO: still playing with the type signature here
  
  /** MultiColumnSources which can be row-concatenated with other MultiColumnSources */
  trait RowBindable[+D <: MultiColumnSource] extends MultiColumnSource {
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
  
}