/*
 * Grammar of Graphics in Scala
 * Copyright (c) 2011, ggscala.org
 */
package org.ggscala.model

import scala.collection.mutable.HashMap

import org.ggscala.model.MultiColumnSource._
import org.ggscala.model.Factor._
import org.ggscala.model.TypeCode._
import org.ggscala.model.DataColumn._
import org.ggscala.model.DataFrame._

object Plyr {
  
  implicit def ddply_function( f:MultiColumnSource => DataFrame ) = { mcs:MultiColumnSource => Some(f(mcs)) }
  
  /** Partition data by factors determined by unique combinations of the columns
   *  specified by split, apply a function to each partition, and combine the
   *  results in a final data source.
   */
  def ddply( data:MultiColumnSource, split:Seq[String], f:MultiColumnSource => Option[DataFrame] ) : DataFrame =
  {
    type D = RowBindable[DataFrame]
    // split data by unique values across columns
    val subData = partition( data, split )
    
    // rbind that can handle 0, 1, or 2 null arguments
    def rbind( a:Option[D], b:Option[D] ) : Option[D] =
      if ( !b.isDefined ) a else a.map( _.rbind(b.get) ).orElse(b)
        
    // map and reduce   
    
    // in order to implement this function we need:
    // 1. to know the type of each key
    // 2. the ability to append columns to a column source
    // (to resolve these items:
    //  - need trait + abstract class for managing DataColumns
    //  - need trait for appending/updating columns
    //  - need to rewrite ddply signature for expecting these traits
    // )
    def combineFunctionAndKeys( mcs:MultiColumnSource ) : Option[DataFrame] =
    {
      // here is where we actually apply the function
      val ans = f(mcs)
      if ( ans.isEmpty )
        return ans
      val ans2 = ans.get
      // the remainder is associating the appropriate columns from the keys (the split columns)
      // with the output
      val keyCols = split.map(mcs(_))
      val keys = keyCols.map( _.data.head.toString )
      // we need to know the number of rows in a MultiColumnSource...so far there's no support for this
      // and we actually want to introduce a sub-trait to allow this
      // cheat for now and find out the # of rows of ans2
      val nrow = ans2(0).data.foldLeft(0)( (s,v) => s+1 )
      val keyColumns = 
        ( keys zip keyCols )
          .map{ case (k,col) => DataFrameColumn( stringArrayToDataVector(Array.fill(nrow)(k),col._type), col._type, col.id ) }
      Some( DataFrame( keyColumns.toList ::: ans2.columns.toList ) )
    }
    
    val nil = Option[D](null)
    val ans = subData.foldLeft( nil )( (s,v) => rbind( s, combineFunctionAndKeys(v) ) )
    if ( ans.isEmpty )
      throw new IllegalStateException( "ddply call resulted in empty multi-column source!" )
    else
      ans.get.asInstanceOf[DataFrame]
  }
  
  /**
   * Partitions this MultiColumnSource into multiple sources, using unique combinations
   * of the columns specified by the split argument.
   */
  def partition[D <: MultiColumnSource]( data:MultiColumnSource, split:Seq[String] ) : Iterator[MultiColumnSource] =
  {
    assert( split.length >= 1 )
    // partition
    val _names = data.names
    val _types = data.types
    val colIndices = split.map( _names.indexOf(_) )
    def compoundKey( row:IndexedSeq[Any] ) = colIndices.map( row(_).toString ) mkString "_"
    val subData = data.rowIterator.toList.groupBy(compoundKey _)
    subData.valuesIterator.map( new RowList(_names,_types,_) )
  }
  
  class RowList( val _names:Seq[String], val _types:Seq[TypeCode], val rows:List[IndexedSeq[Any]] ) extends MultiColumnSource
  {
    val id2num = {
      val hash = new HashMap[String,Int]
      _names.zipWithIndex.foreach { case (v,i) => hash(v) = i }
      hash
    }
    val columns = {
      def toDataColumn(i:Int) = _types(i) match {
        case StringTypeCode => s( _names(i), $s(_names(i)).toArray )
        case DoubleTypeCode => d( _names(i), $d(_names(i)).toArray )
        case FactorTypeCode => f( _names(i), $f(_names(i)).toArray.map(_.toString) )
        case AnyTypeCode => a( _names(i), $a(_names(i)) )
      }
      List.tabulate(_names.length)( toDataColumn _ )
    }
    def idMap(id:String) = id2num(id)
    override def names = _names
    override def $a( id:String ) = new IterableDataVector(rows.map( r => r(id2num(id)) ))
    def $s( id:String ) = new IterableDataVector(rows.map( r => r(id2num(id)).toString ))
    def $d( id:String ) = new IterableDataVector(rows.map( r => r(id2num(id)).asInstanceOf[Double] ))
    def $f( id:String ) = new IterableDataVector(rows.map( r => r(id2num(id)).asInstanceOf[Factor] ))
    override def ncol = id2num.size
  }
  
  class RowListColumn( val _type:TypeCode ) extends DataColumn
  {
    var id = ""
    var data : DataVector[Any] = new IterableDataVector( (0 to 1) )
  }
}