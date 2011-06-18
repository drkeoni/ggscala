/*
 * Grammar of Graphics in Scala
 * Copyright (c) 2011, ggscala.org
 */
package org.ggscala.model

import org.ggscala.model.MultiColumnSource._
import org.ggscala.model.Factor._
import org.ggscala.model.TypeCode._
import scala.collection.mutable.HashMap

object Plyr {
  
  /** Partition data by factors determined by unique combinations of the columns
   *  specified by split, apply a function to each partition, and combine the
   *  results in a final data source.
   */
  def ddply[D <: MultiColumnSource]( data:MultiColumnSource, split:Seq[String], f:MultiColumnSource => Option[RowBindable[D]] ) : RowBindable[D] =
  {
    val subData = partition( data, split )
    
    // rbind that can handle 0, 1, or 2 null arguments
    def rbind( a:Option[RowBindable[D]], b:Option[RowBindable[D]] ) : Option[RowBindable[D]] =
      if ( !b.isDefined ) a else a.map( _.rbind(b.get)).orElse(b)
        
    // map and reduce   
    val nil = Option[RowBindable[D]](null)
    
    // in order to implement this function we need:
    // 1. to know the type of each key
    // 2. the ability to append columns to a column source
    // (to resolve these items:
    //  - need trait + abstract class for managing DataColumns
    //  - need trait for appending/updating columns
    //  - need to rewrite ddply signature for expecting these traits
    // )
    def combineFunctionAndKeys( mcs:MultiColumnSource ) : Option[RowBindable[D]] =
    {
      f(mcs)
    }
    
    val ans = subData.foldLeft( nil )( (s,v) => rbind( s, combineFunctionAndKeys(v) ) )
    if ( ans.isEmpty )
      throw new IllegalStateException( "ddply call resulted in empty multi-column source!" )
    else
      ans.get
  }
  
  def partition[D <: MultiColumnSource]( data:MultiColumnSource, split:Seq[String] ) : Iterator[MultiColumnSource] =
  {
    assert( split.length >= 1 )
    // partition
    val _names = data.names
    val _types = data.columns.map( _._type )
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
    // not actually implemented yet
    val columns = List.tabulate(_names.length)( i => new RowListColumn(_types(i)) )
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