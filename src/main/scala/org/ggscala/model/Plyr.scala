/*
 * Grammar of Graphics in Scala
 * Copyright (c) 2011, ggscala.org
 */
package org.ggscala.model

import org.ggscala.model.MultiColumnSource.{MultiColumnSource,RowBindable,IterableDataVector}
import org.ggscala.model.Factor._
import scala.collection.mutable.HashMap

object Plyr {
  // TODO: work out the type signature for this
  trait Plyr[D <: RowBindable[MultiColumnSource]] {
    /** Partition data by factors determined by unique combinations of the columns
     *  specified by split, apply a function to each partition, and combine the
     *  results in a final data source.
     */
    def ddply( data:D, split:Seq[String], f:D => D ) : D =
    {
      assert( split.length >= 1 )
      // enumerate unique compound keys
      /*
      def col(k:String) = data.$a(k).map(_.toString)
      def compound(a:Iterable[String],key:String) = (a zip col(key)).map{ case (s1,s2) => s1 + "_" + s2 }
      val compoundKeys = split.tail.foldLeft( col(split(0)) )( (a,b) => compound(a,b) ).toArray
      val compoundFactors = new FactorVector(compoundKeys)
      */
      val _names = data.names
      val colIndices = split.map( _names.indexOf(_) )
      def compoundKey( row:IndexedSeq[Any] ) = colIndices.map( row(_).toString ) mkString "_"
      data.rowIterator.toList.groupBy(compoundKey _)
      // partition
      // map and fold
      data
    }
  }
  
  class RowList( val _names:Seq[String], val rows:List[IndexedSeq[Any]] ) extends MultiColumnSource
  {
    val id2num = {
      val hash = new HashMap[String,Int]
      _names.zipWithIndex.foreach { case (v,i) => hash(v) = i }
      hash
    }
    def names = _names
    def $a( id:String ) = new IterableDataVector(rows.map( r => r(id2num(id)) ))
    def $s( id:String ) = new IterableDataVector(rows.map( r => r(id2num(id)).toString ))
    def $d( id:String ) = new IterableDataVector(rows.map( r => r(id2num(id)).asInstanceOf[Double] ))
    def $f( id:String ) = new IterableDataVector(rows.map( r => r(id2num(id)).asInstanceOf[Factor] ))
  }
}