/*
 * Grammar of Graphics in Scala
 * Copyright (c) 2011, ggscala.org
 */
package org.ggscala.model

import org.ggscala.model.MultiColumnSource.{MultiColumnSource,RowBindable}
import org.ggscala.model.Factor._

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
      def col(k:String) = data.$a(k).map(_.toString)
      def compound(a:Iterable[String],key:String) = (a zip col(key)).map{ case (s1,s2) => s1 + "_" + s2 }
      val compoundKeys = split.tail.foldLeft( col(split(0)) )( (a,b) => compound(a,b) ).toArray
      val compoundFactors = new FactorVector(compoundKeys)
      // partition
      // map and fold
      data
    }
  }
}