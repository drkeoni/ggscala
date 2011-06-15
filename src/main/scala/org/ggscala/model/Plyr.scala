/*
 * Grammar of Graphics in Scala
 * Copyright (c) 2011, ggscala.org
 */
package org.ggscala.model

import org.ggscala.model.MultiColumnSource.{MultiColumnSource,RowBindable}

object Plyr {
  // TODO: work out the type signature for this
  trait Plyr[Data <: RowBindable[MultiColumnSource]] {
    def ddply( data:Data, split:Seq[String], f:Data => Data )
  }
}