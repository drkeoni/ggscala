/*
 * Grammar of Graphics in Scala
 * Copyright (c) 2011, ggscala.org
 */
package org.ggscala.model

import org.scalatest._
import flatspec._
import matchers._

import org.ggscala.model.DataColumn.StringVector
import org.ggscala.test.TestUtils

class DataVectorSpec extends AnyFlatSpec with should.Matchers {
  
  "A StringVector" should "be simple to construct" in
  {
    val sv = new StringVector( Array( "A", "B", "C" ) )
    sv.toArray should equal ( Array( "A", "B", "C" ) )
  }
  
  it should "support iteration" in
  {
    val sv = new StringVector( Array( "A", "B", "C" ) )
    var count = 0
    for( a <- sv ) count += 1
    count should be (3)
  }
  
}

object DataVectorSpec {
  def main(args:Array[String]) = 
  {
    TestUtils.timing {
      (new DataVectorSpec).execute()
    }
  }
}
