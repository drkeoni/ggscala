/*
 * Grammar of Graphics in Scala
 * Copyright (c) 2011, ggscala.org
 */
package org.ggscala.model

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import org.ggscala.test.TestUtils
import org.ggscala.model.DataFrame._

class DataFrameSpec extends FlatSpec with ShouldMatchers {
  
  "A MemoryDataFrame" should "be simple to construct" in
  {
    val data = Array( 1.0, 2.0, 3.0 )
    // make data frame with a single numeric column
    val dataFrame = MemoryDataFrame( d("values",data) )
    dataFrame.ncol should be (1)
  }
  
  "A MemoryDataFrame" should "be simple to construct with multiple columns" in
  {
    val data1 = Array( 1.0, 2.0, 3.0 )
    val data2 = Array( "A", "B", "C" )
    // make data frame with two columns
    val dataFrame = MemoryDataFrame( d("values",data1), s("letters",data2) )
    dataFrame.ncol should be (2)
    val expectNames = Array( "values", "letters" )
    dataFrame.names.toArray should equal (expectNames)
  }
  
}

object DataFrameSpec {
  def main(args:Array[String]) = 
  {
    TestUtils.timing {
	  (new DataFrameSpec).execute
    }
  }
}
