/*
 * Grammar of Graphics in Scala
 * Copyright (c) 2011, ggscala.org
 */
package org.ggscala.model

import org.scalatest._
import flatspec._
import matchers._
import org.ggscala.test.TestUtils
import org.ggscala.model.DataFrame._

class DataFrameSpec extends AnyFlatSpec with should.Matchers {
  
  "A MemoryDataFrame" should "be simple to construct" in
  {
    val data = Array( 1.0, 2.0, 3.0 )
    // make data frame with a single numeric column
    val dataFrame = MemoryDataFrame( d("values",data) )
    dataFrame.ncol should be (1)
    dataFrame.names.toArray should equal ( Array("values") )
  }
  
  it should "be simple to construct with multiple columns" in
  {
    val data1 = Array( 1.0, 2.0, 3.0 )
    val data2 = Array( "A", "B", "C" )
    // make data frame with two columns
    val dataFrame = MemoryDataFrame( d("values",data1), s("letters",data2) )
    dataFrame.ncol should be (2)
    val expectNames = Array( "values", "letters" )
    dataFrame.names.toArray should equal (expectNames)
  }
  
  it should "be row-bindable" in
  {
    // construct start state
    val (data1,data2,data3,data4) = ( Array( 1.0, 2.0, 3.0 ),
      Array( "A", "B", "C" ),
      Array( 4.0, 5.0, 6.0 ),
      Array( "A", "B", "C" )
    )
    val dataFrame1 = MemoryDataFrame( d("values",data1), s("letters",data2) )
    val dataFrame2 = MemoryDataFrame( d("values",data3), s("letters",data4) )
    // action
    val dataFrame3 = dataFrame1.rbind(dataFrame2)
    // asserts
    val expectNames = Array( "values", "letters" )
    val expectData1 = Array( 1.0, 2.0, 3.0, 4.0, 5.0, 6.0 )
    val expectData2 = Array( "A", "B", "C", "A", "B", "C" )
    dataFrame3.ncol should be (2)
    dataFrame3.names.toArray should equal (expectNames)
    dataFrame3.$d("values").toArray should equal (expectData1)
    dataFrame3.$s("letters").toArray should equal (expectData2)
  }
  
  it should "support row iteration" in
  {
    val (data1,data2) = (Array( 1.0, 2.0, 3.0 ),Array( "A", "B", "C" ))
    val dataFrame = MemoryDataFrame( d("values",data1), s("letters",data2) )
    
    var count = 0
    for( row <- dataFrame.rowIterator )
    {
      row.length should be (2)
      // TODO assert casts (might be better way to do this with ScalaTest)
      count += 1
    }
    count should be (3)
  }
}

object DataFrameSpec {
  def main(args:Array[String]) = 
  {
    TestUtils.timing {
      (new DataFrameSpec).execute()
    }
  }
}
