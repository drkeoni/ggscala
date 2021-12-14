/*
 * Grammar of Graphics in Scala
 * Copyright (c) 2011, ggscala.org
 */
package org.ggscala.io

import java.io.File
import org.scalatest._
import flatspec._
import matchers._
import org.ggscala.io.Csv._
import org.ggscala.model.MultiColumnSource._
import org.ggscala.model.TypeCode._
import org.ggscala.test.TestUtils

class SimpleCsvSpec extends AnyFlatSpec with should.Matchers {
  
  "A DelimitedLineReader" should "iterate over List[String]" in
  {
  	val csv = new DelimitedLineReader( CsvSpec.TestDataFile1 )
  	var lineCount = 0
  	for( v <- csv.iterator )
  	{
  	  v should have length (2)
  	  // Scala's static type system already asserts the following
  	  // but it's fun to test
  	  for( v2 <- v )
  	    v2 should have { 'class (classOf[String]) }
  	  lineCount += 1
  	}
  	lineCount should be (6)
  }
  
  it should "read files with arbitrary delimiters, comments, and metadata" in
  {
    val csv = new DelimitedLineReader( new File(CsvSpec.TestDataFile2), delimiter="\\|",
        metadataFilter={v:String => v.startsWith("##")},
        skipFilter={v:String => v.startsWith("#")} )
    var lineCount = 0
    for( v <- csv.iterator )
    {
      v should have length (2)
      // Scala's static type system already asserts the following
      // but it's fun to test
      for( v2 <- v )
        v2 should have { 'class (classOf[String]) }
      lineCount += 1
    }
    lineCount should be (6)
    csv.metadata should have length (1)
  }
}
  
class DataFrameCsvSpec extends AnyFlatSpec with should.Matchers {
    
  "A DataFrameCsv" should "have selectors" in
  {
    val csv = new DataFrameCsv( CsvSpec.TestDataFile1 )
    csv.setColTypes( Array($s,$d) )
    val expectCol1 = Array( "John", "Jane", "Andrew", "Julie", "John" )
    (csv $s "name").toArray should equal (expectCol1)
    val expectCol2 = Array( 36, 42, 6, 4, 2 )
    (csv $d "age").toArray should equal (expectCol2)
  }
    
  it should "have factors" in
  {
    val csv = new DataFrameCsv( CsvSpec.TestDataFile1 )
    csv.setColTypes( Array($f,$d) )
    // assert factors have labels
    val expectCol1 = Array( "John", "Jane", "Andrew", "Julie", "John" )
    csv.$f("name").toArray.map(_.toString) should equal (expectCol1)
    // assert factors reuse IDs
    val expectCol1b = Array( 0, 1, 2, 3, 0 )
    csv.$f("name").toArray.map(_.toInt) should equal (expectCol1b)
  }
    
  it should "have a DSL" in
  {
    val infile = csv( CsvSpec.TestDataFile1 ) as "f,d"
    val expectCol1 = Array( "John", "Jane", "Andrew", "Julie", "John" )
    infile.$f("name").toArray.map(_.toString) should equal (expectCol1)
  }
  
}

object CsvSpec {
  val TestDataFile1 = TestUtils getDataFile "simple_csv_test.csv"
  val TestDataFile2 = TestUtils getDataFile "comment_csv_test.csv"
  
  def main(args:Array[String]) = 
  {
    TestUtils.timing {
  	  (new SimpleCsvSpec).execute()
  	  (new DataFrameCsvSpec).execute()
    }
  }
}
