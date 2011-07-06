/*
 * Grammar of Graphics in Scala
 * Copyright (c) 2011, ggscala.org
 */
package org.ggscala.model

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

import org.ggscala.model.DataFrame._
import org.ggscala.model.MultiColumnSource.MultiColumnSource
import org.ggscala.model.Plyr._
import org.ggscala.test.TestUtils

class PlyrSpec extends FlatSpec with ShouldMatchers {
  
  def mean(v:Iterable[Double]) = {
    val v2 = v.toArray
    if ( v2.length==0 ) 0.0; else v2.sum / v2.length
  }
  
  "ddply" should "group by a single factor and generate a data frame" in
  {
    val dataFrame = DataFrame( 
      d("values",Array( 1.0, 2.0, 3.0, 2.0, 3.0 )), 
      f("letters",Array( "ApplesBananasAndYogurt", "B", "C", "ApplesBananasAndYogurt", "B" )) 
    )
    println( "starting with " )
    println( dataFrame )
    
    // take the column named values from d2 and compute the mean
    // return a new data frame with a column named "avg" with this value
    def func( d2:MultiColumnSource ) = DataFrame( d( "avg", mean(d2.$d("values")) ) )
    
    val e = ddply( dataFrame, List("letters"), func _ )
    println( "after ddply " )
    println( e )
    e.ncol should be (2)
  }
  
  it should "group by a single string column and generate a data frame" in
  {
    val dataFrame = DataFrame( 
      d("values",Array( 1.0, 2.0, 3.0, 2.0, 3.0 )), 
      s("letters",Array( "ApplesBananasAndYogurt", "B", "C", "ApplesBananasAndYogurt", "B" )) 
    )
    
    def func( d2:MultiColumnSource ) = Some(DataFrame( d("avg",mean(d2.$d("values"))) ))
    
    val e = ddply( dataFrame, List("letters"), func _ )
    e.ncol should be (2)
  }
  
  it should "group by a single factor and generate a data frame with two computed columns" in
  {
    val dataFrame = DataFrame( 
      d("values",Array( 1.0, 2.0, 3.0, 2.0, 3.0 )), 
      f("letters",Array( "ApplesBananasAndYogurt", "B", "C", "ApplesBananasAndYogurt", "B" )) 
    )
    println( "starting with " )
    println( dataFrame )
    
    // take the column named values from d2 and compute the mean and minimum
    // return a new data frame with a column named "avg" with this value and a column named "min" with the minimum value
    def func( d2:MultiColumnSource ) = DataFrame( 
      d( "avg", mean(d2.$d("values")) ),
      d( "min", d2.$d("values").min )
    )
    
    val e = ddply( dataFrame, List("letters"), func _ )
    
    println( "after ddply " )
    println( e )
    
    e.ncol should be (3)
    // this is essentially "which" written in scala
    // i.e. which( e$letters=="B" )
    val i = e.$f("letters").indexWhere( _.toString=="B" )
    e.$d("min")(i) should equal (2.0)
  }
}

object PlyrSpec {
  def main(args:Array[String]) = 
  {
    TestUtils.timing {
      (new PlyrSpec).execute()
    }
  }
}
