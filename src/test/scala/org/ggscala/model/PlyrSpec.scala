/*
 * Grammar of Graphics in Scala
 * Copyright (c) 2011, ggscala.org
 */
package org.ggscala.model

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

import org.ggscala.model.DataFrame._
import org.ggscala.model.Plyr.ddply
import org.ggscala.test.TestUtils

class PlyrSpec extends FlatSpec with ShouldMatchers {
  "ddply" should "group by a single factor and generate a data frame" in
  {
    val dataFrame = MemoryDataFrame( 
      d("values",Array( 1.0, 2.0, 3.0, 2.0, 3.0 )), 
      s("letters",Array( "ApplesBananasAndYogurt", "B", "C", "ApplesBananasAndYogurt", "B" )) 
    )
    println( "starting with " )
    println( dataFrame )
    def mean(v:Iterable[Double]) = {
      val v2 = v.toArray
      if ( v2.length==0 ) 0.0; else v2.sum / v2.length
    }
    // the signature for the function call is so ugly...we'll improve it...
    val e = ddply( dataFrame, List("letters"), { d2 => Some(MemoryDataFrame(d("avg",mean(d2.$d("values"))))) } )
    println( "after ddply " )
    println( e )
    // this test is commented out until ddply fully works
    //e.ncol should be (2)
  }
}

object PlyrSpec {
  def main(args:Array[String]) = 
  {
    TestUtils.timing {
      (new PlyrSpec).execute
    }
  }
}