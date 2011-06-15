/*
 * Grammar of Graphics in Scala
 * Copyright (c) 2011, ggscala.org
 */
package org.ggscala.test

import java.io.File

object TestUtils {
  val ResourceDir = (new File( getClass.getResource("TestUtils.class").getFile )).getParent
  val TestDataPath =  new File( ResourceDir + "/../../../../../../testdata" )
  
  /** Retrieve a file from the testdata/ directory which is found relative to this
   *  code base.
   */
  def getDataFile( f:String ) = (new File(TestDataPath,f)).getAbsolutePath
  
  /**
   * Execute operation f and report the amount of time that it took
   */
  def timing[F]( f: =>F ) =
  {
    val then = System.nanoTime
    val ans = f
    val now = System.nanoTime
    println( "Took %g sec".format( (now-then).toDouble*1e-9 ) )
    ans
  }
}