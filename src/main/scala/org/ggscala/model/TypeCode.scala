/*
 * Grammar of Graphics in Scala
 * Copyright (c) 2011, ggscala.org
 */
package org.ggscala.model

import org.ggscala.model.Factor.Factor

/**
 * Defines the dynamic type system for multi-column sources
 */
object TypeCode 
{
  sealed abstract class TypeCode( val label:String )
  final case object StringTypeCode extends TypeCode("s")
  final case object DoubleTypeCode extends TypeCode("d")
  final case object FactorTypeCode extends TypeCode("f")
  
  // syntactic sugar for type codes
  final val $s = StringTypeCode
  final val $d = DoubleTypeCode
  final val $f = FactorTypeCode
  
  /** Implements the reverse mapping between string and TypeCode */
  def strToTypeCode( s:String ) = s match {
    case "s" => $s
    case "d" => $d
    case "f" => $f
  }
  
  def objectToTypeCode( obj : Any ) = obj match {
    case _ : String => $s
    case _ : Double => $d
    case _ : Factor => $f
    case _ =>
      throw new IllegalArgumentException( "Couldn't find type code for object = %s".format(obj) )
  }
}