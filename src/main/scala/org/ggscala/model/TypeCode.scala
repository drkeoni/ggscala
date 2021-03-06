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
  final case object AnyTypeCode extends TypeCode("a")
  final case object StringTypeCode extends TypeCode("s")
  final case object DoubleTypeCode extends TypeCode("d")
  final case object FactorTypeCode extends TypeCode("f")
  
  // syntactic sugar for type codes
  final val $a = AnyTypeCode
  final val $s = StringTypeCode
  final val $d = DoubleTypeCode
  final val $f = FactorTypeCode
  
  /** Implements the reverse mapping between string and TypeCode */
  def strToTypeCode( s:String ) = s match {
    case "a" => $a
    case "s" => $s
    case "d" => $d
    case "f" => $f
  }
  
  /** Maps a give object's class to a known type code */
  def objectToTypeCode( obj : Any ) = obj match {
    case _ : String => $s
    case _ : Double => $d
    case _ : Factor => $f
    case _ => $a
  }
}