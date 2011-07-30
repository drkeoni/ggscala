/*
 * Grammar of Graphics in Scala
 * Copyright (c) 2011, ggscala.org
 */
package org.ggscala.io

import java.io.File
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap
import org.ggscala.model.MultiColumnSource._
import org.ggscala.model.TypeCode._
import org.ggscala.model.DataFrame.TempStringDataFrame

object Csv
{
  /** Provides simple iteration over a CSV file, returning List[String] for each row */
  class SimpleCsv( private val filePath:File ) extends Iterable[List[String]]
  {
    /** constructor from a file path string instead of java.io.File **/
    def this( f:String ) = this( new File(f) )
    
    private lazy val _open = io.Source.fromFile( filePath.getAbsolutePath ).getLines
	  
    def iterator = new Iterator[List[String]]
    {
      def hasNext = _open.hasNext
      def next = _open.next.split(",").toList
    }
  }

  /** Provides type-specific manipulation of columns from a CSV file. */
  class DataFrameCsv( filePath:File ) extends MultiColumnSourceDelegate
  {
    def this( f:String ) = this( new File(f) )
    
    val csv = new SimpleCsv(filePath)
    protected var colTypes : Seq[TypeCode] = null
    // These three (identical) methods provide clues to the parser for how to assign types for each column
    def setColTypes( newColTypes:Seq[TypeCode] ) = colTypes = newColTypes
    def setColTypes( newColTypes:String ) : Unit = setColTypes( newColTypes.split(",").toArray.map(strToTypeCode) )
    def as( newColTypes:String ) = { setColTypes(newColTypes); this }
    
    /** 
     * The actual data for this DataFrameCsv is initialized lazily.
     * Read the CSV file into memory and convert to appropriate type-specific columns.
     **/
    protected def columnSource = 
    {
      val cols = new TempStringDataFrame( colTypes )
      csv.iterator.zipWithIndex.foreach
      {
        _ match {
          case (line,0) => cols.setIds(line)
          // all of the data is initially stored as String
          case (line,_) => cols.addLine(line)
        }
      }
      // this call unmarshals the columns to the appropriate types
      cols.unmarshalAll
      cols
    }
  }
  
  /** simple constructor for a mini CSV/data-frame DSL */
  def csv( filePath:String ) = new DataFrameCsv(filePath)
}
