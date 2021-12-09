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
  case class LineReaderCallback( pattern:String=>Boolean, action:String=>Unit )
  
  class CallbackLineReader[A]( protected val filePath:File, protected val default : String=>A )
    extends Iterable[A]
  {
    protected val callbacks = new ListBuffer[LineReaderCallback]
    private lazy val _open = io.Source.fromFile( filePath.getAbsolutePath ).getLines
    
    def iterator = new Iterator[A]
    {
      var nextLine : Option[String] = None
      var _hasNext = true
      
      advance
      
      private def advance = {
        nextLine = None
        while( !nextLine.isDefined && _open.hasNext )
        {
          nextLine = Some(_open.next)
          val callback = callbacks.find( _.pattern(nextLine.get) )
          callback.map( _.action(nextLine.get) )
          if ( callback.isDefined )
            nextLine = None
        }
        _hasNext = nextLine.isDefined
      }
      
      def hasNext = _hasNext
      
      def next = {
        val line = nextLine.get
        advance
        default(line)
      }
    }
  }
  
  /** Provides simple iteration over a delimited line file, returning List[String] for each row.
   *  Optional configuration includes
   *  <ul>
   *  <li>setting an arbitrary regex delimiter</li>
   *  <li>recording lines which are considered "metadata"</li>
   *  <li>skipping arbitrary lines</li>
   *  </ul>
   *  */
  class DelimitedLineReader( filePath:File,
      protected val delimiter:String = ",", 
      protected val metadataFilter:String=>Boolean = {s=>false},
      protected val skipFilter:String=>Boolean = {s=>false} ) extends
      CallbackLineReader( filePath, { l => l.split( delimiter ).toList } )
  {
    /** constructor from a file path string instead of java.io.File **/
    def this( f:String ) = this( new File(f) )
    
    callbacks += LineReaderCallback( metadataFilter, {l=>metadata+=l} )
    callbacks += LineReaderCallback( skipFilter, {l=>()} )
    val metadata = new ListBuffer[String]
  }

  /** Provides type-specific manipulation of columns from a CSV file. */
  class DataFrameCsv( filePath:File ) extends MultiColumnSourceDelegate
  {
    def this( f:String ) = this( new File(f) )
    
    val csv = new DelimitedLineReader(filePath)
    protected var colTypes : Option[Seq[TypeCode]] = None
    // These three (identical) methods provide clues to the parser for how to assign types for each column
    def setColTypes( newColTypes:Seq[TypeCode] ) = colTypes = Some(newColTypes)
    def setColTypes( newColTypes:String ) : Unit = setColTypes( newColTypes.split(",").toArray.map(strToTypeCode) )
    def as( newColTypes:String ) = { setColTypes(newColTypes); this }
    
    /** 
     * The actual data for this DataFrameCsv is initialized lazily.
     * Read the CSV file into memory and convert to appropriate type-specific columns.
     **/
    protected def columnSource = 
    {
      require( colTypes.isDefined, "Column types must be defined before reading from a DataFrameCsv" )
      val cols = new TempStringDataFrame( colTypes.get )
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
