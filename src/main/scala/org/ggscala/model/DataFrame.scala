/*
 * Grammar of Graphics in Scala
 * Copyright (c) 2011, ggscala.org
 */
package org.ggscala.model

import scala.collection.mutable.{HashMap,ListBuffer}
import org.ggscala.model.MultiColumnSource._
import org.ggscala.model.TypeCode._
import org.ggscala.model.Factor._

object DataFrame {
  
  /** A data frame which contains all of its values in memory. */
  class MemoryDataFrame( val colTypes : Seq[TypeCode] ) extends MultiColumnSource
    with RowBindable[MemoryDataFrame]
  {
    val columns = Array.tabulate( colTypes.length ){ i => new DataFrameColumn( colTypes(i) ) }
    val id2num = new HashMap[String,Int]
    
    /** Set the column identifiers for this data frame. */
    def setIds( ids : Seq[String] ) = 
    {
      (columns zip ids) foreach { case (c,s) => c.id = s }
      syncIds
    }
    private def syncIds = columns.zipWithIndex.foreach { case (c,i) => id2num(c.id) = i }
    
    /** Provides access to a DataColumn (type,id,data) for specified column. */
    def apply( id:String ) : DataColumn = columns( id2num(id) )
    
    private def keyAs[T]( key:String ) = this( key ).data.asInstanceOf[T]
    private def keyAs[T]( i:Int ) = columns(i).data.asInstanceOf[T]
    def $a( key:String ) = keyAs[ArrayDataVector[Any]](key)
    def $s( key:String ) = keyAs[StringVector](key)
    def $d( key:String ) = keyAs[DoubleVector](key)
    def $f( key:String ) = keyAs[FactorVector](key)
    
    def names = columns.map(_.id)
    
    override def ncol = columns.length
    
    def rbind( that:MemoryDataFrame ) =
    {
      // assert both data frames have same number of columns
      assert( that.ncol == this.ncol )
      // assert both data frames have same column IDs and types
      (columns zip that.columns).foreach { case (c1,c2) => assert( c1.id==c2.id ); assert( c1._type==c2._type) }
      val mdf = new MemoryDataFrame( colTypes )
      mdf.setIds( columns map (_.id) )
      for( i <- 0 until columns.length )
      {
        mdf.columns(i).data = columns(i)._type match {
          case StringTypeCode => keyAs[StringVector](i).cbind( that.keyAs[StringVector](i) )
          case DoubleTypeCode => keyAs[DoubleVector](i).cbind( that.keyAs[DoubleVector](i) )
          case FactorTypeCode => keyAs[FactorVector](i).cbind( that.keyAs[FactorVector](i) )
        }
      }
      mdf
    }
  }
  
  object MemoryDataFrame
  {
    def apply( cols : DataFrameColumn* ) =
    {
      val dfc = new MemoryDataFrame( cols.map(_._type) )
      cols.zipWithIndex.foreach { case (c,i) => dfc.columns(i)=c }
      dfc.syncIds
      dfc
    }
  }
  
  /**
   * Data frame which builds up its columns from string buffers
   */
  class TempStringDataFrame( colTypes : Seq[TypeCode] )
    extends MemoryDataFrame(colTypes)
  {
    {
      (0 until colTypes.length).foreach { i => columns(i) = new TempStringDataFrameColumn(colTypes(i)) }
    }
    // I'm just not doing something right.
    // It should be easier to convince a subclass that Array[TempStringDataFrameColumn] is an Array[DataFrameColumn]
    private def cols = columns.map( _.asInstanceOf[TempStringDataFrameColumn] )
    def addLine( line : Seq[String] ) = line.zipWithIndex.foreach { case (v,i) => cols(i).tmp += v }
    def unmarshalAll = cols.foreach { c => c.unmarshal }
  }
  
  class DataFrameColumn( val _type : TypeCode ) extends DataColumn
  {
    var id : String = null
    var data : DataVector[Any] = null
  }
    
  object DataFrameColumn
  {
    def apply( values:DataVector[Any], _type:TypeCode ) : DataFrameColumn =
    {
      val dfc = new DataFrameColumn(_type)
      dfc.data = values
      dfc
    }
      
    def apply( values:DataVector[Any], _type:TypeCode, id:String ) : DataFrameColumn =
    {
      val dfc = apply(values,_type)
      dfc.id = id
      dfc
    }
    
    private[model] def makeDfc[A <: Any]( _type:TypeCode, id:String, values:Array[A] ) = 
      DataFrameColumn(anyArrayToDataVector(values,_type),_type,id)
  }
  
  // factory methods for DataFrameColumn
  def s( id:String, values:Array[String] ) = DataFrameColumn.makeDfc($s,id,values)
  def d( id:String, values:Array[Double] ) = DataFrameColumn.makeDfc($d,id,values)
  def d( id:String, values:Array[Int] ) = DataFrameColumn.makeDfc($d,id,values)      
  def f( id:String, values:Array[String] ) = DataFrameColumn.makeDfc($f,id,values)
  
  class TempStringDataFrameColumn( _type : TypeCode ) extends DataFrameColumn(_type)
  {
    var tmp : ListBuffer[String] = new ListBuffer[String]
  
    def unmarshal =
    {
      data = stringArrayToDataVector(tmp.toArray,_type)
      tmp = null
    }
  }
}