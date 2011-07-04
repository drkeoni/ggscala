/*
 * Grammar of Graphics in Scala
 * Copyright (c) 2011, ggscala.org
 */
package org.ggscala.model

import scala.collection.mutable.{HashMap,ListBuffer}
import org.ggscala.model.MultiColumnSource._
import org.ggscala.model.TypeCode._
import org.ggscala.model.Factor._
import org.ggscala.model.DataColumn._

object DataFrame {
  
  trait DataFrame extends MultiColumnSource with RowBindable[DataFrame]
  
  //
  // factory methods for DataFrame
  //
  //def apply( colTypes : Seq[TypeCode] )( implicit x:TypeCode ) : MultiColumnSource = new MemoryDataFrame( colTypes )
  
  def apply( cols : List[DataColumn] ) : DataFrame =
  {
    val dfc = new MemoryDataFrame( cols.map(_._type) )
    cols.zipWithIndex.foreach { case (c,i) => dfc.columns(i).id=c.id; dfc.columns(i).data=c.data }
    dfc.syncIds
    dfc
  }
    
  def apply( cols : DataColumn* ) : DataFrame = apply(cols.toList)
    
  //
  // factory methods for DataFrameColumn
  //
  
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
    
    private[model] def makeDfc[A <: Any]( _type:TypeCode, id:String, values:Iterable[A] ) = 
      DataFrameColumn( new IterableDataVector[A](values), _type, id )
    
    private[model] def makeDfc[A <: Any]( _type:TypeCode, id:String, values:Array[A] ) = 
      DataFrameColumn( anyArrayToDataVector(values,_type), _type, id )
  }
  
  // these are the easy ways to create DataFrameColumns
  
  def d( id:String, values:Array[Double] ) = DataFrameColumn.makeDfc($d,id,values)
  def d( id:String, values:Double ) = DataFrameColumn.makeDfc($d,id,Array(values))
  def d( id:String, values:Array[Int] ) = DataFrameColumn.makeDfc($d,id,values)
  def d( id:String, values:Int ) = DataFrameColumn.makeDfc($d,id,Array(values))  
  def f( id:String, values:Seq[String] ) = DataFrameColumn.makeDfc($f,id,values.toArray)
  def a( id:String, values:Iterable[Any] ) = DataFrameColumn.makeDfc($a,id,values)
  def s( id:String, values:Seq[String] ) = DataFrameColumn.makeDfc($s,id,values.toArray)
  def s( id:String, values:String ) = DataFrameColumn.makeDfc($s,id,Array(values))
  
  //
  // Most all (if not all) of the following implementations will get hidden in the end
  // (i.e. use the above factory methods)
  //
  
  /** A data frame which contains all of its values in memory. */
  class MemoryDataFrame( val colTypes : Seq[TypeCode] ) extends DataFrame
  {
    val columns = List.tabulate( colTypes.length ){ i => new DataFrameColumn( colTypes(i) ) }
    val id2num = new HashMap[String,Int]
    def idMap( id:String ) = id2num(id)
    
    /** Set the column identifiers for this data frame. */
    def setIds( ids : Seq[String] ) = 
    {
      (columns zip ids) foreach { case (c,s) => c.id = s }
      syncIds
    }
    private[DataFrame] def syncIds = columns.zipWithIndex.foreach { case (c,i) => id2num(c.id) = i }
    
    override def $a( key:String ) = keyAs[DataVector[Any]](key)
    def $s( key:String ) = keyAs[StringVector](key)
    def $d( key:String ) = keyAs[DoubleVector](key)
    def $f( key:String ) = keyAs[FactorVector](key)
    
    def rbind( that:RowBindable[MultiColumnSource] ) : MemoryDataFrame =
    {
      // the typing is still in need of an overhaul
      // we need another trait that suggests more what's special
      // about memory data frames (knowing their column types for instance)
      // until then, please bear with me...
      assert( that.isInstanceOf[MemoryDataFrame] )
      val _that = that.asInstanceOf[MemoryDataFrame]
      // assert both data frames have same number of columns
      assert( _that.ncol == this.ncol )
      // assert both data frames have same column IDs and types
      (columns zip _that.columns).foreach { case (c1,c2) => assert( c1.id==c2.id ); assert( c1._type==c2._type) }
      val mdf = new MemoryDataFrame( colTypes )
      mdf.setIds( columns map (_.id) )
      for( i <- 0 until columns.length )
      {
        mdf.columns(i).data = columns(i)._type match {
          case StringTypeCode => keyAs[StringVector](i).cbind( _that.keyAs[StringVector](i) )
          case DoubleTypeCode => keyAs[DoubleVector](i).cbind( _that.keyAs[DoubleVector](i) )
          case FactorTypeCode => keyAs[FactorVector](i).cbind( _that.keyAs[FactorVector](i) )
        }
      }
      mdf
    }
    
    /** 
     * generates string like:
     *      name   age
     * 0     Bob    25
     * 1   April    34
     * 2    Carl    52
     * Keeps string output to no longer than 15 chars for each column by removing excess
     * middle characters and replacing with "..."
     * (intended for debugging use)
     * */
    override def toString =
    {
      // use an odd number
      val Width = 15
      val StringWidth = "%" + Width + "s"
      val buffer = new StringBuilder
      def trunc( s:String ) = if (s.length<Width ) s else s.substring(0,Width/2-1)+"..."+s.substring(s.length-Width/2+1,s.length)
      def rowLine( v:Seq[Any] ) = v.map( s=>StringWidth.format(trunc(s.toString)) ) mkString "  "
      buffer.append( "        " + rowLine(names) + "\n" )
      for( (r,i) <- rowIterator.zipWithIndex )
        buffer.append( "%6d".format(i) + "  " + rowLine(r) + "\n" )
      buffer.toString
    }
  }
  
  object MemoryDataFrame
  {
    def apply( cols : DataFrameColumn* ) =
    {
      val dfc = new MemoryDataFrame( cols.map(_._type) )
      cols.zipWithIndex.foreach { case (c,i) => dfc.columns(i).id=c.id; dfc.columns(i).data=c.data }
      dfc.syncIds
      dfc
    }
  }
  
  /**
   * Data frame which builds up its columns from string buffers
   */
  class TempStringDataFrame( colTypes : Seq[TypeCode] ) extends MemoryDataFrame(colTypes)
  {
    override val columns = List.tabulate( colTypes.length ){ i => new TempStringDataFrameColumn( colTypes(i) ) }
    // I'm just not doing something right.
    // It should be easier to convince a subclass that Array[TempStringDataFrameColumn] is an Array[DataFrameColumn]
    //private def cols = columns.map( _.asInstanceOf[TempStringDataFrameColumn] )
    def addLine( line : Seq[String] ) = line.zipWithIndex.foreach { case (v,i) => columns(i).tmp += v }
    def unmarshalAll = columns.foreach { c => c.unmarshal }
  }
  
  /** Concrete class implementing a basic DataColumn */
  class DataFrameColumn( val _type : TypeCode ) extends DataColumn
  {
    var id : String = null
    var data : DataVector[Any] = null
  }
  
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