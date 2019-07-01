package com.dgmall.sparktest.hbase

import java.util

import org.apache.hadoop.hbase.{Cell, HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Delete, Get, HTable, Put, Result, ResultScanner, Scan, Table}
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.util.Bytes


/**
  * @Author: Cedaris
  * @Date: 2019/6/19 11:03
  */
object HBaseUtils {

  /**
    * 设置HBaseConfiguration
    * @param quorum
    * @param port
    */
  def getHBaseConfiguration(quorum:String, port:String): Configuration ={
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum",quorum)
    conf.set("hbase.zookeeper.property.clientPort",port)

    conf
  }

  /**
    * 返回或者新建HBASEAdmin
    * @param conf
    * @param tableName
    */
  def getHBaseAdmin(conf:Configuration,tableName:String): Admin ={
    val conn: Connection = ConnectionFactory.createConnection(conf)
    val admin: Admin = conn.getAdmin
    if(!admin.isTableAvailable(TableName.valueOf(tableName))){
      val tabledesc = new HTableDescriptor(TableName.valueOf(tableName))
      admin.createTable(tabledesc)
    }
    admin
  }

  /**
    * 判断制定表是否存在
    * @param tableName
    * @param admin
    * @return
    */
  def isTableExists(tableName:String,admin:Admin): Boolean ={
    val isExists: Boolean = admin.tableExists(TableName.valueOf(tableName))
    isExists
  }

  /**
    * 返回HTable
    * @param configuration
    * @param tableName
    * @return
    */
  def getTable(configuration: Configuration,tableName: String): HTable ={

    new HTable(configuration,TableName.valueOf(tableName))
  }

  /**
    * 创建表
    * @param tableName
    * @param admin
    * @param cfs
    */
  def createTable(tableName:String,admin: Admin ,cfs:String*){
    //表如果存在就直接结束方法
    if(isTableExists(tableName,admin)) {return}
    val desc = new HTableDescriptor(TableName.valueOf(tableName))
    for (cf <- cfs) {
      desc.addFamily(new HColumnDescriptor(cf))
    }

    admin.createTable(desc)
    println("表:" + tableName + "创建成功!")
  }

  /**
    * 删除表
    * @param tableName
    * @param admin
    */
  def deleteTable(tableName: String,admin: Admin): Unit ={
    if (isTableExists(tableName,admin)){
      admin.disableTable(TableName.valueOf(tableName));
      admin.deleteTable(TableName.valueOf(tableName));
    }else{
      println("你要删除的表：" + tableName + "不存在！！");
    }
  }

  /**
    * 向表中插入数据
    * @param tableName
    * @param rowKey
    * @param cf 列族
    * @param column
    * @param value
    * @param admin
    */
  def addData(tableName:String,rowKey:String,cf:String,column:String,
              value:String,admin: Admin): Unit ={
    if(!isTableExists(tableName,admin)){
      createTable(tableName,admin)
    }
    val conn: Connection = admin.getConnection
    val table: Table = conn.getTable(TableName.valueOf(tableName))
    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(Bytes.toBytes(cf),
      Bytes.toBytes(column),
      Bytes.toBytes(value)
    )

    table.put(put)
    table.close()
  }

  /**
    * 删除多行数据
    * @param tableName
    * @param admin
    * @param rows
    */
  def deleteRows(tableName:String,admin: Admin,rows :String*): Unit ={
    if(!isTableExists(tableName,admin)){return }
    val table: Table = admin.getConnection.getTable(TableName.valueOf(tableName))
    //批量删除
    val deletes = new util.ArrayList[Delete]()
    for (row <- rows) {
    deletes.add(new Delete(Bytes.toBytes(row)))
    }
    table.delete(deletes)
  }

  /**
    * 获取某一行指定列的数据
    * @param tableName
    * @param row
    * @param cf
    * @param column
    * @param admin
    */
  def getDataByColumn(tableName:String,row :String,cf:String,column:String,
                      admin: Admin): Unit ={
    if(!isTableExists(tableName,admin)){return }
    val table: Table = admin.getConnection.getTable(TableName.valueOf(tableName))
    val get = new Get(Bytes.toBytes(row))

    get.addColumn(Bytes.toBytes(cf),Bytes.toBytes(column))
    val result: Result = table.get(get)
    val cells: util.List[Cell] = result.listCells()

    while(cells.iterator.hasNext)  {
      val cell: Cell = cells.iterator.next()
      val family: String = Bytes.toString(CellUtil.cloneFamily(cell))
      val value: String = Bytes.toString(CellUtil.cloneValue(cell))
      val rowName: String = Bytes.toString(CellUtil.cloneRow(cell))
      val column: String = Bytes.toString(CellUtil.cloneQualifier(cell))
      println("rowName = " + rowName)
      println("family = " + family)
      println("column = " + column)
      println("value = " + value)
    }
  }

  /**
    * 获取某一行数据
    * @param tableName
    * @param row
    * @param admin
    */
  def getRowData(tableName:String,row:String,admin: Admin): Unit ={
    val table: Table = admin.getConnection.getTable(TableName.valueOf(tableName))

    val get = new Get(Bytes.toBytes(row))
    val result: Result = table.get(get)
    val cells: util.List[Cell] = result.listCells()


    while(cells.iterator.hasNext)  {
      val cell: Cell = cells.iterator.next()
      val family: String = Bytes.toString(CellUtil.cloneFamily(cell))
      val value: String = Bytes.toString(CellUtil.cloneValue(cell))
      val rowName: String = Bytes.toString(CellUtil.cloneRow(cell))
      val column: String = Bytes.toString(CellUtil.cloneQualifier(cell))
      println("rowName = " + rowName)
      println("family = " + family)
      println("column = " + column)
      println("value = " + value)
    }
  }

  /**
    * 获取表中的所有数据
    * @param tableName
    * @param admin
    */
  def getAllData(tableName:String,admin: Admin): Unit ={
    val table: Table = admin.getConnection.getTable(TableName.valueOf(tableName))
    val results: ResultScanner = table.getScanner(new Scan)

    results.iterator()
    /*while(result != null){
      /*val row: Array[Byte] = result.getRow
      print(row.toString)*/
      var cells: Array[Cell] = result.rawCells()
      for (cell <- cells){
        val cell: Cell = cells.iterator.next()
        val family: String = Bytes.toString(CellUtil.cloneFamily(cell))
        val value: String = Bytes.toString(CellUtil.cloneValue(cell))
        val rowName: String = Bytes.toString(CellUtil.cloneRow(cell))
        val column: String = Bytes.toString(CellUtil.cloneQualifier(cell))
        println("rowName = " + rowName)
        println("family = " + family)
        println("column = " + column)
        println("value = " + value)
        println("----------------------------")
      }
      result = results.next()
    }*/
  }

  def close(admin: Admin): Unit ={
    if(admin != null){
      admin.close()
    }
  }
}
