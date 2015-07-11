package org.sparklinedata.tpch

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

object Functions {
  //  def substring(s : String, start : Int, end : Int) : String =
  //    s.substring(start, end)

  def concat(s1: String, s2: String): String =
    s"$s1$s2"
}

class TpchGenMain(sqlCtx : SQLContext, base: String, scale : String) {

  val tpchGen = new TpchGen {

    val baseDir: String = base
    val scaleDir = scale

    val sqlContext = sqlCtx

    override val cacheViews: Boolean = false
    //override val cacheRawData : Boolean = true

    sqlContext.udf.register("concat", Functions.concat _)

  }

  import tpchGen._

  def loadShow {
    rawDFs.foreach { df =>
      println(df.count())
      df.show(100)
    }
  }

  def debugData {
    println(rawCustomerDF.count())
    println(customerDF.count())
    //customerDF.show(100)

    println(rawSupplierDF.count())
    println(supplierDF.count())
    //supplierDF.show(100)

    println(rawPartSupplierDF.count())
    println(partSuppDF.count())
    partSuppDF.show(100)
    //
    //    println(lineitemDF.count())
    //    println(orderLineItemPartSupplierDF.queryExecution.sparkPlan.toString())
    //    val s = System.currentTimeMillis()
    //    println(orderLineItemPartSupplierDF.show(100))
    //    val e = System.currentTimeMillis()
    //    println(e - s)
    //orderLineItemDF.show(100)


    //    tpch.sqlContext.sparkContext.getExecutorMemoryStatus


  }

  def createDenormalizedData {
    import com.databricks.spark.csv._

    orderLineItemPartSupplierCustomerDF.saveAsCsvFile(
      tpchGen.tableDir("orderLineItemPartSupplierCustomer"),
      Map("delimiter" -> "|") /*,
    classOf[GzipCodec]*/) // haven't figured out indexsvc reading gzipped files.

  }

  def showData {
    orderLineItemPartSupplierDF.show(100)
  }

  def functionResolution {
    rawSupplierDF
    val df1 = TPCHSQLContext.sql("select * from rawsupplier where s_name like 's%'")
    println(df1.count())
  }

  def printSchema {
    orderLineItemPartSupplierCustomerDF.schema.printTreeString
  }

  def lineDFShow {
    lineitemDF.show(100)
  }

  def loadDenormData {
    val ol = dfFromFile("orderLineItemPartSupplierCustomer", "orderLineItemPartSupplierCustomer",
      orderLineItemPartSupplierDF.schema)
    println(ol.schema)
    println(ol.count)
    ol.show(100)
  }


}
