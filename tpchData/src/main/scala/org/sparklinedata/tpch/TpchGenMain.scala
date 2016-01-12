package org.sparklinedata.tpch

import org.apache.spark.sql.hive.HiveContext
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

case class Config(baseDir : String = "", scale : String = "")

/**
 * Program to create a flattened tpch dataset using spark-submit. Sample Usage:
  * {{{
  *   bin/spark-submit --packages com.databricks:spark-csv_2.10:1.1.0,SparklineData:spark-datetime:0.0.2 \
  *   --jars /Users/hbutani/sparkline/spark-druid-olap/target/scala-2.10/spark-druid-olap-assembly-0.0.1.jar \
  *   --class org.sparklinedata.tpch.TpchGenMain \
*   /Users/hbutani/sparkline/tpch-spark-druid/tpchData/target/scala-2.10/tpchdata-assembly-0.0.1.jar \
  *   --baseDir /Users/hbutani/tpch/ --scale 1
  * }}}
 *
 */
object TpchGenMain {

  def main(args: Array[String]): Unit = {

    val parser = new scopt.OptionParser[Config]("tpchGenFlat") {
      head("tpchGenFlat", "0.1")
      opt[String]('d', "baseDir") required() valueName("<dir>") action { (x, c) =>
        c.copy(baseDir = x) } text("the base dir for tpch data")
      opt[Int]('s', "scale") required() valueName("<dataScale>") action { (x, c) =>
        c.copy(scale = s"datascale$x") } text("the scale of data(1/10/100/...)")
      help("help") text("prints this usage text")
    }

    val c = parser.parse(args, Config()) match {
      case Some(config) => config

      case None => sys.exit(-1)
    }

    val sc = new SparkContext(new SparkConf().setAppName("TPCHGen Flattened Data"))

    val sqlCtx = new HiveContext(sc)

    val tpchGenMain = new TpchGenMain(sqlCtx,
      c.baseDir,
      c.scale)

    tpchGenMain.createDenormalizedData
  }

}