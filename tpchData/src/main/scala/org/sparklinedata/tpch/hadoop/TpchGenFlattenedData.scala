package org.sparklinedata.tpch.hadoop

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.sparklinedata.tpch.Functions


case class Config(tpchDB : String = "", outputDir : String = "")

object TpchGenFlattenedData {

  def main(args: Array[String]) {

    /*
     - tpchDB
     - outputDir
     */

    val parser = new scopt.OptionParser[Config]("tpchGenFlat") {
      head("tpchGenFlat", "0.1")
      opt[String]('d', "tpchDB") required() valueName("<dbName>") action { (x, c) =>
        c.copy(tpchDB = x) } text("the source tpch db")
      opt[String]('o', "outputDir") required() valueName("<outputDir>") action { (x, c) =>
        c.copy(outputDir = x) } text("the source tpch db")
      help("help") text("prints this usage text")
    }

    val c = parser.parse(args, Config()) match {
      case Some(config) => config

      case None => sys.exit(-1)
    }

    val sc = new SparkContext(new SparkConf().setAppName("TPCHGen Flattened Data"))

    val sqlCtx = new HiveContext(sc)

    sqlCtx.sql(s"use ${c.tpchDB}").collect()

    val tpchGen = new TpchGen {

      val outputDir: String = c.outputDir

      val sqlContext = sqlCtx

      override val cacheViews: Boolean = false

      sqlContext.udf.register("concat", Functions.concat _)

    }

    import tpchGen._

    import com.databricks.spark.csv._

    orderLineItemPartSupplierCustomerDF.saveAsCsvFile(
      tpchGen.tableDir("orderLineItemPartSupplierCustomer"),
      Map("delimiter" -> "|")
    )
  }
}
