package org.sparklinedata.tpch.hadoop

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.plans.logical.RepartitionByExpression
import org.apache.spark.{SparkConf, SparkContext}
import org.sparklinedata.spark.dateTime.Functions._
import org.sparklinedata.spark.dateTime.dsl.expressions._

import scala.language.postfixOps

case class ParquetBuildConfig(tpchFlatDir: String = "",
                               outputDir : String = "")

object TpchBuildParquetPartitioned {

  def registerBaseDF(sqlCtx: SQLContext, cfg: ParquetBuildConfig): Unit = {
    sqlCtx.sql(
      s"""CREATE TEMPORARY TABLE orderLineItemPartSupplierBase(o_orderkey integer, o_custkey integer,
      o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string,
      o_shippriority integer, o_comment string, l_partkey integer, l_suppkey integer, l_linenumber integer,
      l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,
      l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string,
      l_shipmode string, l_comment string, order_year string, ps_partkey integer, ps_suppkey integer,
      ps_availqty integer, ps_supplycost double, ps_comment string, s_name string, s_address string,
      s_phone string, s_acctbal double, s_comment string, s_nation string, s_region string, p_name string,
      p_mfgr string, p_brand string, p_type string, p_size integer, p_container string, p_retailprice double,
      p_comment string, c_name string , c_address string , c_phone string , c_acctbal double ,
      c_mktsegment string , c_comment string , c_nation string , c_region string)
USING com.databricks.spark.csv
OPTIONS (path "tpchFlattenedData_10/orderLineItemPartSupplierCustomer", header "false", delimiter "|")"""
    )

  }


  def init(sqlCtx: SQLContext, cfg: ParquetBuildConfig): Unit = {
    register(sqlCtx)
    registerBaseDF(sqlCtx, cfg)
  }

  def main(args: Array[String]) {

    val parser = new scopt.OptionParser[ParquetBuildConfig]("tpchBenchmark") {
      head("tpchBenchmark", "0.1")
      opt[String]('t', "tpchFlatDir") required() valueName ("<tpchDir>") action { (x, c) =>
        c.copy(tpchFlatDir = x)
      } text ("the folder containing tpch flattened data")
      opt[String]('p', "outputDir") required() valueName ("<parquetDir>") action { (x, c) =>
        c.copy(outputDir = x)
      } text ("the output folder")
      help("help") text ("prints this usage text")
    }

    val c = parser.parse(args, ParquetBuildConfig()) match {
      case Some(config) => config
      case None => sys.exit(-1)
    }

    val sc = new SparkContext(new SparkConf().setAppName("TPCHBenchmark"))

    val sqlCtx = new SQLContext(sc)

    init(sqlCtx, c)

    register(sqlCtx)

    val shipYear = dateTime('l_shipDate) year
    val shipMonth = dateTime('l_shipDate) monthOfYear


    val df2 = sqlCtx.sql(
      date"select t.*, $shipYear as shipYear, $shipMonth as shipMonth from orderLineItemPartSupplierBase t")

    val lP = RepartitionByExpression(Seq('shipYear, 'shipMonth), df2.queryExecution.logical)
    val df3 = new DataFrame(sqlCtx, lP)

    df3.write.partitionBy("shipYear", "shipMonth").
      parquet("tpchFlattenedData_10/orderLineItemPartSupplierCustomer.parquet.partitioned")

  }

}