package com.databricks.spark.csv.tpch.sparkshell

import org.scalatest.FunSuite

import com.databricks.spark.csv.tpch.TPCHSQLContext._

class QueriesSuite extends FunSuite {

  test("load") {
    val ol = sql(
      s"""CREATE TEMPORARY TABLE orderLineItemPartSupplier(o_orderkey integer, o_custkey integer, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority integer, o_comment string, l_partkey integer, l_suppkey integer, l_linenumber integer, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string, l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string, l_comment string, order_year string, ps_partkey integer, ps_suppkey integer, ps_availqty integer, ps_supplycost double, ps_comment string, s_name string, s_address string, s_phone string, s_acctbal double, s_comment string, s_nation string, s_region string, p_name string, p_mfgr string, p_brand string, p_type string, p_size integer, p_container string, p_retailprice double, p_comment string)
USING com.databricks.spark.csv
OPTIONS (path "/Users/hbutani/tpch/datascale1/orderLineItemPartSupplier/", header "false", delimiter "|")"""
    )

    println(ol.queryExecution.logical.treeString)
    println(ol.queryExecution.sparkPlan.treeString)
    println(ol.rdd.toDebugString)

    val d = sql("select * from orderLineItemPartSupplier")

    d.show(100)
  }
}