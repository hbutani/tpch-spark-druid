package org.sparklinedata.tpch

import com.databricks.spark.csv.tpch.CsvUtil
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.plans.logical.RepartitionByExpression
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext}


trait TpchGen {

  def sqlContext: SQLContext

  import com.databricks.spark.csv._

  val baseDir: String
  val scaleDir : String

  def cacheRawData = false

  def cacheViews = true

  val delimiter = '|'
  val quote: Char = '"'
  val escape: Character = null
  val parseMode: String = "PERMISSIVE"
  val parserLib: String = "COMMONS"
  val ignoreLeadingWhiteSpace: Boolean = false
  val ignoreTrailingWhiteSpace: Boolean = false

  def tableDir(tName: String) = s"$baseDir/$scaleDir/$tName/"

  def csvRelation(tableName: String, tableSchema: StructType): CsvRelation = {
    CsvUtil.csvRelation(
      tableDir(tableName),
      false,
      delimiter,
      quote,
      escape,
      parseMode,
      parserLib,
      ignoreLeadingWhiteSpace,
      ignoreTrailingWhiteSpace,
      tableSchema)(sqlContext)
  }

  def dfFromFile(fileName: String, tableName: String, tableSchema: StructType,
                 partitionBy: Seq[String] = Seq()) = {
    val s = System.currentTimeMillis()
    var df = sqlContext.baseRelationToDataFrame(csvRelation(fileName, tableSchema))
    if (partitionBy.size > 0) {
      val attrs = partitionBy.map(UnresolvedAttribute(_))
      df = new DataFrame(df.sqlContext, RepartitionByExpression(attrs, df.queryExecution.logical))
    }
    df.registerTempTable(tableName)
    if (cacheRawData) {
      df.cache()
      df.count
    }

    val e = System.currentTimeMillis()
    println(tableName, "Num Partitions:", df.rdd.partitions.size)
    println(s"\t time to cache : ${(e - s)}")
    df
  }

  def dfFromSql(tableName: String, sql: String, numPartitions: Option[Int] = None) = {
    val s = System.currentTimeMillis()
    val df = numPartitions.map(n => sqlContext.sql(sql).repartition(n)).getOrElse(sqlContext.sql(sql))
    df.registerTempTable(tableName)
    if (cacheViews) {
      df.cache()
      df.count()
    }
    val e = System.currentTimeMillis()
    println(tableName, "Num Partitions:", df.rdd.partitions.size)
    println(s"\t time to cache : ${(e - s)}")
    df
  }

  val regionSchema = StructType(Seq(
    StructField("r_regionkey", IntegerType),
    StructField("r_name", StringType),
    StructField("r_comment", StringType)
  ))

  lazy val regionDF = dfFromFile("region", "region", regionSchema)

  val nationSchema = StructType(Seq(
    StructField("n_nationkey", IntegerType),
    StructField("n_name", StringType),
    StructField("n_regionkey", IntegerType),
    StructField("n_comment", StringType)
  ))

  lazy val nationDF = dfFromFile("nation", "nation", nationSchema)

  val rawCustomerSchema = StructType(Seq(
    StructField("c_custkey", IntegerType),
    StructField("c_name", StringType),
    StructField("c_address", StringType),
    StructField("c_nationkey", IntegerType),
    StructField("c_phone", StringType),
    StructField("c_acctbal", DoubleType),
    StructField("c_mktsegment", StringType),
    StructField("c_comment", StringType)
  ))

  lazy val rawCustomerDF = dfFromFile("customer", "rawCustomer", rawCustomerSchema)

  val partSchema = StructType(Seq(
    StructField("p_partkey", IntegerType),
    StructField("p_name", StringType),
    StructField("p_mfgr", StringType),
    StructField("p_brand", StringType),
    StructField("p_type", StringType),
    StructField("p_size", IntegerType),
    StructField("p_container", StringType),
    StructField("p_retailprice", DoubleType),
    StructField("p_comment", StringType)
  ))

  lazy val partDF = dfFromFile("part", "part", partSchema)

  val rawSupplierSchema = StructType(Seq(
    StructField("s_suppkey", IntegerType),
    StructField("s_name", StringType),
    StructField("s_address", StringType),
    StructField("s_nationkey", IntegerType),
    StructField("s_phone", StringType),
    StructField("s_acctbal", DoubleType),
    StructField("s_comment", StringType)
  ))

  lazy val rawSupplierDF = dfFromFile("supplier", "rawSupplier", rawSupplierSchema)

  val rawPartSupplierSchema = StructType(Seq(
    StructField("ps_partkey", IntegerType),
    StructField("ps_suppkey", IntegerType),
    StructField("ps_availqty", IntegerType),
    StructField("ps_supplycost", DoubleType),
    StructField("ps_comment", StringType)))

  lazy val rawPartSupplierDF = dfFromFile("partsupp", "rawPartsupp", rawPartSupplierSchema)

  val orderSchema = StructType(Seq(
    StructField("o_orderkey", IntegerType),
    StructField("o_custkey", IntegerType),
    StructField("o_orderstatus", StringType),
    StructField("o_totalprice", DoubleType),
    StructField("o_orderdate", StringType),
    StructField("o_orderpriority", StringType),
    StructField("o_clerk", StringType),
    StructField("o_shippriority", IntegerType),
    StructField("o_comment", StringType)
  ))

  lazy val orderDF = dfFromFile("orders", "orders", orderSchema)

  val lineitemSchema = StructType(Seq(
    StructField("l_orderkey", IntegerType),
    StructField("l_partkey", IntegerType),
    StructField("l_suppkey", IntegerType),
    StructField("l_linenumber", IntegerType),
    StructField("l_quantity", DoubleType),
    StructField("l_extendedprice", DoubleType),
    StructField("l_discount", DoubleType),
    StructField("l_tax", DoubleType),
    StructField("l_returnflag", StringType),
    StructField("l_linestatus", StringType),
    StructField("l_shipdate", StringType),
    StructField("l_commitdate", StringType),
    StructField("l_receiptdate", StringType),
    StructField("l_shipinstruct", StringType),
    StructField("l_shipmode", StringType),
    StructField("l_comment", StringType)
  ))

  lazy val lineitemDF = {
    val d = dfFromFile("lineitem", "lineitem", lineitemSchema)
    sqlContext.setConf("spark.sql.shuffle.partitions", d.rdd.partitions.size.toString)
    d
  }

  lazy val customerDF = {
    nationDF
    regionDF
    rawCustomerDF
    dfFromSql("customer",
      """ select
      |    c_custkey,
      |    c_name,
      |    c_address,
      |    c_phone,
      |    c_acctbal,
      |    c_mktsegment,
      |    c_comment,
      |    n_name as c_nation,
      |    r_name as c_region
      |from rawCustomer c join nation n on c.c_nationkey = n.n_nationkey join region r on n.n_regionkey = r.r_regionkey
    """.stripMargin)
    }

  lazy val
  supplierDF = {
    rawSupplierDF
    nationDF
    regionDF
    dfFromSql("supplier",
      """select
      |    s_suppkey,
      |    s_name,
      |    s_address,
      |    s_phone,
      |    s_acctbal,
      |    s_comment,
      |    n_name as s_nation,
      |    r_name as s_region
      | from rawSupplier s join nation n on s.s_nationkey = n.n_nationkey join region r on n.n_regionkey = r.r_regionkey
      |
    """.
        stripMargin)
    }

  lazy val partSuppDF = {
    rawPartSupplierDF
    partDF
    supplierDF
    dfFromSql("partsupplier",
      """select
        |    ps_partkey,
        |    ps_suppkey,
        |    ps_availqty,
        |    ps_supplycost,
        |    ps_comment,
        |    s_name,
        |    s_address,
        |    s_phone,
        |    s_acctbal,
        |    s_comment,
        |    s_nation,
        |    s_region,
        |    p_name,
        |    p_mfgr,
        |    p_brand,
        |    p_type,
        |    p_size,
        |    p_container,
        |    p_retailprice,
        |    p_comment
        |from rawPartsupp ps join part p on ps.ps_partkey = p.p_partkey
        |                join supplier s on ps.ps_suppkey = s.s_suppkey
      """.stripMargin)
  }

  lazy val orderLineItemDF = {
    orderDF
    lineitemDF
    dfFromSql("order_lineitem",
      """select
        |    o_orderkey,
        |    o_custkey,
        |    o_orderstatus,
        |    o_totalprice,
        |    concat(o_orderdate, "T00:00:00.000Z") as o_orderdate,
        |    o_orderpriority,
        |    o_clerk,
        |    o_shippriority,
        |    o_comment,
        |    l_partkey,
        |    l_suppkey,
        |    l_linenumber,
        |    l_quantity,
        |    l_extendedprice,
        |    l_discount,
        |    l_tax,
        |    l_returnflag,
        |    l_linestatus,
        |    l_shipdate,
        |    l_commitdate,
        |    l_receiptdate,
        |    l_shipinstruct,
        |    l_shipmode,
        |    l_comment,
        |    substring(o_orderdate, 0, 4) as order_year
        | from orders o join lineitem l on o.o_orderkey = l.l_orderkey
      """.stripMargin)
  } //, Some(lineitemDF.rdd.partitions.size))

  lazy val orderLineItemPartSupplierDF = {
    partSuppDF
    orderLineItemDF
    dfFromSql("order_lineitem_part_supplier",
      """select
        |    o_orderkey,
        |    o_custkey,
        |    o_orderstatus,
        |    o_totalprice,
        |    o_orderdate,
        |    o_orderpriority,
        |    o_clerk,
        |    o_shippriority,
        |    o_comment,
        |    l_partkey,
        |    l_suppkey,
        |    l_linenumber,
        |    l_quantity,
        |    l_extendedprice,
        |    l_discount,
        |    l_tax,
        |    l_returnflag,
        |    l_linestatus,
        |    l_shipdate,
        |    l_commitdate,
        |    l_receiptdate,
        |    l_shipinstruct,
        |    l_shipmode,
        |    l_comment,
        |    order_year,
        |    ps_partkey,
        |    ps_suppkey,
        |    ps_availqty,
        |    ps_supplycost,
        |    ps_comment,
        |    s_name,
        |    s_address,
        |    s_phone,
        |    s_acctbal,
        |    s_comment,
        |    s_nation,
        |    s_region,
        |    p_name,
        |    p_mfgr,
        |    p_brand,
        |    p_type,
        |    p_size,
        |    p_container,
        |    p_retailprice,
        |    p_comment
        |
        | from order_lineitem ol join partsupplier ps
        |       on ol.l_partkey = ps.ps_partkey and ol.l_suppkey = ps.ps_suppkey
      """.stripMargin
    ) //, Some(lineitemDF.rdd.partitions.size))
  }

    lazy val orderLineItemPartSupplierCustomerDF = {
      customerDF
      orderLineItemPartSupplierDF
      dfFromSql("order_lineitem_part_supplier_customer",
        """select
          |    o_orderkey,
          |    o_custkey,
          |    o_orderstatus,
          |    o_totalprice,
          |    o_orderdate,
          |    o_orderpriority,
          |    o_clerk,
          |    o_shippriority,
          |    o_comment,
          |    l_partkey,
          |    l_suppkey,
          |    l_linenumber,
          |    l_quantity,
          |    l_extendedprice,
          |    l_discount,
          |    l_tax,
          |    l_returnflag,
          |    l_linestatus,
          |    l_shipdate,
          |    l_commitdate,
          |    l_receiptdate,
          |    l_shipinstruct,
          |    l_shipmode,
          |    l_comment,
          |    order_year,
          |    ps_partkey,
          |    ps_suppkey,
          |    ps_availqty,
          |    ps_supplycost,
          |    ps_comment,
          |    s_name,
          |    s_address,
          |    s_phone,
          |    s_acctbal,
          |    s_comment,
          |    s_nation,
          |    s_region,
          |    p_name,
          |    p_mfgr,
          |    p_brand,
          |    p_type,
          |    p_size,
          |    p_container,
          |    p_retailprice,
          |    p_comment,
          |    c_name,
          |    c_address,
          |    c_phone,
          |    c_acctbal,
          |    c_mktsegment,
          |    c_comment,
          |    c_nation,
          |    c_region
          | from order_lineitem_part_supplier olps join customer c
          |       on olps.o_custkey = c.c_custkey
        """.stripMargin
      )
  }
  //lineitem fix date
  // line join order join customer join partsupp

  lazy
  val rawDFs =
    Seq(regionDF, nationDF, rawCustomerDF, partDF, rawSupplierDF, rawPartSupplierDF, orderDF, lineitemDF)
}


