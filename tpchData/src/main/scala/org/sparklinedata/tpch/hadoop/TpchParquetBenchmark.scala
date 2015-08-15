package org.sparklinedata.tpch.hadoop

import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.sparklinedata.spark.dateTime.Functions._
import org.sparklinedata.spark.dateTime.dsl.expressions._

import scala.collection.mutable.ArrayBuffer
import scala.language.postfixOps

case class BenchmarkConfig(tpchFlatDir: String = "")

object TpchParquetBenchmark {

  def registerBaseDF(sqlCtx: SQLContext, cfg: BenchmarkConfig): Unit = {
    val df = sqlCtx.read.parquet(cfg.tpchFlatDir)
    df.cache()
    df.registerTempTable("orderLineItemPartSupplier")
  }

  def queries(sqlCtx: SQLContext): Seq[(String, DataFrame)] = {

    val basicAgg = ("Basic Aggregation",
      sqlCtx.sql( s"""select l_returnflag, l_linestatus, count(*),
        sum(l_extendedprice) as s, max(ps_supplycost) as m,
        avg(ps_availqty) as a,count(distinct o_orderkey)
          from orderLineItemPartSupplier
          group by l_returnflag, l_linestatus"""))

    val shipDtPredicateA = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)


    val intervalAndDimFilters = ("ShipDate range + Nation predicate",
      sqlCtx.sql(
        date"""
      select f, s, count(*) as count_order
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate, s_region, s_nation, c_nation,
          shipYear, shipMonth
         from orderLineItemPartSupplier
      ) t
      where
        (shipYear <= 1997)
       and $shipDtPredicateA
       and ((s_nation = 'FRANCE' and c_nation = 'GERMANY') or
                                  (c_nation = 'FRANCE' and s_nation = 'GERMANY')
                                 )
      group by f,s
      order by f,s
""")
      )

    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)
    val shipDtPredicate2 = dateTime('l_shipdate) > (dateTime("1995-12-01"))

    val shipDteRange = ("Ship Date Range",

      sqlCtx.sql(
        date"""
      select f, s, count(*) as count_order
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate, s_region, s_nation, c_nation,
          shipYear, shipMonth
         from orderLineItemPartSupplier
      ) t
      where
       shipYear <= 1997
       and shipYear >= 1995
       and $shipDtPredicate and $shipDtPredicate2
      group by f,s
      order by f,s"""
      )
      )

    val shipDtPredicateL = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)
    val shipDtPredicateH = dateTime('l_shipdate) > (dateTime("1995-12-01"))

    val projFiltRange = ("SubQuery + nation,Type predicates + ShipDate Range",
      sqlCtx.sql(
        date"""
      select s_nation,
      count(*) as count_order,
      sum(l_extendedprice) as s,
      max(ps_supplycost) as m,
      avg(ps_availqty) as a,
      count(distinct o_orderkey)
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate,
         s_region, s_nation, c_nation, p_type,
         l_extendedprice, ps_supplycost, ps_availqty, o_orderkey,
         shipYear, shipMonth
         from orderLineItemPartSupplier
         where p_type = 'ECONOMY ANODIZED STEEL'
      ) t
      where
       shipYear <= 1997
       and shipYear >= 1995
       and
          $shipDtPredicateL and
            $shipDtPredicateH and ((s_nation = 'FRANCE' and c_nation = 'GERMANY') or
                                  (c_nation = 'FRANCE' and s_nation = 'GERMANY')
                                 )
      group by s_nation
      order by s_nation
"""))

    val q1 = ("TPCH Q1",
      sqlCtx.sql( """select l_returnflag, l_linestatus, count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m,
       avg(ps_availqty) as a,count(distinct o_orderkey)
       from orderLineItemPartSupplier
       group by l_returnflag, l_linestatus""")
      )

    Seq(basicAgg, shipDteRange, projFiltRange, q1)
  }

  def init(sqlCtx: SQLContext, cfg: BenchmarkConfig): Unit = {
    register(sqlCtx)
    registerBaseDF(sqlCtx, cfg)
  }

  def run(sqlCtx : SQLContext, c : BenchmarkConfig) : Unit = {
    init(sqlCtx, c)

    val qs = queries(sqlCtx)

    val results: Seq[(String, DataFrame, ArrayBuffer[Long])] =
      qs.map(q => (q._1, q._2, ArrayBuffer[Long]()))

    (0 to 5).foreach { i =>
      results.foreach { q =>
        println("running " + q._1)
        val df: DataFrame = q._2
        val times: ArrayBuffer[Long] = q._3
        val sTime = System.currentTimeMillis()
        df.collect()
        val eTime = System.currentTimeMillis()
        times += (eTime - sTime)
      }
    }

    val headings = Seq("Query", "Avg. Time", "Min. Time", "Max. Time")
    println(f"${headings(0)}%50s${headings(1)}%10s${headings(2)}%10s${headings(3)}%10s")
    results.foreach { r =>
      val qName = r._1
      val times: ArrayBuffer[Long] = r._3
      val minTime = times.min
      val maxTime = times.max
      val avgTime = times.sum / times.size

      println(f"$qName%50s $avgTime%10.3f $minTime%10d $maxTime%10d")
    }

  }

  def main(args: Array[String]) {

    val parser = new scopt.OptionParser[BenchmarkConfig]("tpchBenchmark") {
      head("tpchBenchmark", "0.1")
      opt[String]('t', "tpchFlatDir") required() valueName ("<tpchDir>") action { (x, c) =>
        c.copy(tpchFlatDir = x)
      } text ("the folder containing tpch flattened data")
      help("help") text ("prints this usage text")
    }

    val c = parser.parse(args, BenchmarkConfig()) match {
      case Some(config) => config
      case None => sys.exit(-1)
    }

    val sc = new SparkContext(new SparkConf().setAppName("TPCHBenchmark"))

    val sqlCtx = new SQLContext(sc)

    run(sqlCtx, c)
  }

}