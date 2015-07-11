package org.sparklinedata.tpch

import scala.language.implicitConversions

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLConf, SQLContext}

/** A SQLContext that can be used for local testing. */
class LocalSQLContext
  extends SQLContext({
    val sc = new SparkContext(
    "local[*]",
    "TPCHSQLContext", {
      val conf = new SparkConf().
        set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
        set("spark.sql.autoBroadcastJoinThreshold", (100 * 1024 * 1024).toString).
        set("spark.sql.planner.externalSort", "true").
        set("spark.driver.memory", "4g")
      //set("spark.sql.planner.sortMergeJoin", "true")
      conf
    }
    )
    //sc.setLogLevel("DEBUG")
    sc
  }) {

}

object TPCHSQLContext extends LocalSQLContext

