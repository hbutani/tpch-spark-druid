package com.databricks.spark.csv.tpch

import com.databricks.spark.csv.CsvRelation
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType

object CsvUtil {

  def csvRelation(location: String,
                 useHeader : Boolean,
                  delimiter: Char,
                  quote: Char,
                  escape: Character,
                  parseMode: String,
                  parserLib: String,
                  ignoreLeadingWhiteSpace: Boolean,
                  ignoreTrailingWhiteSpace: Boolean,
                  tableSchema: StructType)(sqlContext : SQLContext): CsvRelation = {
    CsvRelation(
      location,
      useHeader,
      delimiter,
      quote,
      escape,
      parseMode,
      parserLib,
      ignoreLeadingWhiteSpace,
      ignoreTrailingWhiteSpace,
      tableSchema)(sqlContext)
  }

}
