package org.sparklinedata.tpch

import org.scalatest.FunSuite

class TPCHGenTest extends FunSuite {

  test("1") {
    val tpchGenMain = new TpchGenMain(TPCHSQLContext,
    "/Users/hbutani/tpch/",
    "datascale1")

    //tpchGenMain.loadShow

    //tpchGenMain.debugData

    tpchGenMain.loadDenormData
  }

}
