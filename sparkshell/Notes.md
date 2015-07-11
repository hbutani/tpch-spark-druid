
## Start shell
```sh
export JAVA_OPTS="-Xmx8g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"
bin/spark-shell --executor-memory 4G --properties-file  /Users/hbutani/sparkline/spark-csv/src/test/scala/com/databricks/spark/csv/tpch/sparkshell/spark.properties --packages com.databricks:spark-csv_2.10:1.1.0 --jars /Users/hbutani/sparkline/spark-datetime/target/scala-2.10/spark-datetime-assembly-0.0.1.jar
```

### load data
```scala
sql(
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
OPTIONS (path "/Users/hbutani/tpch/datascale1/orderLineItemPartSupplierCustomer/", header "false", delimiter "|")"""
    )
var df = sqlContext.table("orderLineItemPartSupplierBase")
df = df.coalesce(40)
df.registerTempTable("orderLineItemPartSupplier")
df.cache

import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.sparklinedata.spark.dateTime.dsl.expressions._
import org.sparklinedata.spark.dateTime.Functions._
import org.sparklinedata.spark.dateTime.Utils
import org.joda.time.format.{DateTimeFormatter, ISODateTimeFormat}
register(sqlContext)


```

## Queries

### Q1, Pricing Summary Report
```scala
val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1998-12-01") - 90.day)

val q1 = sql(date"""
select
l_returnflag,
l_linestatus,
sum(l_quantity) as sum_qty,
sum(l_extendedprice) as sum_base_price,
sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
avg(l_quantity) as avg_qty,
avg(l_extendedprice) as avg_price,
avg(l_discount) as avg_disc,
count(*) as count_order
from
orderLineItemPartSupplier
where
$shipDtPredicate
group by
l_returnflag,
l_linestatus
order by
l_returnflag,
l_linestatus
""")

q1.show(4)
```

### Q3, Shipping Priority
```scala
val orderDtPredicate = dateTime('o_orderdate) < dateTime("1995-03-15")
val shipDtPredicate = dateTime('l_shipdate) > dateTime("1995-03-15")

val q3 = sql(date"""
select
o_orderkey,
sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate,
o_shippriority
from
orderLineItemPartSupplier where
c_mktsegment = 'BUILDING' and $orderDtPredicate and $shipDtPredicate
group by o_orderkey,
o_orderdate,
o_shippriority order by
revenue desc, o_orderdate
limit 10
""")

q3.show()
```

### Q4, Order Priority Checking
- hoist the lineitem exists check to the Outer Query Block.
- convert the count(*) into a count distinct

```scala
val orderDtPredicate1 = dateTime('o_orderdate) >= dateTime("1993-07-01")
val orderDtPredicate2 = dateTime('o_orderdate) < (dateTime("1993-07-01") + 3.months)

val q4 = sql(date"""
select
o_orderpriority,
count(distinct o_orderkey) as order_count 
from
orderLineItemPartSupplier where
$orderDtPredicate1 and $orderDtPredicate2 and l_commitdate < l_receiptdate
group by o_orderpriority 
order by o_orderpriority
""")

q4.show()
```