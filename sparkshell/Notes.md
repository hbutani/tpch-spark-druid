
## Start shell
```sh
export SPARK_SUBMIT_OPTS="-Xmx8g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"
bin/spark-shell --executor-memory 4G --properties-file  /Users/hbutani/sparkline/spark-csv/src/test/scala/com/databricks/spark/csv/tpch/sparkshell/spark.properties --packages com.databricks:spark-csv_2.10:1.1.0 --jars /Users/hbutani/sparkline/spark-datetime/target/scala-2.10/spark-datetime-assembly-0.0.1.jar
```

### load data


```
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

#### Spark SQL Plan
```
ExternalSort [l_returnflag#16 ASC,l_linestatus#17 ASC], true
 Aggregate false, [l_returnflag#16,l_linestatus#17], 
            [l_returnflag#16,l_linestatus#17,
             CombineSum(PartialSum#1562) AS sum_qty#318,
             CombineSum(PartialSum#1563) AS sum_base_price#319,
             CombineSum(PartialSum#1564) AS sum_disc_price#320,
             CombineSum(PartialSum#1565) AS sum_charge#321,
             (CAST(SUM(PartialSum#1566), DoubleType) / CAST(SUM(PartialCount#1567L), DoubleType)) AS avg_qty#322,
             (CAST(SUM(PartialSum#1568), DoubleType) / CAST(SUM(PartialCount#1569L), DoubleType)) AS avg_price#323,
             (CAST(SUM(PartialSum#1570), DoubleType) / CAST(SUM(PartialCount#1571L), DoubleType)) AS avg_disc#324,
             Coalesce(SUM(PartialCount#1572L),0) AS count_order#325L]
  Aggregate true, [l_returnflag#16,l_linestatus#17], 
                  [l_returnflag#16,l_linestatus#17,
                  SUM((l_extendedprice#13 * (1.0 - l_discount#14))) AS PartialSum#1564,
                  SUM(l_quantity#12) AS PartialSum#1562,
                  COUNT(l_quantity#12) AS PartialCount#1567L,
                  SUM(l_quantity#12) AS PartialSum#1566,
                  COUNT(l_discount#14) AS PartialCount#1571L,
                  SUM(l_discount#14) AS PartialSum#1570,
                  SUM(l_extendedprice#13) AS PartialSum#1563,
                  COUNT(l_extendedprice#13) AS PartialCount#1569L,
                  SUM(l_extendedprice#13) AS PartialSum#1568,
                  COUNT(1) AS PartialCount#1572L,
                  SUM(((l_extendedprice#13 * (1.0 - l_discount#14)) * (1.0 + l_tax#15))) AS PartialSum#1565]
   Project [l_returnflag#16,l_linestatus#17,l_discount#14,l_tax#15,l_extendedprice#13,l_quantity#12]
    Filter scalaUDF(scalaUDF(l_shipdate#18),scalaUDF(scalaUDF(1998-12-01),scalaUDF(P90D)))
     InMemoryColumnarTableScan [l_returnflag#16,l_linestatus#17,l_discount#14,l_shipdate#18,l_tax#15,l_extendedprice#13,l_quantity#12], [scalaUDF(scalaUDF(l_shipdate#18),scalaUDF(scalaUDF(1998-12-01),scalaUDF(P90D)))], (InMemoryRelation [o_orderkey#0,o_custkey#1,o_orderstatus#2,o_totalprice#3,o_orderdate#4,o_orderpriority#5,o_clerk#6,o_shippriority#7,o_comment#8,l_partkey#9,l_suppkey#10,l_linenumber#11,l_quantity#12,l_extendedprice#13,l_discount#14,l_tax#15,l_returnflag#16,l_linestatus#17,l_shipdate#18,l_commitdate#19,l_receiptdate#20,l_shipinstruct#21,l_shipmode#22,l_comment#23,order_year#24,ps_partkey#25,ps_suppkey#26,ps_availqty#27,ps_supplycost#28,ps_comment#29,s_name#30,s_address#31,s_phone#32,s_acctbal#33,s_comment#34,s_nation#35,s_region#36,p_name#37,p_mfgr#38,p_brand#39,p_type#40,p_size#41,p_container#42,p_retailprice#43,p_comment#44,c_name#45,c_address#46,c_phone#47,c_acctbal#48,c_mktsegment#49,c_comment#50,c_nation#51,c_region#52], true, 10000, StorageLevel(true, true, false, true, 1), (Repartition 40, false), None)
```

#### Translation
- TableScan is on OLAP Index
- Filter gets translated to an Interval.
- Aggregate:
  - Grouping Expressions to Dimension clauses
  - sum_qty#318 -> Aggregate
  - sum_disc_price -> JSAggregate (sum expressions are arithmetic operators)
  - avg_qty, avg_price, avg_disc -> PostAggregate
  
There is no Limit, so ExternalSort leave in Plan.

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

#### SQL Spark Plan
```
TakeOrdered 10, [revenue#1595 DESC,o_orderdate#4 ASC]
 Aggregate false, [o_orderkey#0,o_orderdate#4,o_shippriority#7], 
     [o_orderkey#0,CombineSum(PartialSum#1862) AS revenue#1595,
     o_orderdate#4,o_shippriority#7]
  Aggregate true, [o_orderkey#0,o_orderdate#4,o_shippriority#7], 
       [o_orderkey#0,o_orderdate#4,o_shippriority#7,
       SUM((l_extendedprice#13 * (1.0 - l_discount#14))) AS PartialSum#1862]
   Project [o_orderdate#4,o_shippriority#7,l_discount#14,l_extendedprice#13,o_orderkey#0]
    Filter (((c_mktsegment#49 = BUILDING) && scalaUDF(scalaUDF(o_orderdate#4),scalaUDF(1995-03-15))) && scalaUDF(scalaUDF(l_shipdate#18),scalaUDF(1995-03-15)))
     InMemoryColumnarTableScan [o_orderdate#4,o_shippriority#7,l_discount#14,c_mktsegment#49,l_shipdate#18,...
```

#### Translation
- Filter translates to Interval and Filter clauses
- revenue -> as JSAggregate
- o_orderkey is not projected so add a projection on to of OLAP Index Scan.
- TakeOrdered to LimitSpec.

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

#### Spark SQL Plan
```
ExternalSort [o_orderpriority#5 ASC], true
 Aggregate false, [o_orderpriority#5], [o_orderpriority#5,CombineAndCount(partialSets#2402) AS order_count#1865L]
  Aggregate true, [o_orderpriority#5], [o_orderpriority#5,AddToHashSet(o_orderkey#0) AS partialSets#2402]
   Project [o_orderpriority#5,o_orderkey#0]
    Filter ((scalaUDF(scalaUDF(o_orderdate#4),scalaUDF(1993-07-01)) && scalaUDF(scalaUDF(o_orderdate#4),scalaUDF(scalaUDF(1993-07-01),scalaUDF(P3M)))) && (l_commitdate#19 < l_receiptdate#20))
     InMemoryColumnarTableScan [o_orderdate#4,l_commitdate#19,l_receiptdate#20,o_orderkey#0,o_orderpriority#5], [scalaUDF(scalaUDF(o_orderdate#4),scalaUDF(1993-07-01)),scalaUDF(scalaUDF(o_orderdate#4),scalaUDF(scalaUDF(1993-07-01),scalaUDF(P3M))),(l_commitdate#19 < l_receiptdate#20)], (InMemoryRelation [o_orderkey#0,o_custkey#1,o_orderstatus#2,o_totalprice#3,o_orderdate#4,o_orderpriority#5,o_clerk#6,o_shippriority#7,o_comment#8,l_partkey#9,l_suppkey#10,l_linenumber#11,l_quantity#12,l_extendedprice#13,l_discount#14,l_tax#15,l_returnflag#16,l_linestatus#17,l_shipdate#18,l_commitdate#19,l_receiptdate#20,l_shipinstruct#21,l_shipmode#22,l_comment#23,order_year#24,ps_partkey#25,ps_suppkey#26,ps_availqty#27,ps_supplycost#28,ps_comment#29,s_name#30,s_address#31,s_phone#32,s_acctbal#33,s_comment#34,s_nation#35,s_region#36,p_name#37,p_mfgr#38,p_brand#39,p_type#40,p_size#41,p_container#42,p_retailprice#43,p_comment#44,c_name#45,c_address#46,c_phone#47,c_acctbal#48,c_mktsegment#49,c_comment#50,c_nation#51,c_region#52], true, 10000, StorageLevel(true, true, false, true, 1), (Repartition 40, false), None)
 ```
 
#### Translation
- `l_commitdate < l_receiptdate` cannot be translated to Druid

### Q5, Local Supplier Volume Query

```scala
val orderDtPredicate1 = dateTime('o_orderdate) >= dateTime("1994-01-01")
val orderDtPredicate2 = dateTime('o_orderdate) < (dateTime("1994-01-01") + 1.year)

val q5 = sql(date"""
select s_nation,
sum(l_extendedprice * (1 - l_discount)) as revenue 
from orderLineItemPartSupplier
where s_region = 'ASIA'
and $orderDtPredicate1
and $orderDtPredicate2 
group by s_nation 
order by revenue desc
""")

q5.show()
```

#### Spark SQL Plan
```
ExternalSort [revenue#2407 DESC], true
 Aggregate false, [s_nation#35], [s_nation#35,CombineSum(PartialSum#2944) AS revenue#2407]
  Aggregate true, [s_nation#35], [s_nation#35,SUM((l_extendedprice#13 * (1.0 - l_discount#14))) AS PartialSum#2944]
   Project [s_nation#35,l_extendedprice#13,l_discount#14]
    Filter (((s_region#36 = ASIA) && scalaUDF(scalaUDF(o_orderdate#4),scalaUDF(1994-01-01))) && scalaUDF(scalaUDF(o_orderdate#4),scalaUDF(scalaUDF(1994-01-01),scalaUDF(P1Y))))
     InMemoryColumnarTableScan [s_nation#35,o_orderdate#4,l_discount#14,s_region#36,l_extendedprice#13], [(s_region#36 = ASIA),scalaUDF(scalaUDF(o_orderdate#4),scalaUDF(1994-01-01)),scalaUDF(scalaUDF(o_orderdate#4),scalaUDF(scalaUDF(1994-01-01),scalaUDF(P1Y)))], (In...
```

#### Translation
- region filter to FilterSpec
- order filter to JSScript.
  - `'dateIsAfterOrEqual('dateTime('o_orderdate),'dateTime(1994-01-01)))` the pattern is date comparison
     on a date field with a date constant.
- revenue -> JSAggregate

### Q6, Forecasting Revenue Change Query

```scala
val shipDtPredicate1 = dateTime('l_shipdate) >= dateTime("1994-01-01")
val shipDtPredicate2 = dateTime('l_shipdate) < (dateTime("1994-01-01") + 1.year)

val q6 = sql(date"""
select
sum(l_extendedprice*l_discount) as revenue
from orderLineItemPartSupplier
where $shipDtPredicate1 and $shipDtPredicate2
and l_discount between 0.06 - 0.01 and 0.06 + 0.01 and l_quantity < 24
""")

q6.show(10)
```

#### Spark SQL Plan
```
Aggregate false, [CombineSum(PartialSum#3484) AS revenue#2947]
 Aggregate true, [SUM((l_extendedprice#13 * l_discount#14)) AS PartialSum#3484]
  Project [l_extendedprice#13,l_discount#14]
   Filter ((((scalaUDF(scalaUDF(l_shipdate#18),scalaUDF(1994-01-01)) && scalaUDF(scalaUDF(l_shipdate#18),scalaUDF(scalaUDF(1994-01-01),scalaUDF(P1Y)))) && (l_discount#14 >= 0.049999999999999996)) && (l_discount#14 <= 0.06999999999999999)) && (l_quantity#12 < 24.0))
    InMemoryColumnarTableScan [l_extendedprice#13,l_discount#14,l_shipdate#18,l_quantity#12], [scalaUDF(scalaUDF(l_shipdate#18),scalaUDF(1994-01-01)),scalaUDF(scalaUDF(l_shipdate#18),scalaUDF(scalaUDF(1994-01-01),scalaUDF(P1Y))),(l_discount#14 >= 0.049999999999999996),(l_discount#14 <= 0.069...
```

#### Translation
- This is a TimeSeries query, granularity is all
- shipDt predciates translated into an Interval.
- revenue -> JSAggregate


### Q7, Volume Shipping Query 

```scala
val shipDtYear = dateTime('l_shipdate) year

val q7 = sql(date"""
select s_nation, c_nation, $shipDtYear as l_year,
sum(l_extendedprice * (1 - l_discount)) as revenue
from orderLineItemPartSupplier
where ((s_nation = 'FRANCE' and c_nation = 'GERMANY') or
       (c_nation = 'FRANCE' and s_nation = 'GERMANY')
       )
group by s_nation, c_nation, $shipDtYear 
order by s_nation, c_nation, l_year
""")

q7.show(10)
```

#### Spark SQL Plan
```
ExternalSort [s_nation#35 ASC,c_nation#51 ASC,l_year#4030 ASC], true
 Aggregate false, [s_nation#35,c_nation#51,PartialGroup#4573], [s_nation#35,c_nation#51,PartialGroup#4573 AS l_year#4030,CombineSum(PartialSum#4572) AS revenue#4031]
  Aggregate true, [s_nation#35,c_nation#51,scalaUDF(scalaUDF(l_shipdate#18))], [s_nation#35,c_nation#51,scalaUDF(scalaUDF(l_shipdate#18)) AS PartialGroup#4573,SUM((l_extendedprice#13 * (1.0 - l_discount#14))) AS PartialSum#4572]
   Filter (((s_nation#35 = FRANCE) && (c_nation#51 = GERMANY)) || ((c_nation#51 = FRANCE) && (s_nation#35 = GERMANY)))
    InMemoryColumnarTableScan [s_nation#35,l_discount#14,l_shipdate#18,c_nation#51,l_extendedprice#13], [(((s_nation#35 = FRANCE) && (c_nation#51 = GERMANY)) || ((...
```

#### Transaltion
- `year('dateTime('l_shipdate))` translates to a granulairty of year
- revenue -> JSAggregate