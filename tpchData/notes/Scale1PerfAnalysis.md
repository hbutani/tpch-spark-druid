## Baseline

<pre>
(region,Num Partitions:,1)
	 time to cache : 108
(nation,Num Partitions:,1)
	 time to cache : 1
(rawCustomer,Num Partitions:,1)
	 time to cache : 1
(part,Num Partitions:,1)
	 time to cache : 1
(rawSupplier,Num Partitions:,1)
	 time to cache : 1
(rawPartsupp,Num Partitions:,4)
	 time to cache : 1
(orders,Num Partitions:,6)
	 time to cache : 1
(lineitem,Num Partitions:,23)
	 time to cache : 1150000
(customer,Num Partitions:,200)
	 time to cache : 681
150000
10000
(supplier,Num Partitions:,200)
	 time to cache : 144
10000
800000
(partsupplier,Num Partitions:,200)
	 time to cache : 236
800000
(order_lineitem,Num Partitions:,200)
	 time to cache : 266
6001215
62396
</pre>


## Set shuffle parts to num parts in LI
<pre>
(region,Num Partitions:,1)
	 time to cache : 115
(nation,Num Partitions:,1)
	 time to cache : 1
(rawCustomer,Num Partitions:,1)
	 time to cache : 1
(part,Num Partitions:,1)
	 time to cache : 1
(rawSupplier,Num Partitions:,1)
	 time to cache : 1
(rawPartsupp,Num Partitions:,4)
	 time to cache : 1
(orders,Num Partitions:,6)
	 time to cache : 1
(lineitem,Num Partitions:,23)
	 time to cache : 1150000
(customer,Num Partitions:,23)
	 time to cache : 622
150000
10000
(supplier,Num Partitions:,23)
	 time to cache : 233
10000
800000
(partsupplier,Num Partitions:,23)
	 time to cache : 236
800000
(order_lineitem,Num Partitions:,23)
	 time to cache : 197
6001215
51637
</pre>

## master = loca[2]

brought time down by half

## master = local[*]
further reduced by 30%

* memory -Xmx2g vs -Xmx8g
not a factor

## no cache + kryo
not a factor

* autoBroadcastJoinThreshold = 100MB
probably not a factor for o join li

## externalSort
probably not a factor for o join li

## sortMergeJoin, externalSort
30% slower than shuffledHashJoin

## all Cache
About 80% time overhead to cache

<pre>
(region,Num Partitions:,2)
	 time to cache : 1967
(nation,Num Partitions:,2)
	 time to cache : 383
(rawCustomer,Num Partitions:,2)
	 time to cache : 2512
(part,Num Partitions:,2)
	 time to cache : 1846
(rawSupplier,Num Partitions:,2)
	 time to cache : 254
(rawPartsupp,Num Partitions:,4)
	 time to cache : 3459
(orders,Num Partitions:,6)
	 time to cache : 6028
(lineitem,Num Partitions:,23)
	 time to cache : 28456150000
(customer,Num Partitions:,2)
	 time to cache : 1687
150000
10000
(supplier,Num Partitions:,2)
	 time to cache : 465
10000
800000
(partsupplier,Num Partitions:,4)
	 time to cache : 7566
800000
(order_lineitem,Num Partitions:,23)
	 time to cache : 46249

6001215
47043
</pre>