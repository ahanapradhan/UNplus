import reveal_globals


def get_input_query():
    reveal_globals.query1 = getQuery("bothActiveDormant")
    # --------------U1
    # All U1 queries (except Q5) are extracted by aman's replacement componenets
    # the file main_aoa.py comtains those changes
    # Q5 issue because of -- c_nationkey = s_nationkey and s_nationkey = n_nationkey   -- aman's code not able to handle when 3 attributes are requires to have same values 

    # Q1
    # reveal_globals.query1 = "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_discount) as sum_disc_price, sum(l_tax) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate <= date '1998-12-01' - interval '71 days' group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus;"
    # Q2 p
    # reveal_globals.query1 = "select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment from part, supplier, partsupp, nation, region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 38 and p_type like '%TIN' and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'MIDDLE EAST' order by s_acctbal desc, n_name, s_name limit 100;"
    # Q3
    # reveal_globals.query1 = "select l_orderkey, sum(l_discount) as revenue, o_orderdate, o_shippriority from customer, orders, lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < '1995-03-15' and l_shipdate > '1995-03-15' group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate, l_orderkey limit 10;"
    # Q4
    # reveal_globals.query1 = "Select o_orderdate, o_orderpriority, count(*) as order_count From orders Where o_orderdate >= date '1997-07-01' and o_orderdate < date '1997-07-01' + interval '3' month Group By o_orderdate, o_orderpriority Order By o_orderpriority Limit 10;"
    # Q5 p
    # reveal_globals.query1 = "Select  n_name, sum(l_extendedprice) as revenue From  customer, orders, lineitem, supplier, nation, region Where  c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'MIDDLE EAST' and o_orderdate >= date '1994-01-01' and o_orderdate < date '1994-01-01' + interval '1' year Group By n_name Order By  revenue desc Limit  100;"
    # Q6
    # reveal_globals.query1 = "Select  l_shipmode, sum(l_extendedprice) as revenue From  lineitem Where  l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year and l_quantity < 24 Group By  l_shipmode Limit  100;"
    # Q10
    # reveal_globals.query1 = "Select  c_name, sum(l_extendedprice) as revenue, c_acctbal, n_name, c_address, c_phone, c_comment From  customer, orders, lineitem, nation Where  c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate >= date '1994-01-01' and o_orderdate < date '1994-01-01' + interval '3' month and l_returnflag = 'R' and c_nationkey = n_nationkey Group By  c_name, c_acctbal, c_phone, n_name, c_address, c_comment Order By  revenue desc Limit  20;"
    # Q11(n)
    # reveal_globals.query1 = "Select  ps_COMMENT, sum(ps_availqty) as value From  partsupp, supplier, nation Where  ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By  ps_COMMENT Order By  value desc Limit  100;"
    # Q16
    # reveal_globals.query1 = "Select  p_brand, p_type, p_size, count(ps_suppkey) as supplier_cnt From  partsupp, part Where  p_partkey = ps_partkey and p_type like 'SMALL PLATED%' and p_size >= 4 Group By  p_brand, p_type, p_size Order By  supplier_cnt desc, p_brand, p_type, p_size;"

    # Q17
    # reveal_globals.query1 = "Select  AVG(l_extendedprice) as avgTOTAL From  lineitem, part Where  p_partkey = l_partkey and p_brand = 'Brand#52' and p_container = 'LG CAN';"
    # Q18
    # reveal_globals.query1 ="Select  p_brand, p_type, p_size, count(ps_suppkey) as supplier_cnt From  partsupp, part Where  p_partkey = ps_partkey and p_type like 'SMALL PLATED%' and p_size >= 4 Group By  p_brand, p_type, p_size Order By  supplier_cnt desc, p_brand, p_type, p_size;"
    # Q21
    # reveal_globals.query1 = "Select  s_name, count(*) as numwait From  supplier, lineitem l1, orders, nation Where  s_suppkey = l1.l_suppkey and o_orderkey = l1.l_orderkey and o_orderstatus = 'F' and s_nationkey = n_nationkey and n_name = 'GERMANY' Group By  s_name Order By  numwait desc, s_name Limit  100;"
    # Q23
    # reveal_globals.query1 = "Select  min(ps_supplycost) From  partsupp, supplier, nation, region Where  s_suppkey = ps_suppkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'MIDDLE EAST';"
    # reveal_globals.query1 = "select c_mktsegment from customer,nation where c_acctbal <3000 and c_nationkey=n_nationkey intersect select c_mktsegment from customer,nation where c_acctbal> 3000 and c_nationkey=n_nationkey;"
    # reveal_globals.query1 = "select c_mktsegment from customer,nation where c_acctbal <3000 and c_nationkey=n_nationkey intersect select c_mktsegment from customer,nation where c_acctbal> 3000 and c_acctbal <7000 and c_nationkey=n_nationkey intersect  select c_mktsegment from customer,nation where c_acctbal > 7000 and c_nationkey=n_nationkey;" 
    # reveal_globals.query1 = "select o_clerk,l_extendedprice from orders,lineitem where l_orderkey = o_orderkey and l_linenumber < 5 intersect select o_clerk,l_extendedprice from orders,lineitem where l_orderkey = o_orderkey and l_linenumber > 5;"

    # Queries [Level - 3]
    # kkk report Q1, simplified
    # results done
    # reveal_globals.query1 = " Select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice) as sum_disc_price, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order From lineitem Where l_shipdate <= date '1998-12-01' - interval '71 days' Group By l_returnflag, l_linestatus Order by l_returnflag, l_linestatus;"
    # Q2
    # results done
    # reveal_globals.query1 = " Select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment From part, supplier, partsupp, nation, region Where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 38 and p_type like '%TIN' and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'MIDDLE EAST' Order by s_acctbal desc, n_name, s_name Limit 100;"
    # # Q5
    # reveal_globals.query1 = " Select n_name, sum(l_extendedprice) as revenue From customer, orders, lineitem, supplier, nation, region Where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'MIDDLE EAST' and o_orderdate >= date '1994-01-01' and o_orderdate < date '1994-01-01' + interval '1' year Group By n_name Order by revenue desc Limit 100; "
    # Q4
    # reveal_globals.query1 = " Select o_orderdate, o_orderpriority, count(*) as order_count From orders Where o_orderdate >= date '1997-07-01' and o_orderdate < date '1997-07-01' + interval '3' month Group By o_orderkey, o_orderdate, o_orderpriority Order by o_orderpriority Limit 10; "
    # Q10
    # reveal_globals.query1 = " Select c_name,sum(l_extendedprice) as revenue, c_acctbal, n_name, c_address, c_phone, c_comment From customer, orders, lineitem, nation Where c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate >= date '1994-01-01' and o_orderdate < date '1994-01-01' + interval '3' month and l_returnflag = 'R' and c_nationkey = n_nationkey Group By c_name, c_acctbal, c_phone, n_name, c_address, c_comment Order by revenue desc Limit 20; "
    # Q16
    # group detected as only : Group By p_type, p_size
    # reveal_globals.query1 = " Select p_brand, p_type, p_size, count(ps_suppkey) as supplier_cnt From partsupp, part Where p_partkey = ps_partkey and p_brand = 'Brand#45' and p_type Like 'SMALL PLATED%' and p_size >= 4 Group By p_brand, p_type, p_size Order by supplier_cnt desc, p_brand, p_type, p_size; "
    # run all tese queries and document extracted queries and their times

    # reveal_globals.query1="select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment from part, supplier, partsupp, nation, region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 38 and p_type like '%TIN' and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'MIDDLE EAST' order by s_acctbal desc, n_name, s_name limit 100;"
    # reveal_globals.query1= "  select * from lineitem where l_partkey =67310 and l_orderkey =  5649094;"

    ################queries to test NEP extractor
    # reveal_globals.query1 = " select * from nation where n_nationkey<>20;" #extracted successfully
    # reveal_globals.query1 = " select * from region where r_regionkey<>4;" #extracted successfully
    # reveal_globals.query1 = " select * from nation,region where r_regionkey<>4;" #ext unsuccessful
    # reveal_globals.query1 = " select * from nation,region where n_name not in ('CANADA' ,'KENYA','RUSSIA','INDIA', 'GERMANY','IRAN','IRAQ','BRAZIL','CHINA','EGYPT','FRANCE') and r_name NOT IN ('ASIA', 'AFRICA');" #extracted 
    # reveal_globals.query1 = " select * from customer where c_mktsegment<>'HOUSEHOLD';"
    # reveal_globals.query1 = " select * from lineitem where l_returnflag<>'N';"
    # reveal_globals.query1 = " select * from nation,region where n_name <> 'CANADA' and r_name <>'ASIA';" #extracted 

    # reveal_globals.query1 = "select l_discount, l_quantity from lineitem where l_quantity <> 17;"
    # reveal_globals.query1 = "select o_orderdate, o_comment from lineitem, orders where o_totalprice <> 173665.47 and l_orderkey = o_orderkey and o_orderdate <> '1995-07-29';"
    # Q1
    # reveal_globals.query1 = 'select l_linenumber from lineitem where l_orderkey <= 14 and l_orderkey >= -100;'
    # reveal_globals.query1 = "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_discount) as sum_disc_price, sum(l_tax) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate <= date '1998-12-01' and l_linestatus <> 'F' Group by l_returnflag, l_linestatus Order by l_returnflag, l_linestatus;"
    # reveal_globals.query1 = "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_discount) as sum_disc_price, sum(l_tax) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate <= date '1998-12-01' Group by l_returnflag, l_linestatus Order by l_returnflag, l_linestatus;"
    # Q2
    # reveal_globals.query1 = "select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment from part, supplier, partsupp, nation, region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 38 and p_type like '%TIN' and s_nationkey = n_nationkey and n_regionkey = r_regionkey and n_name <> 'KENYA' order by s_acctbal desc, n_name, s_name, p_partkey limit 100;"
    # Q3
    # reveal_globals.query1 = "select l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority from customer, orders, lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < date '1995-03-29' and l_shipdate > date '1995-03-29' group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate limit 10;"
    # reveal_globals.query1 = "select l_orderkey, sum(l_discount) as revenue, o_orderdate, o_shippriority from customer, orders, lineitem where c_mktsegment <> 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < '1995-03-15' and l_shipdate > '1995-03-15' group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate, l_orderkey limit 10;"
    # Q4
    # reveal_globals.query1 = "select o_orderdate, o_orderpriority from orders where o_orderdate <>'1997-07-01' and o_orderdate <> '1997-07-05' Group By o_orderdate, o_orderpriority;"
    # reveal_globals.query1 = "Select o_orderdate, o_orderpriority, count(*) as order_count From orders Where o_orderdate >= date '1997-07-01' and o_orderdate < date '1997-09-01' and o_orderdate <>'1997-07-03' and o_orderdate <> '1997-07-05' Group By o_orderdate, o_orderpriority Order By o_orderpriority;"
    # Q5
    # reveal_globals.query1 = "Select  n_name, sum(l_extendedprice) as revenue From  customer, orders, lineitem, supplier, nation, region Where  c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'MIDDLE EAST' and o_orderdate >= date '1994-01-01' and n_name not like '%IRAN%' Group By n_name Order By  revenue desc Limit  100;"
    # Q6
    # reveal_globals.query1 = "Select  l_shipmode, sum(l_extendedprice) as revenue From  lineitem Where  l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year and l_quantity < 24 and l_shipmode not like 'AIR%'  Group By  l_shipmode Limit  100;"
    # Q10
    # reveal_globals.query1 = "Select  c_name, sum(l_extendedprice) as revenue, c_acctbal, n_name, c_address, c_phone, c_comment From  customer, orders, lineitem, nation Where  c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate >= date '1994-01-01' and o_orderdate < date '1994-01-01' + interval '3' month and l_returnflag = 'R' and c_nationkey = n_nationkey Group By  c_name, c_acctbal, c_phone, n_name, c_address, c_comment Order By  revenue desc Limit  20;"
    # Q11
    # reveal_globals.query1 = "Select  ps_COMMENT, sum(ps_availqty) as value From  partsupp, supplier, nation Where  ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By  ps_COMMENT Order By  value desc Limit  100;"
    # Q16
    # reveal_globals.query1 = "Select p_type, p_size, count(ps_suppkey) as supplier_cnt From  partsupp, part Where  p_partkey = ps_partkey and p_brand = 'Brand#45' and p_size >= 4 and p_size <> 32 and p_type not like '%TIN%' Group By p_type, p_size Order By  supplier_cnt desc, p_type, p_size;"
    # Q17
    # reveal_globals.query1 = "Select  AVG(l_extendedprice) as avgTOTAL From  lineitem, part Where  p_partkey = l_partkey and p_brand = 'Brand#52' and  l_shipdate <> '1996-03-13';"
    # Q18
    # reveal_globals.query1 = "Select  c_name, o_orderdate, o_totalprice, sum(l_quantity) From  customer, orders, lineitem Where  c_phone LIKE '27-_%' and c_custkey = o_custkey and o_orderkey = l_orderkey and c_name <> 'Customer#000060217' Group By  c_name, o_orderdate, o_totalprice Order By  o_orderdate, o_totalprice desc Limit  100;"
    # Q19
    # reveal_globals.query1 = "Select  p_name, AVG(l_extendedprice) as AVG_revenue From  lineitem, part Where  p_partkey = l_partkey and p_brand = 'Brand#12' and p_container like 'LG%' and p_size between 1 and 15 and l_shipmode like 'AIR%' and l_shipinstruct = 'DELIVER IN PERSON'  Group By  p_name Limit  100;"
    # Q21
    # reveal_globals.query1 = "Select  s_name, count(*) as numwait From  supplier, lineitem l1, orders, nation Where  s_suppkey = l1.l_suppkey and o_orderkey = l1.l_orderkey and o_orderstatus = 'F' and s_nationkey = n_nationkey and n_name <> 'GERMANY' Group By  s_name Order By  numwait desc, s_name Limit  100;"
    # Q23
    # reveal_globals.query1 = "Select  min(ps_supplycost) From  partsupp, supplier, nation, region Where  s_suppkey = ps_suppkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'MIDDLE EAST';"	
    # reveal_globals.query1 = 'Select l_discount, l_linenumber from lineitem where l_linenumber <> 3 and l_linenumber <> 4 ;'
    # reveal_globals.query1 = "select count(*) from orders ;"

    ###################
    # to test OJ queries manipulate database so that it gives atleast a null value for a projected attribute
    ###############
    # OUTER- JOIN TEST QUERIES
    # reveal_globals.query1="select * from partsupp left outer join part on ps_partkey=p_partkey;"
    # reveal_globals.query1="select * from partsupp left outer join part on ps_partkey=p_partkey where p_size>20 and ps_availqty>5000;"
    # reveal_globals.query1 ="select * from partsupp left outer join part on ps_partkey=p_partkey where ps_availqty>5000 limit 10;"
    # reveal_globals.query1="select * from supplier left outer join nation on s_nationkey=n_nationkey;"
    # reveal_globals.query1="select s_name, s_nationkey, n_nationkey, n_regionkey, r_regionkey from supplier left outer join nation on s_nationkey=n_nationkey left outer join region on n_regionkey=r_regionkey where s_acctbal>3000 order by s_name limit 10;"
    # reveal_globals.query1="Select p_name , ps_availqty, s_phone from partsupp LEFT OUTER JOIN supplier ON ps_suppkey = s_suppkey LEFT OUTER JOIN part ON ps_partkey=p_partkey where ps_availqty>3000;"
    # reveal_globals.query1="Select p_name , ps_availqty, s_phone from part LEFT OUTER JOIN partsupp ON p_partkey = ps_partkey LEFT OUTER JOIN supplier ON ps_suppkey=s_suppkey where ps_availqty>3000;"
    # reveal_globals.query1="Select p_name , ps_availqty, s_phone from part LEFT OUTER JOIN partsupp ON p_partkey = ps_partkey LEFT OUTER JOIN supplier ON ps_suppkey=s_suppkey where p_retailprice>1000;"
    # reveal_globals.query1="Select p_name , ps_availqty, s_phone from partsupp LEFT OUTER JOIN part ON ps_partkey = p_partkey LEFT OUTER JOIN supplier ON ps_suppkey=s_suppkey where ps_supplycost>400;"
    # reveal_globals.query1="select s_name, s_nationkey, n_nationkey, n_regionkey, r_regionkey from supplier left outer join nation on s_nationkey=n_nationkey left outer join region on n_regionkey=r_regionkey where s_acctbal>3000 order by s_name limit 10"
    # reveal_globals.query1= " select s_name, n_name, s_nationkey, n_nationkey, n_regionkey, r_regionkey from supplier left outer join nation on s_nationkey=n_nationkey and n_name != 'PERU' left outer join region on n_regionkey=r_regionkey where s_acctbal>3000 order by s_name"
    # reveal_globals.query1="select * from part left outer join lineitem on p_partkey=l_partkey right outer join partsupp on l_partkey=ps_partkey "

    # results same
    # reveal_globals.query1="Select p_name , ps_availqty, s_phone from part FULL OUTER JOIN partsupp ON p_partkey = ps_partkey LEFT OUTER JOIN supplier ON ps_suppkey=s_suppkey"
    # reveal_globals.query1="select p_partkey, ps_partkey, l_partkey from part left outer join lineitem on p_partkey=l_partkey right outer join partsupp on l_partkey=ps_partkey"
    # results same
    # reveal_globals.query1="select * from part full outer join partsupp on p_partkey=ps_partkey"
    # results same
    # reveal_globals.query1="select * from part inner join partsupp on p_partkey=ps_partkey "
    # results same
    # reveal_globals.query1="select * from part inner join partsupp on p_partkey=ps_partkey LEFT OUTER JOIN supplier on ps_suppkey=s_suppkey"
    # results diff
    # reveal_globals.query1="select * from part inner join partsupp on p_partkey=ps_partkey LEFT OUTER JOIN supplier on ps_suppkey=s_suppkey right outer join lineitem on ps_suppkey=l_suppkey"
    # extracted correctly but issue with null values in result comparator
    # reveal_globals.query1="select * from partsupp LEFT OUTER JOIN supplier on ps_suppkey=s_suppkey left outer join lineitem on ps_suppkey=l_suppkey"
    # results same
    # reveal_globals.query1="Select  ps_suppkey, p_partkey,ps_partkey , s_suppkey From part, partsupp left outer join supplier  on ps_suppkey=s_suppkey "
    # issue in projection.py
    # reveal_globals.query1="Select s_acctbal,ps_supplycost, p_size,  ps_suppkey, p_partkey , s_suppkey From part, partsupp left outer join supplier  on ps_suppkey=s_suppkey and s_acctbal>1000 where ps_supplycost>300 and p_size>20"

    # ON & filter predicate
    # reveal_globals.query1="Select s_acctbal,ps_supplycost, p_size,  ps_suppkey, p_partkey , s_suppkey From part right outer join partsupp on p_partkey= ps_partkey and p_size>20  left outer join supplier  on ps_suppkey=s_suppkey and s_acctbal>1000 where ps_supplycost>300 ;"
    # reveal_globals.query1="Select s_acctbal,ps_supplycost, p_size,  ps_suppkey, p_partkey , s_suppkey From part, partsupp left outer join supplier  on ps_suppkey=s_suppkey and s_acctbal>1000 where ps_supplycost>300 and p_size>20"
    # reveal_globals.query1="Select s_acctbal,ps_supplycost, p_size,  ps_suppkey, p_partkey , s_suppkey From part, partsupp left outer join supplier  on ps_suppkey=s_suppkey and s_acctbal>1000 where ps_supplycost>=300 "

    # in below query projection clause not able to differentiate between ps_suppkey and l_suppkey
    # reveal_globals.query1=" Select ps_suppkey, l_suppkey, p_partkey, ps_partkey from part left outer join partsupp on p_partkey=ps_partkey left outer join lineitem on ps_suppkey=l_suppkey"
    # below modified version works fine
    # reveal_globals.query1="Select ps_suppkey, l_shipdate , p_partkey,ps_availqty  from part right outer join partsupp on p_partkey=ps_partkey inner join lineitem on ps_suppkey=l_suppkey"

    # 7 jan
    # reveal_globals.query1="select p_partkey, ps_partkey, ps_suppkey, s_suppkey from part left outer join partsupp on p_partkey = ps_partkey right outer join supplier on ps_suppkey= s_suppkey"
    # reveal_globals.query1="Select ps_suppkey, l_suppkey, p_partkey,ps_partkey from part full outer join partsupp on p_partkey=ps_partkey full outer join lineitem on ps_suppkey=l_suppkey"
    # reveal_globals.query1="Select ps_suppkey, l_suppkey, p_partkey,ps_partkey, l_quantity, ps_availqty, p_size from part LEFT outer join partsupp on p_partkey=ps_partkey and p_size>4 and ps_availqty>3350 RIGHT outer join lineitem on ps_suppkey=l_suppkey and l_quantity>10 order by ps_availqty "

    # reveal_globals.query1 = "select * from partsupp left outer join part on ps_partkey=p_partkey where ps_availqty <> 3325;"

    ##############################
    # aman's algebraic predicates
    ##############################

    # aman paper Q1
    # reveal_globals.query1 = " Select l_shipmode, count(*)  as count From orders, lineitem Where o_orderkey = l_orderkey and l_commitdate <= l_receiptdate and l_shipdate <= l_commitdate and l_receiptdate >= '1994-01-01' and l_receiptdate < '1995-01-01' and l_extendedprice <= o_totalprice  and l_extendedprice <= 70000 and o_totalprice > 60000 Group By l_shipmode Order By l_shipmode; " 
    # reveal_globals.query1 = " Select l_shipmode, count(*)  as count From orders, lineitem Where o_orderkey = l_orderkey and l_commitdate <= l_receiptdate and l_shipdate <= l_commitdate and l_receiptdate >= '1994-01-01' and l_receiptdate < '1995-01-01' and l_extendedprice <= o_totalprice  and l_extendedprice <= 100000 and o_totalprice > 60000 Group By l_shipmode Order By l_shipmode; " 

    # aman paper Q2
    # extracted, res same main_aoa_exe.py
    # reveal_globals.query1 =  " Select o_orderpriority, count(*) as ordercount From orders, lineitem Where l_orderkey = o_orderkey and o_orderdate >= '1993-07-01' and o_orderdate < '1993-10-01' and l_commitdate <= l_receiptdate Group By o_orderpriority Order By o_orderpriority; "
    # aman paper Q3
    # reveal_globals.query1 = " Select l_orderkey, l_linenumber From orders, lineitem, partsupp Where ps_partkey = l_partkey and ps_suppkey = l_suppkey and o_orderkey = l_orderkey and l_shipdate >= o_orderdate and ps_availqty <= l_linenumber Order By l_linenumber Limit 10; "
    # aman paper Q4
    # extracted correctly by main_aoa_exe.py
    # reveal_globals.query1 = " Select l_shipmode From lineitem, partsupp Where ps_partkey = l_partkey and ps_suppkey = l_suppkey and ps_availqty = l_linenumber Group By l_shipmode Order By l_shipmode Limit 5; "
    # aman paper Q5
    # reveal_globals.query1 = " Select l_orderkey, l_linenumber From orders, lineitem, partsupp Where o_orderkey = l_orderkey and ps_partkey = l_partkey and ps_suppkey = l_suppkey and ps_availqty = l_linenumber and l_shipdate >= o_orderdate and o_orderdate >= '1990-01-01' and l_commitdate <= l_receiptdate and l_shipdate <= l_commitdate and l_receiptdate > '1994-01-01' Order By l_orderkey Limit 7; "
    # aman paper Q6
    # extracted, res same main_aoa_exe.py
    # reveal_globals.query1 = " Select s_name, count(*) From supplier, lineitem, orders, nation Where s_suppkey = l_suppkey and o_orderkey = l_orderkey and o_orderstatus = 'F' and l_receiptdate >= l_commitdate and s_nationkey = n_nationkey Group By s_name Limit 100; "

    # ATTopATT
    # reveal_globals.query1 = "select l_orderkey from lineitem where l_commitdate <= l_receiptdate and l_shipdate <= l_commitdate and l_receiptdate > date '1994-01-01' and l_commitdate <= date '1994-07-22';" #Q12
    # reveal_globals.query1 = "select l_orderkey from lineitem where l_commitdate >= l_receiptdate and l_shipdate <= l_commitdate and l_receiptdate > date '1994-01-01' and l_commitdate <= date '1994-07-22' and l_linenumber<>100;" #Q12

    # reveal_globals.query1 = "select l_orderkey,l_linenumber from orders, lineitem where o_orderkey = l_orderkey and l_shipdate >= o_orderdate and o_orderdate >= '1990-01-01';"
    # reveal_globals.query1 = "select l_orderkey,l_linenumber from orders, lineitem, partsupp where ps_partkey = l_partkey and ps_suppkey = l_suppkey and o_orderkey = l_orderkey and l_shipdate > o_orderdate and ps_availqty <= l_linenumber;"
    # aoa + nep
    # reveal_globals.query1 = " select l_orderkey,l_linenumber from orders, lineitem, partsupp where ps_partkey = l_partkey and ps_suppkey = l_suppkey and o_orderkey = l_orderkey and l_shipdate >= o_orderdate and ps_availqty <= l_linenumber and l_linenumber <> 5 and l_linenumber <>4; "

    # reveal_globals.query1 = "select l_orderkey,l_linenumber from lineitem, partsupp where ps_partkey = l_partkey and ps_suppkey = l_suppkey and ps_availqty = l_linenumber;"
    # aoa +nep
    # reveal_globals.query1 = " select l_orderkey,l_linenumber from lineitem, partsupp where ps_partkey = l_partkey and ps_suppkey = l_suppkey and ps_availqty = l_linenumber and l_linenumber <>5 ; "
    # reveal_globals.query1 = "select l_orderkey,l_linenumber from orders, lineitem, partsupp where o_orderkey = l_orderkey and ps_partkey = l_partkey and ps_suppkey = l_suppkey and ps_availqty = l_linenumber and l_shipdate >= o_orderdate and o_orderdate >= '1990-01-01' and l_commitdate <= l_receiptdate and l_shipdate <= l_commitdate and l_receiptdate >= date '1994-01-01' and l_commitdate <= date '1994-07-22';"

    # extracted results same (without OR )
    # reveal_globals.query1 = "select l_orderkey,l_linenumber from orders, lineitem, partsupp where o_orderkey = l_orderkey and ps_partkey = l_partkey and ps_suppkey = l_suppkey and ps_availqty = l_linenumber and l_shipdate >= o_orderdate and o_orderdate >= '1990-01-01' and l_commitdate <= l_receiptdate and l_shipdate <= l_commitdate and l_receiptdate >= date '1994-01-01';"

    # reveal_globals.query1 = "select s_name, count(*) as numwait from supplier, lineitem l1, orders, nation where s_suppkey = l1.l_suppkey and o_orderkey = l1.l_orderkey and o_orderstatus = 'F' and l1.l_receiptdate >= l1.l_commitdate and s_nationkey = n_nationkey group by s_name order by numwait desc, s_name limit 100;" #Q21
    # NEQ
    # reveal_globals.query1 = "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_discount) as sum_disc_price, sum(l_tax) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate <= date '1998-12-01' and l_linestatus <> 'F' Group by l_returnflag, l_linestatus Order by l_returnflag, l_linestatus;"
    # reveal_globals.query1 ="select * from lineitem where l_orderkey >= -100 and l_orderkey <= 100;"
    # reveal_globals.query1 = "select age from x where marks <= 90 intersect select y_age from y where y_marks> 30;"
    # Join with AoA
    # reveal_globals.query1 = "select count(*) from customer, orders where c_custkey = o_custkey;" # tried dec18 where clause didnot get extracted
    # reveal_globals.query1 = "select * from customer, orders, lineitem where c_custkey = o_custkey and o_orderkey = l_orderkey limit 10;"
    # reveal_globals.query1 = "select * from customer, orders, lineitem, nation where c_custkey = o_custkey and o_orderkey = l_orderkey and n_nationkey = c_nationkey limit 10;"
    # reveal_globals.query1 = "select * from customer, orders, lineitem, nation, part where c_custkey = o_custkey and o_orderkey = l_orderkey and n_nationkey = c_nationkey and p_partkey = l_partkey limit 5;"

    # sneha
    # reveal_globals.query1="select * from orders, lineitem where o_orderkey=l_orderkey and l_extendedprice<o_totalprice;"#14 dec
    # reveal_globals.query1="select * from orders, lineitem where o_orderkey=l_orderkey and l_commitdate<=l_receiptdate;"#14 dec
    # reveal_globals.query1="select * from orders, lineitem where o_orderkey=l_orderkey and l_commitdate>l_receiptdate;"#not extracted

    # reveal_globals.query1="select count(*) as sneha from nation, region;"#13 dec
    # reveal_globals.query1="select * from customer left outer join orders on c_custkey = o_custkey;"
    # reveal_globals.query1="select * from orders where o_custkey>o_totalprice;"
    # reveal_globals.query1="select o_orderkey, l_orderkey, l_extendedprice, o_totalprice from orders, lineitem where o_orderkey = l_orderkey and l_extendedprice < o_totalprice;" #14 dec
    # reveal_globals.query1="select p_partkey, p_size from part where p_size<p_partkey;" #not extracted

    # -----------------disjunction queries
    # reveal_globals.query1 = "select * from nation,supplier,partsupp where ps_suppkey=s_suppkey and s_nationkey=n_nationkey and (n_name='ARGENTINA' or n_regionkey=3) and (s_acctbal > 2000 or ps_supplycost < 500);"

    # reveal_globals.query1 = "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_discount) as sum_disc_price, sum(l_tax) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate IN (date '1998-12-01', date '1998-11-11', date '1992-01-06' )  group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus;"
    # reveal_globals.query1 = "select l_orderkey, sum(l_discount) as revenue, o_orderdate, o_shippriority from customer, orders, lineitem where ( c_mktsegment = 'FURNITURE' or c_mktsegment = 'AUTOMOBILE' ) and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < '1995-03-15' and l_shipdate > '1995-03-15' group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate, l_orderkey limit 10;"
    # reveal_globals.query1 = "Select  l_shipmode, sum(l_extendedprice) as revenue From  lineitem Where l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year and ( l_quantity = 42 or l_quantity = 50 or l_quantity = 24 ) Group By  l_shipmode Limit  100;" #**not aoa success
    # reveal_globals.query1 = "Select  AVG(l_extendedprice) as avgTOTAL From  lineitem, part Where  p_partkey = l_partkey and ( p_brand = 'Brand#52' or p_brand = 'Brand#12') and ( p_container = 'LG CAN' or p_container = 'LG CASE');"
    # reveal_globals.query1 = "select c_mktsegment from customer where c_nationkey IN (5, 10) group by c_mktsegment;"

    # ---
    # reveal_globals.query1 = "select n_name, SUM(s_acctbal) from supplier, partsupp, nation where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and (n_name = 'ARGENTINA' or n_regionkey =3 ) and (s_acctbal > 2000 or ps_supplycost < 500) group by n_name;"    
    # reveal_globals.query1 = " select n_name, SUM(s_acctbal) from supplier, partsupp, nation where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and (n_name = 'ARGENTINA' or n_regionkey =3 ) and (s_acctbal > 2000 or ps_supplycost < 500) and n_name <>'FRANCE' group by n_name; "
    # reveal_globals.query1 = " select n_name, SUM(s_acctbal) from supplier, partsupp, nation where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and (n_name = 'ARGENTINA' or n_regionkey =3 ) and (s_acctbal > 2000 or ps_supplycost < 500) and n_name <>'FRANCE' and n_regionkey <> 1 group by n_name; "

    # Exemplar Queries
    # reveal_globals.query1 = " SELECT l_shipmode, o_shippriority ,count(o_orderpriority) as low_line_count FROM lineitem LEFT OUTER JOIN orders ON ( l_orderkey = o_orderkey AND o_totalprice < 50000 ) WHERE l_shipmode IN ('MAIL', 'SHIP', 'TRUCK', 'AIR', 'FOB', 'RAIL') AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate AND ( l_receiptdate >= '1994-01-01' OR l_quantity < 30 ) AND l_receiptdate < '1995-01-01' AND l_returnflag NOT IN ('R', 'N') GROUP BY l_shipmode, o_shippriority Order By l_shipmode LIMIT  10; "
    # reveal_globals.query1 = " SELECT  o_shippriority ,count(o_orderpriority) as low_line_count FROM lineitem , orders WHERE l_orderkey = o_orderkey AND o_totalprice < 50000 and  l_shipmode IN ('MAIL', 'SHIP', 'TRUCK', 'AIR', 'FOB', 'RAIL') AND l_commitdate <= l_receiptdate AND l_shipdate <= l_commitdate AND ( l_receiptdate >= '1994-01-01' OR l_quantity < 30 ) AND l_receiptdate < '1995-01-01' AND l_returnflag NOT IN ('R', 'N') GROUP BY  o_shippriority LIMIT  10; "
    # reveal_globals.query1 = " SELECT o_shippriority ,count(o_orderpriority) FROM lineitem , orders WHERE l_orderkey = o_orderkey AND o_totalprice < 50000 and  l_shipmode IN ('MAIL', 'SHIP', 'TRUCK', 'AIR', 'FOB', 'RAIL') AND l_commitdate <= l_receiptdate AND l_shipdate <= l_commitdate AND ( l_quantity < 30  or l_quantity = 10) AND l_receiptdate <= '1995-01-01' GROUP BY  o_shippriority LIMIT  10; "
    # reveal_globals.query1 = " SELECT  o_shippriority ,count(o_orderpriority) FROM lineitem , orders WHERE l_orderkey = o_orderkey AND o_totalprice < 50000 and  l_shipmode='MAIL' AND l_commitdate <= l_receiptdate AND l_shipdate <= l_commitdate AND ( l_quantity < 30 ) AND l_receiptdate <= '1995-01-01' GROUP BY  o_shippriority LIMIT  10; "
    # reveal_globals.query1 = " SELECT o_shippriority ,count(o_orderpriority) FROM lineitem , orders WHERE l_orderkey = o_orderkey and  l_shipmode IN ('MAIL', 'SHIP', 'TRUCK', 'AIR', 'FOB', 'RAIL') AND l_commitdate <= l_receiptdate AND l_shipdate <= l_commitdate AND ( l_quantity < 30 ) AND l_receiptdate <= '1995-01-01' GROUP BY  o_shippriority LIMIT  10; "
    # reveal_globals.query1 = " SELECT o_shippriority ,count(o_orderpriority) FROM lineitem , orders WHERE l_orderkey = o_orderkey and  l_shipmode IN ('MAIL', 'SHIP', 'TRUCK', 'AIR', 'FOB', 'RAIL') AND l_commitdate <= l_receiptdate AND l_shipdate <= l_commitdate AND ( l_quantity < 30  or l_quantity = 10) AND l_receiptdate <= '1995-01-01' and l_returnflag NOT IN ('R', 'N') GROUP BY  o_shippriority LIMIT  10; "

    # trial 

    # reveal_globals.query1 = "select l_orderkey,l_linenumber from orders, lineitem, partsupp where o_orderkey = l_orderkey and ps_partkey = l_partkey and ps_suppkey = l_suppkey and ps_availqty = l_linenumber and l_shipdate >= o_orderdate and o_orderdate >= '1990-01-01' and l_commitdate <= l_receiptdate and l_shipdate <= l_commitdate and l_receiptdate >= date '1994-01-01' and l_shipmode IN ('MAIL', 'SHIP', 'TRUCK', 'AIR', 'FOB', 'RAIL');"

    ########## adding OR operator to aman's base queries
    ######################
    # aman paper Q1
    # reveal_globals.query1 = " Select l_shipmode, count(*)  as count From orders, lineitem Where o_orderkey = l_orderkey and l_commitdate <= l_receiptdate and l_shipdate <= l_commitdate and l_receiptdate >= '1994-01-01' and l_receiptdate < '1995-01-01' and l_extendedprice <= o_totalprice  and l_extendedprice <= 70000 and o_totalprice > 60000 Group By l_shipmode Order By l_shipmode; " 
    # reveal_globals.query1 = " Select l_shipmode, count(*)  as count From orders, lineitem Where o_orderkey = l_orderkey and l_commitdate <= l_receiptdate and l_shipdate <= l_commitdate and l_receiptdate >= '1994-01-01' and l_receiptdate < '1995-01-01' and l_extendedprice <= o_totalprice  and l_extendedprice <= 100000 and o_totalprice > 60000 Group By l_shipmode Order By l_shipmode; " 

    # aman paper Q2
    # extracted, res same main_aoa_exe.py
    # use for oj
    # reveal_globals.query1 =  " Select o_orderpriority, count(*) as ordercount From orders, lineitem Where l_orderkey = o_orderkey and o_orderdate >= '1993-07-01' and (o_orderdate < '1993-10-01' or o_totalprice >8000) and l_commitdate <= l_receiptdate and l_shipmode NOT IN ('AIR','MAIL') Group By o_orderpriority Order By o_orderpriority; "
    # aman paper Q3
    # reveal_globals.query1 = " Select l_orderkey, l_linenumber From orders, lineitem, partsupp Where ps_partkey = l_partkey and ps_suppkey = l_suppkey and o_orderkey = l_orderkey and l_shipdate >= o_orderdate and ps_availqty <= l_linenumber Limit 10; "
    # aman paper Q4
    # extracted correctly by main_aoa_exe.py
    # reveal_globals.query1 = " Select l_shipmode From lineitem, partsupp Where ps_partkey = l_partkey and ps_suppkey = l_suppkey and ps_availqty = l_linenumber Group By l_shipmode Order By l_shipmode Limit 5; "
    # use for oj
    # reveal_globals.query1 = "  Select l_shipmode, count(l_returnflag) From lineitem, partsupp Where ps_partkey = l_partkey and ps_suppkey = l_suppkey and (l_returnflag = 'R' or l_quantity > 20) and ps_availqty = l_linenumber and l_shipmode NOT IN ('AIR', 'MAIL') Group By l_shipmode Order By l_shipmode Limit 5; "
    # reveal_globals.query1 = " Select l_shipmode, count(l_returnflag) From lineitem, partsupp Where ps_partkey = l_partkey and ps_suppkey = l_suppkey and (l_returnflag = 'R' or ps_availqty > 2) and ps_availqty = l_linenumber and l_shipmode NOT IN ('AIR', 'REG AIR') Group By l_shipmode Order By l_shipmode Limit 5; "
    # aman paper Q5
    # reveal_globals.query1 = " Select l_orderkey, l_linenumber From orders, lineitem, partsupp Where o_orderkey = l_orderkey and ps_partkey = l_partkey and ps_suppkey = l_suppkey and ps_availqty = l_linenumber and l_shipdate >= o_orderdate and o_orderdate >= '1990-01-01' and l_commitdate <= l_receiptdate and l_shipdate <= l_commitdate and l_receiptdate > '1994-01-01' and (l_quantity >20 or l_returnflag = 'R'); "
    # aman paper Q6
    # extracted, res same main_aoa_exe.py
    # reveal_globals.query1 = " Select s_name, count(*) From supplier, lineitem, orders, nation Where s_suppkey = l_suppkey and o_orderkey = l_orderkey and o_orderstatus = 'F' and l_receiptdate >= l_commitdate and s_nationkey = n_nationkey Group By s_name Limit 100; "
    # use for oj
    # reveal_globals.query1 = " Select s_name, count(*) From supplier, lineitem, orders, nation Where s_suppkey = l_suppkey and o_orderkey = l_orderkey and ( o_orderstatus = 'F' or o_orderstatus = 'P') and l_receiptdate >= l_commitdate and s_nationkey = n_nationkey and l_returnflag <> 'N' Group By s_name Limit 100; "

    # reveal_globals.query1 = " Select s_name, count(*) From supplier, lineitem, orders, nation Where s_suppkey = l_suppkey and o_orderkey = l_orderkey and ( o_orderstatus = 'F' or o_totalprice>50000) and l_receiptdate >= l_commitdate and s_nationkey = n_nationkey and s_name <> 'Supplier#000000001' Group By s_name Limit 100; "

    return


def getSelectFrom():
    return "select l_shipmode, count(*) as count from orders, lineitem "


def getWhereKeyword():
    return "where "


def getOrderkeyEquiJoin_pred():
    return "o_orderkey = l_orderkey "


def getAndKeyWord():
    return "and "


def getshipDateCommitDateReceiptDate_activePreds():
    return " l_shipdate <= l_commitdate " \
           "and l_commitdate <= l_receiptdate " \
           "and l_receiptdate between '1994-01-01' and '1995-01-01' "


def getextendedPrice_activeDormatPreds():
    return "l_extendedprice <= o_totalprice " \
           "and l_extendedprice <= 7000 and o_totalprice >= 6000 "


def getGroupBy():
    return "group by l_shipmode;"


def getQuery(no):
    query = getSelectFrom() + getWhereKeyword()
    if no == "full":
        query += getOrderkeyEquiJoin_pred()
        query += getAndKeyWord()
        query += getextendedPrice_activeDormatPreds()
        query += getAndKeyWord()
        query += getshipDateCommitDateReceiptDate_activePreds()
    if no == "equijoin":
        query += getOrderkeyEquiJoin_pred()
    if no == "onlyActive":
        query += getshipDateCommitDateReceiptDate_activePreds()
    if no == "onlyDormant":
        query += getextendedPrice_activeDormatPreds()
    if no == "bothActiveDormant":
        query += getextendedPrice_activeDormatPreds()
        query += getAndKeyWord()
        query += getshipDateCommitDateReceiptDate_activePreds()
    query += getGroupBy()
    return query
