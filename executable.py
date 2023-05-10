import os
import sys
import psycopg2
sys.path.append('../')
import reveal_globals

def getExecOutput():
    # print("inside :-- executable.getExecOutput")
    result = []
    try:
        cur = reveal_globals.global_conn.cursor()
        #cur.execute(open(executable_path, "r").read())
        #CREATE QUERY
        
    
        #query = "select * from film,inventory;"
        #query = "select s_acctbal from supplier;"
        #Q1
        #query = "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_discount) as sum_disc_price, sum(l_tax) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate <= date '1998-12-01' - interval '71 days' group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus;"
        #Q2
        #query = "select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment from part, supplier, partsupp, nation, region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 38 and p_type like '%TIN' and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'MIDDLE EAST' order by s_acctbal desc, n_name, s_name, p_partkey limit 100;"
        #Q3
        #query = "select l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority from customer, orders, lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < date '1995-03-29' and l_shipdate > date '1995-03-29' group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate limit 10;"
        # query = "select l_orderkey, sum(l_discount) as revenue, o_orderdate, o_shippriority from customer, orders, lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < '1995-03-15' and l_shipdate > '1995-03-15' group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate, l_orderkey limit 10;"
        #Q4
        #query = "Select o_orderdate, o_orderpriority, count(*) as order_count From orders Where o_orderdate >= date '1997-07-01' and o_orderdate < date '1997-07-01' + interval '3' month Group By o_orderdate, o_orderpriority Order By o_orderpriority Limit 10;"
        #Q5
        #query = "Select  n_name, sum(l_extendedprice) as revenue From  customer, orders, lineitem, supplier, nation, region Where  c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'MIDDLE EAST' and o_orderdate >= date '1994-01-01' and o_orderdate < date '1994-01-01' + interval '1' year Group By  n_name Order By  revenue desc Limit  100;"
        #Q6
        #query = "Select  l_shipmode, sum(l_extendedprice) as revenue From  lineitem Where  l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year and l_quantity < 24 Group By  l_shipmode Limit  100;"
        #Q10
        #query = "Select  c_name, sum(l_extendedprice) as revenue, c_acctbal, n_name, c_address, c_phone, c_comment From  customer, orders, lineitem, nation Where  c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate >= date '1994-01-01' and o_orderdate < date '1994-01-01' + interval '3' month and l_returnflag = 'R' and c_nationkey = n_nationkey Group By  c_name, c_acctbal, c_phone, n_name, c_address, c_comment Order By  revenue desc Limit  20;"
        #Q11(n)
        #query = "Select  ps_COMMENT, sum(ps_availqty) as value From  partsupp, supplier, nation Where  ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By  ps_COMMENT Order By  value desc Limit  100;"
        #Q16
        #query = "Select  p_brand, p_type, p_size, count(ps_suppkey) as supplier_cnt From  partsupp, part Where  p_partkey = ps_partkey and p_brand = 'Brand#45' and p_type like 'SMALL PLATED%' and p_size >= 4 Group By  p_brand, p_type, p_size Order By  supplier_cnt desc, p_brand, p_type, p_size;"
        #Q17
        # query = "Select  AVG(l_extendedprice) as avgTOTAL From  lineitem, part Where  p_partkey = l_partkey and p_brand = 'Brand#52' and p_container = 'LG CAN';"
        #Q18Select  p_brand, p_type, p_size, count(ps_suppkey) as supplier_cnt From  partsupp, part Where  p_partkey = ps_partkey and p_brand = 'Brand#45' and p_type like 'SMALL PLATED%' and p_size >= 4 Group By  p_brand, p_type, p_size Order By  supplier_cnt desc, p_brand, p_type, p_size;edprice) as AVG_revenue From  lineitem, part Where  p_partkey = l_partkey and p_brand = 'Brand#12' and p_container like 'LG%' and p_size between 1 and 15 and l_shipmode like 'AIR%' and l_shipinstruct = 'DELIVER IN PERSON' Group By  p_name Limit  100;"
        #Q21
        #query = "Select  s_name, count(*) as numwait From  supplier, lineitem l1, orders, nation Where  s_suppkey = l1.l_suppkey and o_orderkey = l1.l_orderkey and o_orderstatus = 'F' and s_nationkey = n_nationkey and n_name = 'GERMANY' Group By  s_name Order By  numwait desc, s_name Limit  100;"
        #Q23
        #query = "Select  min(ps_supplycost) From  partsupp, supplier, nation, region Where  s_suppkey = ps_suppkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'MIDDLE EAST';"
        #query = "select c_mktsegment from customer,nation where c_acctbal <3000 and c_nationkey=n_nationkey intersect select c_mktsegment from customer,nation where c_acctbal> 3000 and c_nationkey=n_nationkey;"
        #query = "select c_mktsegment from customer,nation where c_acctbal <3000 and c_nationkey=n_nationkey intersect select c_mktsegment from customer,nation where c_acctbal> 3000 and c_acctbal <7000 and c_nationkey=n_nationkey intersect  select c_mktsegment from customer,nation where c_acctbal > 7000 and c_nationkey=n_nationkey;" 
        #query = "select o_clerk,l_extendedprice from orders,lineitem where l_orderkey = o_orderkey and l_linenumber < 5 intersect select o_clerk,l_extendedprice from orders,lineitem where l_orderkey = o_orderkey and l_linenumber > 5;"

        query=reveal_globals.query1
        #print("query=",query)
        reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1
        cur.execute(query)
        res = cur.fetchall() #fetchone always return a tuple whereas fetchall return list
        #print(res)
        colnames = [desc[0] for desc in cur.description] 
        cur.close()
        result.append(tuple(colnames))
        #print(result) it will print 8 times for from clause 
            #it will append attribute name in the result
        if res is not None:
            for row in res:
                #CHECK IF THE WHOLE ROW IN NONE (SPJA Case)
                nullrow = True
                for val in row:
                    if val != None:
                        nullrow = False
                        break
                if nullrow == True:
                    continue
                temp = []
                for val in row:
                    temp.append(str(val))
                result.append(tuple(temp))
    except Exception as error:
        # reveal_globals.error='Unmasque Error: \n Executable could not be run. Error: ' +  dict(error.args[0])['M']
        print('Executable could not be run. Error: ' + str(error))
        raise error
    return result