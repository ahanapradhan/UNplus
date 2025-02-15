import copy
import time

import psycopg2
import psycopg2.extras

import executable


# import reveal_globals


# import check_nullfree

def getCoreSizes_cs(core_relations):
    # core_sizes = {}
    # for table in core_relations:
    #     cur = reveal_globals.global_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    #     cur.execute('select count(*) from ' + table + ';')
    #     res = cur.fetchone()
    #     cnt = int(str(res[0]))
    #     core_sizes[table] = cnt
    # reveal_globals.global_core_sizes= core_sizes
    reveal_globals.sf
    reveal_globals.global_core_sizes = {
        'nation': 25,
        'region': 5,
        'part': 200000 * reveal_globals.sf,
        'partsupp': 800000 * reveal_globals.sf,
        'lineitem': 6000000 * reveal_globals.sf,
        'orders': 1500000 * reveal_globals.sf,
        'supplier': 10000 * reveal_globals.sf,
        'customer': 150000 * reveal_globals.sf,

    }


def correlated_sampling_start(reveal_globals):
    itr = 3
    cur = reveal_globals.global_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    getCoreSizes_cs(reveal_globals.global_all_relations)

    for table in reveal_globals.global_core_relations:
        cur.execute("drop table if exists " + table + "_restore;")
        cur.execute("alter table " + table + " rename to " + table + "_restore;")
    cur.close()
    # restore original tables somewhere
    start_time = time.time()

    reveal_globals.seed_sample_size_per = 0.16 / reveal_globals.sf
    while itr > 0:
        done, reveal_globals = correlated_sampling(reveal_globals)
        if not done:
            print('sampling failed in iteraation', itr)
            reveal_globals.seed_sample_size_per *= 10
            itr = itr - 1
        else:
            reveal_globals.cs_time = time.time() - start_time
            print("CS PASSED")
            reveal_globals.cs_status = "PASS"
            return reveal_globals

    print("correlated samplin failed totally starting with halving based minimization")
    reveal_globals.cs_status = "FAIL"
    cur = reveal_globals.global_conn.cursor()
    for table in reveal_globals.global_core_relations:
        cur.execute("alter table " + table + "_restore rename to " + table + " ;")
        # cur.execute("create unlogged table " + tabname + " (like " + tabname + "_restore);")
        # cur.execute("Insert into " + tabname + " select * from " + tabname + "_restore;")
    cur.close()
    # cs sampling time
    reveal_globals.cs_time = time.time() - start_time
    return reveal_globals


def correlated_sampling(reveal_globals):
    print(reveal_globals.global_key_lists)
    print("Starting correlated sampling ")

    temp_global_key_list = copy.deepcopy(reveal_globals.global_key_lists)

    not_sampled_tables = copy.deepcopy(reveal_globals.global_core_relations)

    # choose base table from each key list> sample it> sample remaining tables based on base table

    cur = reveal_globals.global_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    for table in reveal_globals.global_core_relations:
        cur.execute("create table " + table + " (like " + table + "_restore);")
    cur.close()
    for key_list in temp_global_key_list:
        max_cs = 0
        # allsampled = True 
        for i in range(0, len(key_list)):
            if max_cs < reveal_globals.global_core_sizes[key_list[i][0]]:
                max_cs = reveal_globals.global_core_sizes[key_list[i][0]]
                base_t = i
                # break
                # allsampled = False

        # Sample base table      
        base_table = key_list[base_t][0]
        base_key = key_list[base_t][1]
        # n = reveal_globals.sf  * 1000
        # cnt = reveal_globals.global_core_sizes[base_table]
        # ssr = (n/cnt)*100
        # ssr = 0.16 / reveal_globals.sf
        # r= reveal_globals.global_core_sizes[base_table] * 0.01
        # limit_row= 0.5 * reveal_globals.global_core_sizes[ base_table ]
        limit_row = reveal_globals.global_core_sizes[base_table]
        cur = reveal_globals.global_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        print("insert into " + base_table + " select * from " + base_table + "_restore tablesample system(" + str(
            reveal_globals.seed_sample_size_per) + ") where (" + base_key + ") not in (select distinct(" + base_key + ") from " + base_table + ")  Limit " + str(
            limit_row) + " ;")
        cur.execute("insert into " + base_table + " select * from " + base_table + "_restore tablesample system(" + str(
            reveal_globals.seed_sample_size_per) + ") where (" + base_key + ") not in (select distinct(" + base_key + ") from " + base_table + ")  Limit " + str(
            limit_row) + " ;")
        cur.close()
        cur = reveal_globals.global_conn.cursor()
        cur.execute("select count(*) from " + base_table + ";")
        res = cur.fetchone()
        print(table, res)

        # sample remaining tables from key_list using the sampled base table
        for i in range(0, len(key_list)):
            print(i)
            tabname2 = key_list[i][0]
            key2 = key_list[i][1]

            # if tabname2 in not_sampled_tables:
            if tabname2 != base_table:
                # limit_row= 0.5 * reveal_globals.global_core_sizes[ tabname2 ]
                limit_row = reveal_globals.global_core_sizes[tabname2]
                cur = reveal_globals.global_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                print(
                    "insert into " + tabname2 + " select * from " + tabname2 + "_restore where " + key2 + " in (select distinct(" + base_key + ") from " + base_table + ") and " + key2 + " not in (select distinct(" + key2 + ") from " + tabname2 + ") Limit " + str(
                        limit_row) + " ;")
                cur.execute(
                    "insert into " + tabname2 + " select * from " + tabname2 + "_restore where " + key2 + " in (select distinct(" + base_key + ") from " + base_table + ") and " + key2 + " not in (select distinct(" + key2 + ") from " + tabname2 + " ) Limit " + str(
                        limit_row) + " ;")
                cur.close()
                cur = reveal_globals.global_conn.cursor()
                cur.execute("select count(*) from " + tabname2 + ";")
                res = cur.fetchone()
                print(table, res)

    if len(temp_global_key_list) == 0:
        for table in not_sampled_tables:
            cur = reveal_globals.global_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            print("insert into " + table + " select * from " + table + "_restore tablesample system(" + str(
                reveal_globals.seed_sample_size_per) + ");")
            cur.execute("insert into " + table + " select * from " + table + "_restore tablesample system(" + str(
                reveal_globals.seed_sample_size_per) + ");")
            cur.close()
            cur = reveal_globals.global_conn.cursor()
            cur.execute("select count(*) from " + table + ";")
            res = cur.fetchone()
            print(table, res)

    for table in reveal_globals.global_core_relations:
        cur = reveal_globals.global_conn.cursor()
        cur.execute("select count(*) from " + table + ";")
        res = cur.fetchone()
        print(table, res)

    # check for null free rows and not just nonempty results
    new_result = executable.getExecOutput()

    if len(new_result) <= 1:
        print('sampling failed in iteraation')
        cur = reveal_globals.global_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        for table in reveal_globals.global_core_relations:
            cur.execute("drop table if exists " + table + ";")
        cur.close()
        return False, reveal_globals
    else:
        # drop original tables
        # convert views to tables 
        return True, reveal_globals


from UNplus import reveal_globals
