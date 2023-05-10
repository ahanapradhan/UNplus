import os
import sys
import csv
import copy
import math

sys.path.append('../')
import reveal_globals
import psycopg2
import time
import executable
import where_clause

from collections import defaultdict
import datetime

min_int_val = -2147483648
max_int_val = 2147483647
orig_filter = []

min_date_val = datetime.date(1, 1, 1)
max_date_val = datetime.date(9999, 12, 31)


class Graph:
    def __init__(self, n):
        self.graph = defaultdict(list)
        self.N = n

    def addEdge(self, m, n):
        self.graph[m].append(n)

    def sortUtil(self, n, visited, stack):
        visited[n] = True
        for element in self.graph[n]:
            if visited[element] == False:
                self.sortUtil(element, visited, stack)
        stack.insert(0, n)

    def topologicalSort(self):
        visited = [False] * self.N
        stack = []
        for element in range(self.N):
            if visited[element] == False:
                self.sortUtil(element, visited, stack)
        print(stack)
        return stack


def append_to_list(l, val):
    if isinstance(val[3], datetime.date):
        modVal = val
    elif len(val) == 5:
        table1 = val[0]
        table2 = val[3]
        col1 = val[1]
        col2 = val[4]
        op = val[2]
        if op == '>=':  # table1.col1 >= table2.col2
            modVal = val
        elif op == "<=":
            modVal = (table2, col2, ">=", table1, col1)
        elif op == '>':  # table1.col1 > table2.col2
            modVal = val
        elif op == "<":
            modVal = (table2, col2, ">", table1, col1)
        elif op == "=":
            tables = sorted(table1 + "." + col1, table2 + "." + col2)
            table1 = tables[0].split(".")[0]
            col1 = tables[0].split(".")[1]
            table2 = tables[1].split(".")[0]
            col2 = tables[1].split(".")[1]
            modVal = (table1, col1, op, table2, col2)
    if modVal not in l:
        l.append(modVal)


def extract_aoa():
    reveal_globals.global_proj = reveal_globals.global_filter_predicates
    reveal_globals.global_AoA = 1
    for tab in reveal_globals.global_core_relations:
        cur = reveal_globals.global_conn.cursor()
        cur.execute('Drop table if exists new_' + tab + ';')
        cur.execute('Create unlogged table new_' + tab + ' (like ' + tab + '4);')
        cur.execute('Insert into new_' + tab + ' select * from ' + tab + '4;')
        cur.execute('alter table new_' + tab + ' add primary key(' + reveal_globals.global_pk_dict[tab] + ');')
        cur.close()
    reveal_globals.local_start_time = time.time()  # aman
    filter_predicates = []
    involved_columns = []

    original_filter = copy.deepcopy(reveal_globals.global_filter_predicates)
    for pred in original_filter:
        # yet to handle join case
        if pred[2] == 'range' or pred[2] == '>=':
            filter_predicates.append([pred[0], pred[1], '>=', pred[3]])
        if pred[2] == 'range' or pred[2] == '<=':
            filter_predicates.append([pred[0], pred[1], '<=', pred[4]])
        elif pred[2] == '=':
            filter_predicates.append([pred[0], pred[1], '=', pred[3]])
        involved_columns.append((pred[0], pred[1]))
    print("Filter pred: ", filter_predicates)
    print("Var Cols: ", involved_columns)

    # step1
    # for checking possibility of equality condition between two attributes
    for pred in filter_predicates:
        print("Step 1")
        if pred[2] == '=':
            handle_eq(involved_columns, pred)

    print("Step1: ", time.time() - reveal_globals.local_start_time)  # aman

    # step2
    reveal_globals.local_start_time = time.time()  # aman
    for pred in filter_predicates:  # [table, col, op, cut-off val]
        print("Step 2")
        if pred[2] == '<=':
            handle_le(involved_columns, pred)
        elif pred[2] == '>=':
            handle_ge(involved_columns, pred)

    print("Step2: ", time.time() - reveal_globals.local_start_time)  # aman

    # step3
    reveal_globals.local_start_time = time.time()  # aman
    print("Step 3")
    attr_list = []
    for pred in reveal_globals.global_filter_aoa:
        attr_list.append((pred[0], pred[1]))
        attr_list.append((pred[3], pred[4]))
    attr_list = list(set(attr_list))
    n = len(attr_list)
    toNum = {}
    toAtt = {}
    i = 0
    for att in attr_list:
        if att not in toNum.keys():
            toNum[att] = i
            toAtt[i] = att
            i += 1
    # make the directed acyclic graph
    graph = Graph(n)
    for pred in reveal_globals.global_filter_aoa:
        if pred[2] == '<=' or pred[2] == '<':
            graph.addEdge(toNum[(pred[0], pred[1])], toNum[(pred[3], pred[4])])
        else:
            graph.addEdge(toNum[(pred[3], pred[4])], toNum[(pred[0], pred[1])])
    topo_order = graph.topologicalSort()
    for tab in reveal_globals.global_core_relations:
        cur = reveal_globals.global_conn.cursor()
        cur.execute('Delete from ' + tab + ';')
        cur.execute('Insert into ' + tab + ' select * from new_' + tab + ';')
        cur.close()
    i = 0
    stack = []
    for att in topo_order:
        cur = reveal_globals.global_conn.cursor()
        cur.execute('Select ' + toAtt[att][1] + ' from ' + toAtt[att][0] + ';')
        prev = cur.fetchone()
        prev = prev[0]
        cur.close()
        if 'int' in reveal_globals.global_attrib_types_dict[(toAtt[att][0], toAtt[att][1])]:
            val = int(prev)
            cur = reveal_globals.global_conn.cursor()
            cur.execute('Update ' + toAtt[att][0] + ' set ' + toAtt[att][1] + ' = ' + str(
                min_int_val + i) + ';')  # for integer domain
            cur.close()
        if 'numeric' in reveal_globals.global_attrib_types_dict[(toAtt[att][0], toAtt[att][1])]:
            val = int(prev)
            cur = reveal_globals.global_conn.cursor()
            cur.execute('Update ' + toAtt[att][0] + ' set ' + toAtt[att][1] + ' = ' + str(
                min_int_val + i) + ';')  # for integer domain
            cur.close()
        elif 'date' in reveal_globals.global_attrib_types_dict[(toAtt[att][0], toAtt[att][1])]:
            val = reveal_globals.global_d_plus_value[toAtt[att][1]]
            cur = reveal_globals.global_conn.cursor()
            cur.execute("Update " + toAtt[att][0] + " set " + toAtt[att][1] + " = '" + str(
                min_date_val + datetime.timedelta(days=i)) + "';")  # for date domain
            cur.close()
        stack.insert(0, att)
        new_res = executable.getExecOutput()
        if len(new_res) < 2:
            if 'int' in reveal_globals.global_attrib_types_dict[(toAtt[att][0], toAtt[att][1])]:
                left = min_int_val + i
                right = val
                while int(right - left) > 0:  # left < right:
                    mid = left + int(math.floor((right - left) / 2))
                    cur = reveal_globals.global_conn.cursor()
                    cur.execute('Update ' + toAtt[att][0] + ' set ' + toAtt[att][1] + ' = ' + str(
                        mid) + ';')  # for integer domain
                    cur.close()
                    new_res = executable.getExecOutput()
                    if len(new_res) < 2:
                        left = mid + 1
                    else:
                        right = mid
                    mid = int(math.ceil((left + right) / 2))
                cur = reveal_globals.global_conn.cursor()
                cur.execute('Update ' + toAtt[att][0] + ' set ' + toAtt[att][1] + ' = ' + str(
                    right) + ';')  # for integer domain
                cur.close()
            elif 'numeric' in reveal_globals.global_attrib_types_dict[(toAtt[att][0], toAtt[att][1])]:  # sneha
                left = min_int_val + i
                right = val
                while int(right - left) > 0:  # left < right:
                    mid = left + int(math.floor((right - left) / 2))
                    cur = reveal_globals.global_conn.cursor()
                    cur.execute('Update ' + toAtt[att][0] + ' set ' + toAtt[att][1] + ' = ' + str(
                        mid) + ';')  # for integer domain
                    cur.close()
                    new_res = executable.getExecOutput()
                    if len(new_res) < 2:
                        left = mid + 1
                    else:
                        right = mid
                    mid = int(math.ceil((left + right) / 2))
                cur = reveal_globals.global_conn.cursor()
                cur.execute('Update ' + toAtt[att][0] + ' set ' + toAtt[att][1] + ' = ' + str(
                    right) + ';')  # for integer domain
                cur.close()
            elif 'date' in reveal_globals.global_attrib_types_dict[(toAtt[att][0], toAtt[att][1])]:
                left = min_date_val + datetime.timedelta(days=i)
                right = val
                while int((right - left).days) > 0:  # left < right:
                    # print(left, mid, right)
                    mid = left + datetime.timedelta(days=int(math.floor(((right - left).days) / 2)))
                    cur = reveal_globals.global_conn.cursor()
                    cur.execute("Update " + toAtt[att][0] + " set " + toAtt[att][1] + " = '" + str(
                        mid) + "';")  # for date domain
                    cur.close()
                    new_res = executable.getExecOutput()
                    if len(new_res) < 2:
                        left = mid + datetime.timedelta(days=1)
                    else:
                        right = mid
                cur = reveal_globals.global_conn.cursor()
                cur.execute(
                    "Update " + toAtt[att][0] + " set " + toAtt[att][1] + " = '" + str(right) + "';")  # for date domain
                cur.close()
            chk3 = 0
            chk4 = 0
            isAoA3 = 0
            isAoA4 = 0
            pos_ge = []
            pos_g = []
            for pred in involved_columns:
                if pred[1] == pred[1]:
                    continue
                # SQL QUERY for checking value
                cur = reveal_globals.global_conn.cursor()
                cur.execute("SELECT " + pred[1] + " FROM new_" + pred[0] + " ;")
                prev = cur.fetchone()
                prev = prev[0]
                cur.close()
                if 'int' in reveal_globals.global_attrib_types_dict[(pred[0], pred[1])]:
                    val = int(prev)  # for INT type
                    if pred[3] == val - 1:
                        chk2 = 1
                        pos_g.append(pred)
                elif 'numeric' in reveal_globals.global_attrib_types_dict[(pred[0], pred[1])]:  # sneha
                    val = int(prev)  # for INT type
                    if pred[3] == val - 1:
                        chk2 = 1
                        pos_g.append(pred)
                elif 'date' in reveal_globals.global_attrib_types_dict[(pred[0], pred[1])]:
                    val = reveal_globals.global_d_plus_value[
                        pred[1]]  # datetime.strptime(prev, '%y-%m-%d') #for DATE type
                    if pred[3] == val - datetime.timedelta(days=1):
                        chk2 = 1
                        pos_g.append(pred)
                if pred[3] == val:
                    chk3 = 1
                    pos_ge.append(pred)

                # sneha- below 3 ifs moved to right by one tab
            if chk3 == 1:
                isAoA3 = ainea(1, i, pred[0], pred[1], pos_ge, '>=')
            if chk4 == 1:
                isAoA4 = ainea(1, i, pred[0], pred[1], pos_g, '>')
            if isAoA3 == 0 and isAoA4 == 0:
                if 'int' in reveal_globals.global_attrib_types_dict[(toAtt[att][0], toAtt[att][1])]:
                    append_to_list(reveal_globals.global_filter_aoa,
                                   (toAtt[att][0], toAtt[att][1], '>=', right, max_int_val))
                elif 'date' in reveal_globals.global_attrib_types_dict[(toAtt[att][0], toAtt[att][1])]:
                    append_to_list(reveal_globals.global_filter_aoa,
                                   (toAtt[att][0], toAtt[att][1], '>=', right, max_date_val))
        # i += 1

    # reverse topo order
    i = 0
    for tab in reveal_globals.global_core_relations:
        cur = reveal_globals.global_conn.cursor()
        cur.execute('Delete from ' + tab + ';')
        cur.execute('Insert into ' + tab + ' select * from new_' + tab + ';')
        cur.close()
    for att in stack:
        cur = reveal_globals.global_conn.cursor()
        cur.execute('Select ' + toAtt[att][1] + ' from ' + toAtt[att][0] + ';')
        prev = cur.fetchone()
        prev = prev[0]
        cur.close()
        if 'int' in reveal_globals.global_attrib_types_dict[(toAtt[att][0], toAtt[att][1])]:
            val = int(prev)
            cur = reveal_globals.global_conn.cursor()
            cur.execute('Update ' + toAtt[att][0] + ' set ' + toAtt[att][1] + ' = ' + str(
                max_int_val - i) + ';')  # for integer domain
            cur.close()
        elif 'date' in reveal_globals.global_attrib_types_dict[(toAtt[att][0], toAtt[att][1])]:
            val = reveal_globals.global_d_plus_value[toAtt[att][1]]
            cur = reveal_globals.global_conn.cursor()
            cur.execute("Update " + toAtt[att][0] + " set " + toAtt[att][1] + " = '" + str(
                max_date_val - datetime.timedelta(days=i)) + "';")  # for date domain
            cur.close()
        new_res = executable.getExecOutput()
        if len(new_res) < 2:
            if 'int' in reveal_globals.global_attrib_types_dict[(toAtt[att][0], toAtt[att][1])]:
                left = val
                right = max_int_val - i
                while int((right - left)) > 0:  # left < right:
                    mid = left + int(math.ceil((right - left) / 2))
                    cur = reveal_globals.global_conn.cursor()
                    cur.execute('Update ' + toAtt[att][0] + ' set ' + toAtt[att][1] + ' = ' + str(
                        mid) + ';')  # for integer domain
                    cur.close()
                    new_res = executable.getExecOutput()
                    if len(new_res) < 2:
                        right = mid - 1
                    else:
                        left = mid
                cur = reveal_globals.global_conn.cursor()
                cur.execute(
                    'Update ' + toAtt[att][0] + ' set ' + toAtt[att][1] + ' = ' + str(left) + ';')  # for integer domain
                cur.close()
            elif 'date' in reveal_globals.global_attrib_types_dict[(toAtt[att][0], toAtt[att][1])]:
                left = val
                right = max_date_val - datetime.timedelta(days=i)
                while int((right - left).days) > 0:  # left < right:
                    mid = left + datetime.timedelta(days=int(math.ceil(((right - left).days) / 2)))
                    cur = reveal_globals.global_conn.cursor()
                    cur.execute("Update " + toAtt[att][0] + " set " + toAtt[att][1] + " = '" + str(
                        mid) + "';")  # for date domain
                    cur.close()
                    new_res = executable.getExecOutput()
                    if len(new_res) < 2:
                        right = mid - datetime.timedelta(days=1)
                    else:
                        left = mid
                cur = reveal_globals.global_conn.cursor()
                cur.execute(
                    "Update " + toAtt[att][0] + " set " + toAtt[att][1] + " = '" + str(left) + "';")  # for date domain
                cur.close()
            chk1 = 0
            chk2 = 0
            isAoA1 = 0
            isAoA2 = 0
            pos_le = []
            pos_l = []
            for pred in involved_columns:
                if pred[1] == pred[1]:
                    continue
                # SQL QUERY for checking value
                cur = reveal_globals.global_conn.cursor()
                cur.execute("SELECT " + pred[1] + " FROM new_" + pred[0] + " ;")
                prev = cur.fetchone()
                prev = prev[0]
                cur.close()
                if 'int' in reveal_globals.global_attrib_types_dict[(pred[0], pred[1])]:
                    val = int(prev)  # for INT type
                    if pred[3] == val + 1:
                        chk2 = 1
                        pos_l.append(pred)
                elif 'date' in reveal_globals.global_attrib_types_dict[(pred[0], pred[1])]:
                    val = reveal_globals.global_d_plus_value[
                        pred[1]]  # datetime.strptime(prev, '%y-%m-%d') #for DATE type
                    if pred[3] == val + datetime.timedelta(days=1):
                        chk2 = 1
                        pos_l.append(pred)
                if pred[3] == val:
                    chk1 = 1
                    pos_le.append(pred)
            if chk1 == 1:
                isAoA1 = ainea(1, i, pred[0], pred[1], pos_le, '<=')
            if chk2 == 1:
                isAoA2 = ainea(1, i, pred[0], pred[1], pos_l, '<')
            if isAoA1 == 0 and isAoA2 == 0:
                if 'int' in reveal_globals.global_attrib_types_dict[(toAtt[att][0], toAtt[att][1])]:
                    append_to_list(reveal_globals.global_filter_aoa,
                                   (toAtt[att][0], toAtt[att][1], '<=', min_int_val, left))
                elif 'date' in reveal_globals.global_attrib_types_dict[(toAtt[att][0], toAtt[att][1])]:
                    append_to_list(reveal_globals.global_filter_aoa,
                                   (toAtt[att][0], toAtt[att][1], '<=', min_date_val, left))
        # i += 1
    print("aman_aoa", reveal_globals.global_filter_aoa)
    print("aman_aeq", reveal_globals.global_filter_aeq)
    for tab in reveal_globals.global_core_relations:
        cur = reveal_globals.global_conn.cursor()
        cur.execute('Delete from ' + tab + ';')
        cur.execute('Insert into ' + tab + ' select * from new_' + tab + ';')
        cur.close()
    # reveal_globals.globalAoA = 0
    # reveal_globals.global_filter_predicates = where_clause.get_filter_predicates()
    print("Step3: ", time.time() - reveal_globals.local_start_time)  # aman

    # sneha
    for tab in reveal_globals.global_core_relations:
        cur = reveal_globals.global_conn.cursor()
        cur.execute('Drop table if exists new_' + tab + ';')
        cur.close()

    return


def handle_eq(cols, pred):
    pos_e = []
    for c in cols:
        if c[1] == pred[1]:
            continue
        # SQL QUERY for checking value
        prev = cursor_exec_query_for_aoa(c)
        if 'int' in reveal_globals.global_attrib_types_dict[(c[0], c[1])]:
            val = int(prev)  # for INT type
        elif 'date' in reveal_globals.global_attrib_types_dict[(c[0], c[1])]:
            val = reveal_globals.global_d_plus_value[c[1]]  # datetime.strptime(prev, '%y-%m-%d') #for DATE type
        else:
            val = int(prev)
        if pred[3] == val:  # sneha tab and moved to left
            pos_e.append(c)
    if pos_e:
        aeqa(pred[0], pred[1], pos_e)


def handle_ge(cols, pred):
    pos_ge = []
    pos_g = []
    try:
        for c in cols:
            if c[1] == pred[1]:
                continue
            # SQL QUERY for checking value
            prev = cursor_exec_query_for_aoa(c)
            print(reveal_globals.global_attrib_types_dict[(c[0], c[1])])
            if 'int' in reveal_globals.global_attrib_types_dict[(c[0], c[1])]:
                val = int(prev)  # for INT type
                if type(pred[3]) == type(val):
                    if pred[3] == val - 1:
                        pos_g.append(c)
                    elif pred[3] == val:
                        pos_ge.append(c)
            elif 'date' in reveal_globals.global_attrib_types_dict[(c[0], c[1])]:
                val = reveal_globals.global_d_plus_value[
                    c[1]]  # datetime.strptime(prev, '%y-%m-%d') #for DATE type
                if type(pred[3]) == type(val):
                    if pred[3] == val - datetime.timedelta(days=1):
                        pos_g.append(c)
                    elif pred[3] == val:
                        pos_ge.append(c)
            elif 'numeric' in reveal_globals.global_attrib_types_dict[(c[0], c[1])]:
                val = int(prev)  # for INT type
                if type(pred[3]) == type(val):
                    if int(pred[3]) == val - 1:
                        pos_g.append(c)
                    elif int(pred[3]) == val:
                        pos_ge.append(c)
    except:
        print("djvhb")
    if pos_ge:
        print("pos_ge", pos_ge)
        isAoA = ainea(0, 0, pred[0], pred[1], pos_ge, '>=')
    if pos_g:
        isAoA = ainea(0, 0, pred[0], pred[1], pos_g, '>')
    return val


def cursor_exec_query_for_aoa(c):
    cur = reveal_globals.global_conn.cursor()
    cur.execute("SELECT " + c[1] + " FROM new_" + c[0] + " ;")
    prev = cur.fetchone()
    prev = prev[0]
    cur.close()
    print(prev)
    return prev


def handle_le(cols, pred):
    pos_le = []
    pos_l = []
    try:
        for c in cols:
            if c[1] == pred[1]:  # the filter from which this column was derived will not furthur contribute to any
                continue  # hence, skipping remaining of the extraction
            # SQL QUERY for checking value
            print(c)
            prev = cursor_exec_query_for_aoa(c)
            print(reveal_globals.global_attrib_types_dict[(c[0], c[1])])
            if 'int' in reveal_globals.global_attrib_types_dict[(c[0], c[1])]:
                val = int(prev)  # for INT type
                if type(pred[3]) == type(val):
                    if pred[3] == val + 1:
                        pos_l.append(c)
                    elif pred[3] == val:
                        pos_le.append(c)
            elif 'numeric' in reveal_globals.global_attrib_types_dict[(c[0], c[1])]:  # sneha
                val = int(prev)  # for INT type
                if type(pred[3]) == type(val):
                    if int(pred[3]) == val + 1:
                        pos_l.append(c)
                    elif int(pred[3]) == val:
                        pos_le.append(c)
            elif 'date' in reveal_globals.global_attrib_types_dict[(c[0], c[1])]:
                val = reveal_globals.global_d_plus_value[
                    c[1]]  # datetime.strptime(prev, '%y-%m-%d') #for DATE type
                if type(pred[3]) == type(val):
                    if pred[3] == val + datetime.timedelta(days=1):
                        pos_l.append(c)
                    elif pred[3] == val:
                        pos_le.append(c)
    except:
        print("nsjsfvkj")
    # snehajsjsd
    print(val)
    print(pred[3])
    print(pos_l, pos_le)
    if pos_le:
        print("pos_le", pos_le)
        isAoA = ainea(0, 0, pred[0], pred[1], pos_le, '<=')
    if pos_l:
        isAoA = ainea(0, 0, pred[0], pred[1], pos_l, '<')


# referenced only in extract_aoa
def aeqa(tab, col, pos):  # A=A
    chk = 0
    cur = reveal_globals.global_conn.cursor()
    cur.execute("SELECT " + col + " FROM new_" + tab + " ;")
    prev = cur.fetchone()
    prev = prev[0]
    cur.close()
    if 'int' in reveal_globals.global_attrib_types_dict[(tab, col)]:
        val = int(prev)  # for INT type
    elif 'date' in reveal_globals.global_attrib_types_dict[(tab, col)]:
        val = reveal_globals.global_d_plus_value[col]  # datetime.strptime(prev, '%y-%m-%d') #for DATE type

    for c in pos:
        # SQL Query for inc c's val
        cur = reveal_globals.global_conn.cursor()
        cur.execute("delete from " + c[0] + ";")
        cur.execute("Insert into " + c[0] + " select * from new_" + c[0] + ";")
        cur.execute("delete from " + tab + ";")
        cur.execute("Insert into " + tab + " select * from new_" + tab + ";")
        cur.close()

        if 'int' in reveal_globals.global_attrib_types_dict[(tab, col)]:
            cur = reveal_globals.global_conn.cursor()
            cur.execute("update " + tab + " set " + col + " = " + str(val + 1) + " ;")
            cur.execute("update " + c[0] + " set " + c[1] + " = " + str(val + 1) + " ;")
            cur.close()
        elif 'date' in reveal_globals.global_attrib_types_dict[(tab, col)]:
            cur = reveal_globals.global_conn.cursor()
            cur.execute("update " + tab + " set " + col + " = '" + str(val + datetime.timedelta(days=1)) + "' ;")
            cur.execute("update " + c[0] + " set " + c[1] + " = '" + str(val + datetime.timedelta(days=1)) + "' ;")
            cur.close()
        new_result = executable.getExecOutput()
        # print("aeqa, new_result length: ", len(new_result), tab, col, c[0], c[1])
        # print("new_result: ", new_result)
        cur = reveal_globals.global_conn.cursor()
        cur.execute("update " + tab + " set " + col + " = " + str(val) + " ;")
        cur.execute("update " + c[0] + " set " + c[1] + " = " + str(val) + " ;")
        cur.close()
        # if len(new_result) < 2:
        #     time.sleep(100)
        #     new_result = executable.getExecOutput()
        if len(new_result) > 1:
            chk = 1
            append_to_list(reveal_globals.global_filter_aeq, (tab, col, '=', c[0], c[1]))
            break
            # return 1
    if chk == 0:
        append_to_list(reveal_globals.global_filter_aeq, (tab, col, '=', val, val))
    return  # 0


# referenced only on extract_aoa
def ainea(flag, ofst, tab, col, pos, op):  # AoA, op={<,<=,>,>=}
    # check wether A=A is present in col or pos
    mark = 0
    for c in pos:
        cur = reveal_globals.global_conn.cursor()
        cur.execute("SELECT " + c[1] + " FROM new_" + c[0] + " ;")
        prev = cur.fetchone()
        prev = prev[0]
        cur.close()
        if 'int' in reveal_globals.global_attrib_types_dict[(c[0], c[1])]:
            val = int(prev)  # for INT type
        elif 'date' in reveal_globals.global_attrib_types_dict[(c[0], c[1])]:
            val = reveal_globals.global_d_plus_value[c[1]]  # datetime.strptime(prev, '%y-%m-%d') #for DATE type
        # SQL Query for inc c's val
        cur = reveal_globals.global_conn.cursor()
        cur.execute("delete from " + c[0] + ";")
        cur.execute("Insert into " + c[0] + " select * from new_" + c[0] + ";")
        cur.close()
        if 'int' in reveal_globals.global_attrib_types_dict[(c[0], c[1])]:
            cur = reveal_globals.global_conn.cursor()
            cur.execute("update " + c[0] + " set " + c[1] + " = " + str(val + 1) + " ;")
            cur.close()
        elif 'date' in reveal_globals.global_attrib_types_dict[(c[0], c[1])]:
            cur = reveal_globals.global_conn.cursor()
            cur.execute("update " + c[0] + " set " + c[1] + " = '" + str(val + datetime.timedelta(days=1)) + "' ;")
            cur.close()
        new_result = executable.getExecOutput()
        if len(new_result) > 1:
            new_filter = where_clause.get_filter_predicates()
        else:
            cur = reveal_globals.global_conn.cursor()
            cur.execute("delete from " + c[0] + ";")
            cur.execute("Insert into " + c[0] + " select * from new_" + c[0] + ";")
            cur.close()
            if 'int' in reveal_globals.global_attrib_types_dict[(c[0], c[1])]:
                cur = reveal_globals.global_conn.cursor()
                cur.execute("update " + c[0] + " set " + c[1] + " = " + str(val - 1) + " ;")
                cur.close()
            elif 'date' in reveal_globals.global_attrib_types_dict[(c[0], c[1])]:
                cur = reveal_globals.global_conn.cursor()
                cur.execute("update " + c[0] + " set " + c[1] + " = '" + str(val - datetime.timedelta(days=1)) + "' ;")
                cur.close()
            new_filter = where_clause.get_filter_predicates()
        new = ()
        orig = ()
        for new_pred in new_filter:
            if new_pred[0] == tab and new_pred[1] and col:
                new = new_pred
                break;
        for orig_pred in orig_filter:
            if orig_pred[0] == tab and orig_pred[1] and col:
                orig = orig_pred
                break;
        if new != orig:
            mark += 1
            append_to_list(reveal_globals.global_filter_aoa, (tab, col, op, c[0], c[1]))
            if flag == 1 and (op == '<=' or op == '<'):
                if 'int' in reveal_globals.global_attrib_types_dict[(c[0], c[1])]:
                    cur = reveal_globals.global_conn.cursor()
                    cur.execute('Update ' + c[0] + ' set ' + c[1] + ' = ' + str(
                        max_int_val - ofst - mark) + ';')  # for integer domain
                    cur.close()
                elif 'date' in reveal_globals.global_attrib_types_dict[(c[0], c[1])]:
                    xyz = ofst + mark
                    cur = reveal_globals.global_conn.cursor()
                    cur.execute("Update " + c[0] + " set " + c[1] + " = '" + str(
                        max_date_val - datetime.timedelta(days=xyz)) + "';")  # for date domain
                    cur.close()
                # run executable
                new_chk_res = executable.getExecOutput()
                if len(new_chk_res) < 2:
                    cur = reveal_globals.global_conn.cursor()
                    cur.execute('Update ' + c[0] + ' set ' + c[1] + ' = ' + str(val) + ';')
                    cur.close()
                    if 'date' in reveal_globals.global_attrib_types_dict[(tab, col)]:  # for date datatypes
                        cur = reveal_globals.global_conn.cursor()
                        cur.execute(
                            "Update " + tab + " set " + col + " = '" + str(val - datetime.timedelta(days=1)) + "';")
                        cur.close()
                    else:  # for int datatype
                        cur = reveal_globals.global_conn.cursor()
                        cur.execute('Update ' + tab + ' set ' + col + ' = ' + str(val - 1) + ';')
                        cur.close()
                else:
                    if 'int' in reveal_globals.global_attrib_types_dict[(tab, col)]:
                        cur = reveal_globals.global_conn.cursor()
                        cur.execute('Update ' + tab + ' set ' + col + ' = ' + str(
                            max_int_val - ofst - mark - 1) + ';')  # for integer domain
                        cur.close()
                    elif 'date' in reveal_globals.global_attrib_types_dict[(tab, col)]:
                        xyz = ofst + mark + 1
                        cur = reveal_globals.global_conn.cursor()
                        cur.execute("Update " + tab + " set " + col + " = '" + str(
                            max_date_val - datetime.timedelta(days=xyz)) + "';")  # for date domain
                        cur.close()
                new_chk_res = executable.getExecOutput()
                if len(new_chk_res) < 2:
                    if 'int' in reveal_globals.global_attrib_types_dict[(tab, col)]:
                        cur = reveal_globals.global_conn.cursor()
                        cur.execute('Update ' + tab + ' set ' + col + ' = ' + str(val - 1) + ';')  # for integer domain
                        cur.close()
                    elif 'date' in reveal_globals.global_attrib_types_dict[(tab, col)]:
                        cur = reveal_globals.global_conn.cursor()
                        cur.execute("Update " + tab + " set " + col + " = '" + str(
                            val - datetime.timedelta(days=1)) + "';")  # for date domain
                        cur.close()
                mark += 2
            elif flag == 1 and (op == '>=' or op == '>'):
                if 'int' in reveal_globals.global_attrib_types_dict[(tab, col)]:
                    cur = reveal_globals.global_conn.cursor()
                    cur.execute('Update ' + c[0] + ' set ' + c[1] + ' = ' + str(
                        min_int_val + ofst + mark) + ';')  # for integer domain
                    cur.close()
                elif 'date' in reveal_globals.global_attrib_types_dict[(tab, col)]:
                    xyz = ofst + mark
                    cur = reveal_globals.global_conn.cursor()
                    cur.execute("Update " + c[0] + " set " + c[1] + " = '" + str(
                        min_date_val + datetime.timedelta(days=xyz)) + "';")  # for date domain
                    cur.close()
                # run executable
                new_chk_res = executable.getExecOutput()
                if len(new_chk_res) < 2:
                    cur = reveal_globals.global_conn.cursor()
                    cur.execute('Update ' + c[0] + ' set ' + c[1] + ' = ' + str(val) + ';')
                    cur.close()
                    if 'date' in reveal_globals.global_attrib_types_dict[(c[0], c[1])]:  # for date datatypes
                        cur = reveal_globals.global_conn.cursor()
                        cur.execute(
                            "Update " + tab + " set " + col + " = '" + str(val + datetime.timedelta(days=1)) + "';")
                        cur.close()
                    else:  # for int datatype
                        cur = reveal_globals.global_conn.cursor()
                        cur.execute('Update ' + tab + ' set ' + col + ' = ' + str(val + 1) + ';')
                        cur.close()
                else:
                    if 'int' in reveal_globals.global_attrib_types_dict[(tab, col)]:
                        cur = reveal_globals.global_conn.cursor()
                        cur.execute('Update ' + tab + ' set ' + col + ' = ' + str(
                            max_int_val - ofst - mark + 1) + ';')  # for integer domain
                        cur.close()
                    elif 'date' in reveal_globals.global_attrib_types_dict[(c[0], c[1])]:
                        xyz = ofst + mark - 1
                        cur = reveal_globals.global_conn.cursor()
                        cur.execute("Update " + tab + " set " + col + " = '" + str(
                            max_date_val - datetime.timedelta(days=xyz)) + "';")  # for date domain
                        cur.close()
                new_chk_res = executable.getExecOutput()
                if len(new_chk_res) < 2:
                    if 'int' in reveal_globals.global_attrib_types_dict[(tab, col)]:
                        cur = reveal_globals.global_conn.cursor()
                        cur.execute('Update ' + tab + ' set ' + col + ' = ' + str(val + 1) + ';')  # for integer domain
                        cur.close()
                    elif 'date' in reveal_globals.global_attrib_types_dict[(tab, col)]:
                        cur = reveal_globals.global_conn.cursor()
                        cur.execute("Update " + tab + " set " + col + " = '" + str(
                            val + datetime.timedelta(days=1)) + "';")  # for date domain
                        cur.close()
                mark += 2
    if mark != 0:
        return mark
    return 0
