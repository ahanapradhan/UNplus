import copy
import math
import sys

sys.path.append('../')
import reveal_globals
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
            if not visited[element]:
                self.sortUtil(element, visited, stack)
        stack.insert(0, n)

    def topologicalSort(self):
        visited = [False] * self.N
        stack = []
        for element in range(self.N):
            if not visited[element]:
                self.sortUtil(element, visited, stack)
        print(stack)
        return stack


def execute_sql(sqls):
    cur = reveal_globals.global_conn.cursor()
    for sql in sqls:
        cur.execute(sql)
    cur.close()


def execute_sql_fetchone(sql):
    cur = reveal_globals.global_conn.cursor()
    cur.execute(sql)
    prev = cur.fetchone()
    prev = prev[0]
    cur.close()
    return prev


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
        execute_sql(['Drop table if exists new_' + tab + ';',
                     'Create unlogged table new_' + tab + ' (like ' + tab + '4);',
                     'Insert into new_' + tab + ' select * from ' + tab + '4;',
                     'alter table new_' + tab + ' add primary key(' + reveal_globals.global_pk_dict[
                         tab] + ');'])
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
            handle_op(involved_columns, pred, True)
            # handle_le(involved_columns, pred)
        elif pred[2] == '>=':
            handle_op(involved_columns, pred, False)
            # handle_ge(involved_columns, pred)
    print("Step2: ", time.time() - reveal_globals.local_start_time)  # aman

    step3(involved_columns, pred)

    # sneha
    for tab in reveal_globals.global_core_relations:
        execute_sql(['Drop table if exists new_' + tab + ';'])

    return


def step3(involved_columns, pred):
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
        execute_sql(['Delete from ' + tab + ';',
                     'Insert into ' + tab + ' select * from new_' + tab + ';'])
    i = 0
    stack = []
    for att in topo_order:
        prev = execute_sql_fetchone('Select ' + toAtt[att][1] + ' from ' + toAtt[att][0] + ';')
        if 'int' in reveal_globals.global_attrib_types_dict[(toAtt[att][0], toAtt[att][1])]:
            val = int(prev)
            execute_sql(['Update ' + toAtt[att][0] + ' set ' + toAtt[att][1] + ' = ' + str(
                min_int_val + i) + ';'])
        if 'numeric' in reveal_globals.global_attrib_types_dict[(toAtt[att][0], toAtt[att][1])]:
            val = int(prev)
            execute_sql(['Update ' + toAtt[att][0] + ' set ' + toAtt[att][1] + ' = ' + str(
                min_int_val + i) + ';'])
        elif 'date' in reveal_globals.global_attrib_types_dict[(toAtt[att][0], toAtt[att][1])]:
            val = reveal_globals.global_d_plus_value[toAtt[att][1]]
            execute_sql(["Update " + toAtt[att][0] + " set " + toAtt[att][1] + " = '" + str(
                min_date_val + datetime.timedelta(days=i)) + "';"])
        stack.insert(0, att)
        new_res = executable.getExecOutput()
        if len(new_res) < 2:
            if 'int' in reveal_globals.global_attrib_types_dict[(toAtt[att][0], toAtt[att][1])]:
                left = min_int_val + i
                right = val
                while int(right - left) > 0:  # left < right:
                    mid = left + int(math.floor((right - left) / 2))
                    execute_sql(['Update ' + toAtt[att][0] + ' set ' + toAtt[att][1] + ' = ' + str(
                        mid) + ';'])
                    new_res = executable.getExecOutput()
                    if len(new_res) < 2:
                        left = mid + 1
                    else:
                        right = mid
                    mid = int(math.ceil((left + right) / 2))
                execute_sql(['Update ' + toAtt[att][0] + ' set ' + toAtt[att][1] + ' = ' + str(
                    right) + ';'], reveal_globals.global_conn)
            elif 'numeric' in reveal_globals.global_attrib_types_dict[(toAtt[att][0], toAtt[att][1])]:  # sneha
                left = min_int_val + i
                right = val
                while int(right - left) > 0:  # left < right:
                    mid = left + int(math.floor((right - left) / 2))
                    execute_sql(['Update ' + toAtt[att][0] + ' set ' + toAtt[att][1] + ' = ' + str(
                        mid) + ';'])
                    new_res = executable.getExecOutput()
                    if len(new_res) < 2:
                        left = mid + 1
                    else:
                        right = mid
                    mid = int(math.ceil((left + right) / 2))
                execute_sql(['Update ' + toAtt[att][0] + ' set ' + toAtt[att][1] + ' = ' + str(
                    right) + ';'])
            elif 'date' in reveal_globals.global_attrib_types_dict[(toAtt[att][0], toAtt[att][1])]:
                left = min_date_val + datetime.timedelta(days=i)
                right = val
                while int((right - left).days) > 0:  # left < right:
                    mid = left + datetime.timedelta(days=int(math.floor(((right - left).days) / 2)))
                    execute_sql(["Update " + toAtt[att][0] + " set " + toAtt[att][1] + " = '" + str(
                        mid) + "';"])
                    new_res = executable.getExecOutput()
                    if len(new_res) < 2:
                        left = mid + datetime.timedelta(days=1)
                    else:
                        right = mid
                execute_sql([
                    "Update " + toAtt[att][0] + " set " + toAtt[att][1] + " = '" + str(right) + "';"])

            isAoA3 = 0
            isAoA4 = 0
            pos_ge = []
            pos_g = []
            for icol in involved_columns:
                if icol[1] == pred[1]:
                    continue
                # SQL QUERY for checking value
                prev = execute_sql_fetchone("SELECT " + icol[1] + " FROM new_" + icol[0] + " ;")
                if 'int' in reveal_globals.global_attrib_types_dict[(icol[0], icol[1])]:
                    val = int(prev)  # for INT type
                    if pred[3] == val - 1:
                        pos_g.append(icol)
                elif 'numeric' in reveal_globals.global_attrib_types_dict[(icol[0], icol[1])]:  # sneha
                    val = int(prev)  # for INT type
                    if pred[3] == val - 1:
                        pos_g.append(icol)
                elif 'date' in reveal_globals.global_attrib_types_dict[(icol[0], icol[1])]:
                    val = reveal_globals.global_d_plus_value[
                        icol[1]]  # datetime.strptime(prev, '%y-%m-%d') #for DATE type
                    if pred[3] == val - datetime.timedelta(days=1):
                        pos_g.append(icol)
                if pred[3] == val:
                    pos_ge.append(icol)

                # sneha- below 3 ifs moved to right by one tab
            if pos_ge:
                isAoA3 = ainea(1, i, pred[0], pred[1], pos_ge, '>=')
            if pos_g:
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
        execute_sql(['Delete from ' + tab + ';',
                     'Insert into ' + tab + ' select * from new_' + tab + ';'])
    for att in stack:
        prev = execute_sql_fetchone('Select ' + toAtt[att][1] + ' from ' + toAtt[att][0] + ';')
        if 'int' in reveal_globals.global_attrib_types_dict[(toAtt[att][0], toAtt[att][1])]:
            val = int(prev)
            execute_sql(['Update ' + toAtt[att][0] + ' set ' + toAtt[att][1] + ' = ' + str(
                max_int_val - i) + ';'])
        elif 'date' in reveal_globals.global_attrib_types_dict[(toAtt[att][0], toAtt[att][1])]:
            val = reveal_globals.global_d_plus_value[toAtt[att][1]]
            execute_sql(["Update " + toAtt[att][0] + " set " + toAtt[att][1] + " = '" + str(
                max_date_val - datetime.timedelta(days=i)) + "';"])
        new_res = executable.getExecOutput()
        if len(new_res) < 2:
            if 'int' in reveal_globals.global_attrib_types_dict[(toAtt[att][0], toAtt[att][1])]:
                left = val
                right = max_int_val - i
                while int((right - left)) > 0:  # left < right:
                    mid = left + int(math.ceil((right - left) / 2))
                    execute_sql(['Update ' + toAtt[att][0] + ' set ' + toAtt[att][1] + ' = ' + str(
                        mid) + ';'])
                    new_res = executable.getExecOutput()
                    if len(new_res) < 2:
                        right = mid - 1
                    else:
                        left = mid
                execute_sql([
                    'Update ' + toAtt[att][0] + ' set ' + toAtt[att][1] + ' = ' + str(left) + ';'])
            elif 'date' in reveal_globals.global_attrib_types_dict[(toAtt[att][0], toAtt[att][1])]:
                left = val
                right = max_date_val - datetime.timedelta(days=i)
                while int((right - left).days) > 0:  # left < right:
                    mid = left + datetime.timedelta(days=int(math.ceil((right - left).days / 2)))
                    execute_sql(["Update " + toAtt[att][0] + " set " + toAtt[att][1] + " = '" + str(
                        mid) + "';"])
                    new_res = executable.getExecOutput()
                    if len(new_res) < 2:
                        right = mid - datetime.timedelta(days=1)
                    else:
                        left = mid
                execute_sql([
                    "Update " + toAtt[att][0] + " set " + toAtt[att][1] + " = '" + str(left) + "';"])

            isAoA1 = 0
            isAoA2 = 0
            pos_le = []
            pos_l = []
            for icol in involved_columns:
                if icol[1] == pred[1]:
                    continue
                # SQL QUERY for checking value
                prev = execute_sql_fetchone("SELECT " + icol[1] + " FROM new_" + icol[0] + " ;")
                if 'int' in reveal_globals.global_attrib_types_dict[(icol[0], icol[1])]:
                    val = int(prev)  # for INT type
                    if pred[3] == val + 1:
                        pos_l.append(icol)
                elif 'date' in reveal_globals.global_attrib_types_dict[(icol[0], icol[1])]:
                    val = reveal_globals.global_d_plus_value[
                        icol[1]]  # datetime.strptime(prev, '%y-%m-%d') #for DATE type
                    if pred[3] == val + datetime.timedelta(days=1):
                        pos_l.append(icol)
                if pred[3] == val:
                    pos_le.append(icol)
            if pos_le:
                isAoA1 = ainea(1, i, pred[0], pred[1], pos_le, '<=')
            if pos_l:
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
        execute_sql(['Delete from ' + tab + ';',
                     'Insert into ' + tab + ' select * from new_' + tab + ';'])
    print("Step3: ", time.time() - reveal_globals.local_start_time)  # aman


def handle_eq(cols, pred):
    pos_e = []
    for c in cols:
        if c[1] == pred[1]:
            continue
        # SQL QUERY for checking value
        prev = execute_sql_fetchone("SELECT " + c[1] + " FROM new_" + c[0] + " ;")
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
            prev = execute_sql_fetchone("SELECT " + c[1] + " FROM new_" + c[0] + " ;")
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


def datatype_compare(datatype, prev, pred_val, delta, col, c, pos_op, pos_ope):
    if datatype in ('int', 'numeric'):
        val = int(prev)
        val_plus_delta = val + delta
    elif datatype == 'date':
        val = reveal_globals.global_d_plus_value[col]
        val_plus_delta = val + datetime.timedelta(days=delta)

    if type(pred_val) == type(val):
        if pred_val == val_plus_delta:
            pos_op.append(c)
        elif pred_val == val:
            pos_ope.append(c)
    return pos_op, pos_ope


def populate_pos_based_on_datatype(c, attrib_types_dict, pred, prev, delta, pos_op, pos_ope):
    if 'int' in attrib_types_dict[(c[0], c[1])]:
        pos_op, pos_ope = datatype_compare('int', prev, pred[3], delta, c[1], c, pos_op, pos_ope)
    elif 'date' in attrib_types_dict[(c[0], c[1])]:
        pos_op, pos_ope = datatype_compare('date', prev, pred[3], delta, c[1], c, pos_op, pos_ope)
    elif 'numeric' in attrib_types_dict[(c[0], c[1])]:
        pos_op, pos_ope = datatype_compare('numeric', prev, pred[3], delta, c[1], c, pos_op, pos_ope)
    return pos_op, pos_ope


def handle_op(cols, pred, plus):
    pos_op, pos_ope = [], []
    op, ope = ('<', '<=') if plus else ('>', '>=')
    delta = 1 if plus else -1

    for c in cols:
        if c[1] != pred[1]:
            prev = execute_sql_fetchone("SELECT " + c[1] + " FROM new_" + c[0] + " ;")
            pos_op, pos_ope = populate_pos_based_on_datatype(c, reveal_globals.global_attrib_types_dict,
                                                             pred, prev, delta, pos_op, pos_ope)
    if pos_op:
        isAoA = ainea(0, 0, pred[0], pred[1], pos_op, op)
    if pos_ope:
        isAoA = ainea(0, 0, pred[0], pred[1], pos_ope, ope)


def handle_le(cols, pred):
    pos_le = []
    pos_l = []
    try:
        for c in cols:
            if c[1] == pred[1]:  # the filter from which this column was derived will not furthur contribute to any
                continue  # hence, skipping remaining of the extraction
            # SQL QUERY for checking value
            prev = execute_sql_fetchone("SELECT " + c[1] + " FROM new_" + c[0] + " ;")
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
    if pos_le:
        isAoA = ainea(0, 0, pred[0], pred[1], pos_le, '<=')
    if pos_l:
        isAoA = ainea(0, 0, pred[0], pred[1], pos_l, '<')


# referenced only in extract_aoa
def aeqa(tab, col, pos):  # A=A
    chk = False
    prev = execute_sql_fetchone("SELECT " + col + " FROM new_" + tab + " ;")

    if 'int' in reveal_globals.global_attrib_types_dict[(tab, col)]:
        val = int(prev)  # for INT type
    elif 'date' in reveal_globals.global_attrib_types_dict[(tab, col)]:
        val = reveal_globals.global_d_plus_value[col]  # datetime.strptime(prev, '%y-%m-%d') #for DATE type

    for c in pos:
        # SQL Query for inc c's val
        execute_sql(["delete from " + c[0] + ";",
                     "Insert into " + c[0] + " select * from new_" + c[0] + ";",
                     "delete from " + tab + ";",
                     "Insert into " + tab + " select * from new_" + tab + ";"])

        if 'int' in reveal_globals.global_attrib_types_dict[(tab, col)]:
            execute_sql(["update " + tab + " set " + col + " = " + str(val + 1) + " ;",
                         "update " + c[0] + " set " + c[1] + " = " + str(val + 1) + " ;"])
        elif 'date' in reveal_globals.global_attrib_types_dict[(tab, col)]:
            execute_sql(
                ["update " + tab + " set " + col + " = '" + str(val + datetime.timedelta(days=1)) + "' ;",
                 "update " + c[0] + " set " + c[1] + " = '" + str(val + datetime.timedelta(days=1)) + "' ;"])
        new_result = executable.getExecOutput()

        execute_sql(["update " + tab + " set " + col + " = " + str(val) + " ;"
                        , "update " + c[0] + " set " + c[1] + " = " + str(val) + " ;"])
        if len(new_result) > 1:
            chk = True
            append_to_list(reveal_globals.global_filter_aeq, (tab, col, '=', c[0], c[1]))
            break
    if not chk:
        append_to_list(reveal_globals.global_filter_aeq, (tab, col, '=', val, val))
    return


# referenced only on extract_aoa
def ainea(flag, ofst, tab, col, pos, op):  # AoA, op={<,<=,>,>=}
    # check wether A=A is present in col or pos
    mark = 0
    for c in pos:
        prev = execute_sql_fetchone("SELECT " + c[1] + " FROM new_" + c[0] + " ;")
        if 'int' in reveal_globals.global_attrib_types_dict[(c[0], c[1])]:
            val = int(prev)  # for INT type
        elif 'date' in reveal_globals.global_attrib_types_dict[(c[0], c[1])]:
            val = reveal_globals.global_d_plus_value[c[1]]  # datetime.strptime(prev, '%y-%m-%d') #for DATE type
        # SQL Query for inc c's val
        execute_sql(["delete from " + c[0] + ";",
                     "Insert into " + c[0] + " select * from new_" + c[0] + ";"])
        if 'int' in reveal_globals.global_attrib_types_dict[(c[0], c[1])]:
            execute_sql(["update " + c[0] + " set " + c[1] + " = " + str(val + 1) + " ;"])
        elif 'date' in reveal_globals.global_attrib_types_dict[(c[0], c[1])]:
            execute_sql(
                ["update " + c[0] + " set " + c[1] + " = '" + str(val + datetime.timedelta(days=1)) + "' ;"])
        new_result = executable.getExecOutput()
        if len(new_result) > 1:
            new_filter = where_clause.get_filter_predicates()
        else:
            execute_sql(["delete from " + c[0] + ";",
                         "Insert into " + c[0] + " select * from new_" + c[0] + ";"])
            if 'int' in reveal_globals.global_attrib_types_dict[(c[0], c[1])]:
                execute_sql(["update " + c[0] + " set " + c[1] + " = " + str(val - 1) + " ;"])
            elif 'date' in reveal_globals.global_attrib_types_dict[(c[0], c[1])]:
                execute_sql(
                    ["update " + c[0] + " set " + c[1] + " = '" + str(val - datetime.timedelta(days=1)) + "' ;"])
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
                    execute_sql(['Update ' + c[0] + ' set ' + c[1] + ' = ' + str(
                        max_int_val - ofst - mark) + ';'])
                elif 'date' in reveal_globals.global_attrib_types_dict[(c[0], c[1])]:
                    xyz = ofst + mark
                    execute_sql(["Update " + c[0] + " set " + c[1] + " = '" + str(
                        max_date_val - datetime.timedelta(days=xyz)) + "';"])
                # run executable
                new_chk_res = executable.getExecOutput()
                if len(new_chk_res) < 2:
                    execute_sql(['Update ' + c[0] + ' set ' + c[1] + ' = ' + str(val) + ';'])
                    if 'date' in reveal_globals.global_attrib_types_dict[(tab, col)]:  # for date datatypes
                        execute_sql([
                            "Update " + tab + " set " + col + " = '" + str(val - datetime.timedelta(days=1)) + "';"])
                    else:  # for int datatype
                        execute_sql(['Update ' + tab + ' set ' + col + ' = ' + str(val - 1) + ';'])
                else:
                    if 'int' in reveal_globals.global_attrib_types_dict[(tab, col)]:
                        execute_sql(['Update ' + tab + ' set ' + col + ' = ' + str(
                            max_int_val - ofst - mark - 1) + ';'])
                    elif 'date' in reveal_globals.global_attrib_types_dict[(tab, col)]:
                        xyz = ofst + mark + 1
                        execute_sql(["Update " + tab + " set " + col + " = '" + str(
                            max_date_val - datetime.timedelta(days=xyz)) + "';"])
                new_chk_res = executable.getExecOutput()
                if len(new_chk_res) < 2:
                    if 'int' in reveal_globals.global_attrib_types_dict[(tab, col)]:
                        execute_sql(['Update ' + tab + ' set ' + col + ' = ' + str(val - 1) + ';'])
                    elif 'date' in reveal_globals.global_attrib_types_dict[(tab, col)]:
                        execute_sql(["Update " + tab + " set " + col + " = '" + str(
                            val - datetime.timedelta(days=1)) + "';"])
                mark += 2
            elif flag == 1 and (op == '>=' or op == '>'):
                if 'int' in reveal_globals.global_attrib_types_dict[(tab, col)]:
                    execute_sql(['Update ' + c[0] + ' set ' + c[1] + ' = ' + str(
                        min_int_val + ofst + mark) + ';'])
                elif 'date' in reveal_globals.global_attrib_types_dict[(tab, col)]:
                    xyz = ofst + mark
                    execute_sql(["Update " + c[0] + " set " + c[1] + " = '" + str(
                        min_date_val + datetime.timedelta(days=xyz)) + "';"])
                # run executable
                new_chk_res = executable.getExecOutput()
                if len(new_chk_res) < 2:
                    execute_sql(['Update ' + c[0] + ' set ' + c[1] + ' = ' + str(val) + ';'])
                    if 'date' in reveal_globals.global_attrib_types_dict[(c[0], c[1])]:  # for date datatypes
                        execute_sql([
                            "Update " + tab + " set " + col + " = '" + str(val + datetime.timedelta(days=1)) + "';"])
                    else:  # for int datatype
                        execute_sql(['Update ' + tab + ' set ' + col + ' = ' + str(val + 1) + ';'])
                else:
                    if 'int' in reveal_globals.global_attrib_types_dict[(tab, col)]:
                        execute_sql(['Update ' + tab + ' set ' + col + ' = ' + str(
                            max_int_val - ofst - mark + 1) + ';'])
                    elif 'date' in reveal_globals.global_attrib_types_dict[(c[0], c[1])]:
                        xyz = ofst + mark - 1
                        execute_sql(["Update " + tab + " set " + col + " = '" + str(
                            max_date_val - datetime.timedelta(days=xyz)) + "';"])
                new_chk_res = executable.getExecOutput()
                if len(new_chk_res) < 2:
                    if 'int' in reveal_globals.global_attrib_types_dict[(tab, col)]:
                        execute_sql(['Update ' + tab + ' set ' + col + ' = ' + str(val + 1) + ';'])
                    elif 'date' in reveal_globals.global_attrib_types_dict[(tab, col)]:
                        execute_sql(["Update " + tab + " set " + col + " = '" + str(
                            val + datetime.timedelta(days=1)) + "';"])
                mark += 2
    if mark != 0:
        return mark
    return 0
