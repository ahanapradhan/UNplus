import datetime
import math
import numbers

from UNplus import reveal_globals, executable
from UNplus.dbcon import execute_sql, execute_sql_fetchone
from UNplus import constants


def bin_search_update1(att, i, toAtt, val):
    att_info = (toAtt[att][0], toAtt[att][1])
    if 'int' in reveal_globals.global_attrib_types_dict[att_info]:
        left = get_val_plus_delta('int', constants.min_int_val, i)
    elif 'numeric' in reveal_globals.global_attrib_types_dict[att_info]:
        left = get_val_plus_delta('numeric', constants.min_int_val, i)
    elif 'date' in reveal_globals.global_attrib_types_dict[att_info]:
        left = get_val_plus_delta('date', constants.min_date_val, i)
    right = val
    execute_sql_fn = lambda mid: execute_sql([f"Update {att_info[0]} set {att_info[1]} = '{mid}';"])
    binary_search(left, right, execute_sql_fn)
    return left, right


def binary_search(left, right, execute_sql_fn):
    if isinstance(left, numbers.Number):
        binary_search_for_number(left, right, execute_sql_fn)
    elif isinstance(left, datetime.date):
        binary_search_for_date(left, right, execute_sql_fn)


def binary_search_for_number(left, right, execute_sql_fn):
    while int(right - left) > 0:
        mid = left + int(math.floor((right - left) / 2))
        execute_sql_fn(mid)
        new_res = executable.getExecOutput()
        if len(new_res) < 2:
            left = mid + 1
        else:
            right = mid
    execute_sql_fn(right)


def binary_search_for_date(left, right, execute_sql_fn):
    while int((right - left).days) > 0:
        mid = left + datetime.timedelta(days=int(math.floor((right - left).days / 2)))
        execute_sql_fn(mid)
        new_res = executable.getExecOutput()
        if len(new_res) < 2:
            left = mid + datetime.timedelta(days=1)
        else:
            right = mid
    execute_sql_fn(right)


def get_val_plus_delta(datatype, min_val, delta):
    if datatype in ('int', 'numeric'):
        plusdelta = min_val + delta
    elif datatype == 'date':
        plusdelta = min_val + datetime.timedelta(days=delta)
    return plusdelta


def get_max_val(tab, col, dict):
    if 'int' in dict[(tab, col)]:
        return constants.max_int_val
    elif 'date' in dict[(tab, col)]:
        return constants.max_date_val


def get_min_val(tab, col, dict):
    if 'int' in dict[(tab, col)]:
        return constants.min_int_val
    elif 'date' in dict[(tab, col)]:
        return constants.min_date_val


def get_mid_val(datatype, left, right):
    if datatype == 'int':
        delta = int(math.ceil((right - left) / 2))
    elif datatype == 'date':
        delta = int(math.ceil((right - left).days / 2))
    return get_val_plus_delta(datatype, left, delta)


def is_left_less_than_right(datatype, left, right):
    yes = False
    if datatype == 'int':
        yes = int((right - left)) > 0
    elif datatype == 'date':
        yes = int((right - left).days) > 0
    return yes


def update_tab_set_col_val_plus_delta(tab, col, dict, val, delta, query_tab, query_col):
    updated_val = val
    if 'int' in dict[(tab, col)]:
        updated_val = get_val_plus_delta('int', val, delta)
    elif 'date' in dict[(tab, col)]:
        updated_val = get_val_plus_delta('date', val, delta)
    execute_sql(["update " + query_tab + " set " + query_col + " = '" + str(updated_val) + "' ;"])
    return updated_val


def update_tab_set_col_val_plus_delta_both(tab, col, dict, val, delta, query_tab, query_col):
    updated_val = update_tab_set_col_val_plus_delta(tab, col, dict, val, delta, query_tab, query_col)
    execute_sql(["update " + tab + " set " + col + " = '" + str(updated_val) + "' ;"])


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
            tables = sorted([table1 + "." + col1, table2 + "." + col2])
            table1 = tables[0].split(".")[0]
            col1 = tables[0].split(".")[1]
            table2 = tables[1].split(".")[0]
            col2 = tables[1].split(".")[1]
            modVal = (table1, col1, op, table2, col2)
    if modVal not in l:
        l.append(modVal)


def get_val_from_new_tab(col, reveal_globals, tab):
    prev = execute_sql_fetchone("SELECT " + col + " FROM new_" + tab + " ;")
    if 'int' in reveal_globals.global_attrib_types_dict[(tab, col)]:
        val = int(prev)  # for INT type
    elif 'date' in reveal_globals.global_attrib_types_dict[(tab, col)]:
        val = reveal_globals.global_d_plus_value[col]  # datetime.strptime(prev, '%y-%m-%d') #for DATE type
    else:
        val = int(prev)
    return val


def get_datatype(tab, col, reveal_globals):
    if 'int' in reveal_globals.global_attrib_types_dict[(tab, col)]:
        datatype = 'int'
    elif 'date' in reveal_globals.global_attrib_types_dict[(tab, col)]:
        datatype = 'date'
    return datatype


def bin_search_2(att, i, reveal_globals, toAtt, val):
    left = val
    datatype = get_datatype(toAtt[att][0], toAtt[att][1], reveal_globals)
    max_boundary = get_max_val(toAtt[att][0], toAtt[att][1], reveal_globals.global_attrib_types_dict)
    right = get_val_plus_delta(datatype, max_boundary, -i)

    while is_left_less_than_right(datatype, left, right):  # left < right:
        mid = get_mid_val(datatype, left, right)
        execute_sql(["update " + toAtt[att][0] + " set " + toAtt[att][1] + " = '" + str(mid) + "' ;"])
        new_res = executable.getExecOutput()
        if len(new_res) < 2:
            right = get_val_plus_delta(datatype, mid, -1)
        else:
            left = mid

    execute_sql(["update " + toAtt[att][0] + " set " + toAtt[att][1] + " = '" + str(left) + "' ;"])
    return left
