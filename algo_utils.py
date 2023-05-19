import datetime
import math

from UNplus import executable
from UNplus.dbcon import execute_sql, execute_sql_fetchone
from UNplus import constants


def bin_search(att_info, i, reveal_globals, val, is_ceil=True):
    datatype = get_datatype(att_info[0], att_info[1], reveal_globals)
    execute_sql_fn = lambda mid: execute_sql([f"Update {att_info[0]} set {att_info[1]} = '{mid}';"])

    if is_ceil:
        boundary = get_max_val(att_info[0], att_info[1], reveal_globals.global_attrib_types_dict)
        left = val
        right = get_val_plus_delta(datatype, boundary, -i)
    else:
        boundary = get_min_val(att_info[0], att_info[1], reveal_globals.global_attrib_types_dict)
        left = get_val_plus_delta(datatype, boundary, i)
        right = val

    left, right = binary_search_algo(datatype, execute_sql_fn, left, right, is_ceil)
    return left, right


def binary_search_algo(datatype, execute_sql_fn, left, right, is_ceil=True):
    while is_left_less_than_right(datatype, left, right):
        mid = get_mid_val_by_ceil(datatype, left, right) if is_ceil \
            else get_mid_val_by_floor(datatype, left, right)
        execute_sql_fn(mid)
        new_res = executable.getExecOutput()
        if len(new_res) < 2:
            if is_ceil:
                right = get_val_plus_delta(datatype, mid, -1)
            else:
                left = get_val_plus_delta(datatype, mid, 1)
        else:
            if is_ceil:
                left = mid
            else:
                right = mid

    execute_sql_fn(left) if is_ceil else execute_sql_fn(right)
    return left, right


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


def get_mid_val_by_ceil(datatype, left, right):
    if datatype == 'int':
        delta = int(math.ceil((right - left) / 2))
    elif datatype == 'date':
        delta = int(math.ceil((right - left).days / 2))
    return get_val_plus_delta(datatype, left, delta)


def get_mid_val_by_floor(datatype, left, right):
    if datatype == 'int':
        delta = int(math.floor((right - left) / 2))
    elif datatype == 'date':
        delta = int(math.floor((right - left).days / 2))
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
    elif 'numeric' in reveal_globals.global_attrib_types_dict[(tab, col)]:
        datatype = 'numeric'
    return datatype
