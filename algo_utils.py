import datetime
import math
import numbers

from UNplus import reveal_globals, executable
from UNplus.dbcon import execute_sql

min_int_val = -2147483648
max_int_val = 2147483647
min_date_val = datetime.date(1, 1, 1)
max_date_val = datetime.date(9999, 12, 31)


def bin_search_update1(att, i, toAtt, val):
    att_info = (toAtt[att][0], toAtt[att][1])
    if 'int' in reveal_globals.global_attrib_types_dict[att_info]:
        left = get_val_plus_delta('int', min_int_val, i)
    elif 'numeric' in reveal_globals.global_attrib_types_dict[att_info]:
        left = get_val_plus_delta('numeric', min_int_val, i)
    elif 'date' in reveal_globals.global_attrib_types_dict[att_info]:
        left = get_val_plus_delta('date', min_date_val, i)
    right = val
    execute_sql_fn = lambda mid: execute_sql([f"Update {att_info[0]} set {att_info[1]} = '{mid}';"])
    binary_search(left, right, execute_sql_fn)
    return left, right


def binary_search(left, right, execute_sql_fn):
    if isinstance(left, numbers.Number):
        binary_search_for_number(left, right, execute_sql_fn)
    elif isinstance(left, datetime.date):
        binary_search_for_date(left, right, execute_sql_fn)


def bin_search_update(att, i, toAtt, val):
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
            right) + ';'])
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
    return left, right


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
