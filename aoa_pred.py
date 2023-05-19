import copy
import functools
import sys

from UNplus.algo_utils import bin_search_update1, get_val_plus_delta, append_to_list, update_tab_set_col_val_plus_delta, \
    get_max_val, get_min_val, update_tab_set_col_val_plus_delta_both, get_val_from_new_tab, bin_search_2
from UNplus.dbcon import execute_sql, execute_sql_fetchone

sys.path.append('../')
import time
import executable
import where_clause
import constants
from collections import defaultdict
import datetime

orig_filter = []


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


def extract_aoa(reveal_globals):
    reveal_globals.global_proj = reveal_globals.global_filter_predicates
    # reveal_globals.global_AoA = 1
    for tab in reveal_globals.global_core_relations:
        execute_sql(['Drop table if exists new_' + tab + ';',
                     'Create unlogged table new_' + tab + ' (like ' + tab + '4);',
                     'Insert into new_' + tab + ' select * from ' + tab + '4;',
                     'alter table new_' + tab + ' add primary key(' + reveal_globals.global_pk_dict[
                         tab] + ');'])

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

    # step1-step2
    reveal_globals.local_start_time = time.time()  # aman
    for pred in filter_predicates:  # [table, col, op, cut-off val]
        print("Step 2")
        if pred[2] == '=':
            reveal_globals = handle_eq(involved_columns, pred, reveal_globals)
        if pred[2] == '<=':
            l, le, pos_l, pos_le = handle_op(involved_columns, pred, True, reveal_globals)
            validate_ainea_predicates(0, 0, l, le, pos_l, pos_le, pred, reveal_globals)
        elif pred[2] == '>=':
            g, ge, pos_g, pos_ge = handle_op(involved_columns, pred, False, reveal_globals)
            validate_ainea_predicates(0, 0, g, ge, pos_g, pos_ge, pred, reveal_globals)
    print("Step2: ", time.time() - reveal_globals.local_start_time)  # aman

    print(reveal_globals.global_filter_aoa)
    print(reveal_globals.global_filter_aeq)
    # exit(0)

    step3(involved_columns, pred, reveal_globals)

    # sneha
    for tab in reveal_globals.global_core_relations:
        execute_sql(['Drop table if exists new_' + tab + ';'])

    return reveal_globals


def step3(involved_columns, pred, reveal_globals):
    # step3
    print("Step 3")

    reveal_globals.local_start_time = time.time()  # aman

    for tab in reveal_globals.global_core_relations:
        execute_sql(['Delete from ' + tab + ';',
                     'Insert into ' + tab + ' select * from new_' + tab + ';'])

    attr_list = {(pred[0], pred[1]) for pred in reveal_globals.global_filter_aoa} | {(pred[3], pred[4]) for pred in
                                                                                     reveal_globals.global_filter_aoa}
    toNum = {att: i for i, att in enumerate(attr_list)}
    toAtt = {i: att for att, i in toNum.items()}
    n = len(attr_list)
    pred, topo_order = make_DAG(n, pred, toNum, reveal_globals.global_filter_aoa)

    stack, reveal_globals = traverse_topo_order(involved_columns, pred, reveal_globals, toAtt, topo_order)
    # reverse topo order
    reveal_globals = traverse_reverse_topo_order(involved_columns, pred, reveal_globals, stack, toAtt)

    print("aman_aoa", reveal_globals.global_filter_aoa)
    print("aman_aeq", reveal_globals.global_filter_aeq)
    for tab in reveal_globals.global_core_relations:
        execute_sql(['Delete from ' + tab + ';',
                     'Insert into ' + tab + ' select * from new_' + tab + ';'])
    print("Step3: ", time.time() - reveal_globals.local_start_time)  # aman
    return reveal_globals


def traverse_reverse_topo_order(involved_columns, pred, reveal_globals, stack, toAtt):
    i = 0
    for tab in reveal_globals.global_core_relations:
        execute_sql(['Delete from ' + tab + ';',
                     'Insert into ' + tab + ' select * from new_' + tab + ';'])
    for att in stack:
        prev = execute_sql_fetchone('Select ' + toAtt[att][1] + ' from ' + toAtt[att][0] + ';')
        val = update_attr_for_val(att, -i, prev, toAtt, constants.max_int_val, constants.max_date_val, reveal_globals)
        new_res = executable.getExecOutput()
        if len(new_res) < 2:
            left = bin_search_2(att, i, reveal_globals, toAtt, val)

            isAoA1 = 0
            isAoA2 = 0
            l, le, pos_l, pos_le = handle_op(involved_columns, pred, True, reveal_globals)
            if pos_le:
                isAoA1, reveal_globals = ainea(1, i, pred[0], pred[1], pos_le, le, reveal_globals)
            if pos_l:
                isAoA2, reveal_globals = ainea(1, i, pred[0], pred[1], pos_l, l, reveal_globals)
            if isAoA1 == 0 and isAoA2 == 0:
                reveal_globals = append_min_val_to_global_filter_aoa(att, left, toAtt, reveal_globals)
    return reveal_globals


def traverse_topo_order(involved_columns, pred, reveal_globals, toAtt, topo_order):
    for att in topo_order:
        prev = execute_sql_fetchone('Select ' + toAtt[att][1] + ' from ' + toAtt[att][0] + ';')
        val = update_attr_for_val(att, 0, prev, toAtt, constants.min_int_val, constants.min_date_val, reveal_globals)
        new_res = executable.getExecOutput()
        if len(new_res) < 2:
            left, right = bin_search_update1(att, 0, toAtt, val)
            isAoA3, isAoA4 = 0, 0
            g, ge, pos_g, pos_ge = handle_op(involved_columns, pred, False, reveal_globals)
            if pos_ge:
                isAoA3, reveal_globals = ainea(1, 0, pred[0], pred[1], pos_ge, ge, reveal_globals)
            if pos_g:
                isAoA4, reveal_globals = ainea(1, 0, pred[0], pred[1], pos_g, g, reveal_globals)
            if isAoA3 == 0 and isAoA4 == 0:
                reveal_globals = append_max_val_to_global_filter_aoa(att, right, toAtt, reveal_globals)
    stack = functools.reduce(lambda x, y: [y] + x, topo_order, [])
    return stack, reveal_globals


def append_min_val_to_global_filter_aoa(att, left, toAtt, reveal_globals):
    if 'int' in reveal_globals.global_attrib_types_dict[(toAtt[att][0], toAtt[att][1])]:
        append_to_list(reveal_globals.global_filter_aoa,
                       (toAtt[att][0], toAtt[att][1], '<=', constants.min_int_val, left))
    elif 'date' in reveal_globals.global_attrib_types_dict[(toAtt[att][0], toAtt[att][1])]:
        append_to_list(reveal_globals.global_filter_aoa,
                       (toAtt[att][0], toAtt[att][1], '<=', constants.min_date_val, left))
    return reveal_globals


def append_max_val_to_global_filter_aoa(att, right, toAtt, reveal_globals):
    if 'int' in reveal_globals.global_attrib_types_dict[(toAtt[att][0], toAtt[att][1])]:
        append_to_list(reveal_globals.global_filter_aoa,
                       (toAtt[att][0], toAtt[att][1], '>=', right, constants.max_int_val))
    elif 'date' in reveal_globals.global_attrib_types_dict[(toAtt[att][0], toAtt[att][1])]:
        append_to_list(reveal_globals.global_filter_aoa,
                       (toAtt[att][0], toAtt[att][1], '>=', right, constants.max_date_val))
    return reveal_globals


def update_attr_for_val(att, i, prev, toAtt, number_boundary, date_boundary, reveal_globals):
    if 'int' in reveal_globals.global_attrib_types_dict[(toAtt[att][0], toAtt[att][1])]:
        val = int(prev)
        execute_sql(['Update ' + toAtt[att][0] + ' set ' + toAtt[att][1] + ' = ' + str(
            get_val_plus_delta('int', number_boundary, i)) + ';'])
    if 'numeric' in reveal_globals.global_attrib_types_dict[(toAtt[att][0], toAtt[att][1])]:
        val = int(prev)
        execute_sql(['Update ' + toAtt[att][0] + ' set ' + toAtt[att][1] + ' = ' + str(
            get_val_plus_delta('numeric', number_boundary, i)) + ';'])
    elif 'date' in reveal_globals.global_attrib_types_dict[(toAtt[att][0], toAtt[att][1])]:
        val = reveal_globals.global_d_plus_value[toAtt[att][1]]
        execute_sql(["Update " + toAtt[att][0] + " set " + toAtt[att][1] + " = '" + str(
            get_val_plus_delta('date', date_boundary, i)) + "';"])
    return val


def make_DAG(n, pred, toNum, filter_aoa):
    # make the directed acyclic graph
    graph = Graph(n)
    for pred in filter_aoa:
        if pred[2] == '<=' or pred[2] == '<':
            graph.addEdge(toNum[(pred[0], pred[1])], toNum[(pred[3], pred[4])])
        else:
            graph.addEdge(toNum[(pred[3], pred[4])], toNum[(pred[0], pred[1])])
    topo_order = graph.topologicalSort()
    return pred, topo_order


def handle_eq(cols, pred, reveal_globals):
    pos_e = [c for c in cols if c[1] != pred[1] and pred[3] == get_val_from_new_tab(c[1], reveal_globals, c[0])]
    reveal_globals = aeqa(pred[0], pred[1], pos_e, reveal_globals) if pos_e else reveal_globals
    return reveal_globals


def datatype_compare(datatype, prev, pred_val, delta, dateval, c, pos_op, pos_ope):
    if datatype in ('int', 'numeric'):
        val = int(prev)
        val_plus_delta = val + delta
    elif datatype == 'date':
        val = dateval
        val_plus_delta = val + datetime.timedelta(days=delta)

    if type(pred_val) == type(val):
        if pred_val == val_plus_delta:
            pos_op.append(c)
        elif pred_val == val:
            pos_ope.append(c)
    return pos_op, pos_ope


def populate_pos_based_on_datatype(c, attrib_types_dict, pred, prev, delta, pos_op, pos_ope, d_plus_value):
    if 'int' in attrib_types_dict[(c[0], c[1])]:
        pos_op, pos_ope = datatype_compare('int', prev, pred[3], delta, d_plus_value[c[1]], c, pos_op, pos_ope)
    elif 'date' in attrib_types_dict[(c[0], c[1])]:
        pos_op, pos_ope = datatype_compare('date', prev, pred[3], delta, d_plus_value[c[1]], c, pos_op, pos_ope)
    elif 'numeric' in attrib_types_dict[(c[0], c[1])]:
        pos_op, pos_ope = datatype_compare('numeric', prev, pred[3], delta, d_plus_value[c[1]], c, pos_op, pos_ope)
    return pos_op, pos_ope


def handle_op(cols, pred, plus, reveal_globals):
    d_plus_value = reveal_globals.global_d_plus_value
    dict = reveal_globals.global_attrib_types_dict
    pos_op, pos_ope = [], []
    op, ope = ('<', '<=') if plus else ('>', '>=')
    delta = 1 if plus else -1

    for c in cols:
        if c[1] != pred[1]:
            prev = execute_sql_fetchone("SELECT " + c[1] + " FROM new_" + c[0] + " ;")
            pos_op, pos_ope = populate_pos_based_on_datatype(c, dict,
                                                             pred, prev, delta,
                                                             pos_op, pos_ope,
                                                             d_plus_value)
    return op, ope, pos_op, pos_ope


def validate_ainea_predicates(flag, offset, op, ope, pos_op, pos_ope, pred, reveal_globals):
    if pos_op:
        ainea(flag, offset, pred[0], pred[1], pos_op, op, reveal_globals)
    if pos_ope:
        ainea(flag, offset, pred[0], pred[1], pos_ope, ope, reveal_globals)


# referenced only in extract_aoa
def aeqa(tab, col, pos, reveal_globals):  # A=A
    chk = False
    val = get_val_from_new_tab(col, reveal_globals, tab)

    for c in pos:
        # SQL Query for inc c's val
        execute_sql(["delete from " + c[0] + ";",
                     "Insert into " + c[0] + " select * from new_" + c[0] + ";",
                     "delete from " + tab + ";",
                     "Insert into " + tab + " select * from new_" + tab + ";"])

        update_tab_set_col_val_plus_delta_both(tab, col, reveal_globals.global_attrib_types_dict, val, 1, c[0], c[1])

        new_result = executable.getExecOutput()

        execute_sql(["update " + tab + " set " + col + " = " + str(val) + " ;",
                     "update " + c[0] + " set " + c[1] + " = " + str(val) + " ;"])
        if len(new_result) > 1:
            chk = True
            append_to_list(reveal_globals.global_filter_aeq, (tab, col, '=', c[0], c[1]))
            break
    if not chk:
        append_to_list(reveal_globals.global_filter_aeq, (tab, col, '=', val, val))
    return reveal_globals


# referenced only on extract_aoa
def ainea(flag, ofst, tab, col, pos, op, reveal_globals):  # AoA, op={<,<=,>,>=}
    # check wether A=A is present in col or pos
    mark = 0
    for c in pos:
        val = get_val_from_new_tab(c[1], reveal_globals, c[0])
        # SQL Query for inc c's val
        execute_sql(["delete from " + c[0] + ";",
                     "Insert into " + c[0] + " select * from new_" + c[0] + ";"])

        update_tab_set_col_val_plus_delta(c[0], c[1], reveal_globals.global_attrib_types_dict, val, 1, c[0], c[1])

        new_result = executable.getExecOutput()
        if len(new_result) > 1:
            new_filter, reveal_globals = where_clause.get_filter_predicates(reveal_globals)
        else:
            execute_sql(["delete from " + c[0] + ";",
                         "Insert into " + c[0] + " select * from new_" + c[0] + ";"])
            update_tab_set_col_val_plus_delta(c[0], c[1], reveal_globals.global_attrib_types_dict, val, -1, c[0], c[1])
            new_filter, reveal_globals = where_clause.get_filter_predicates(reveal_globals)

        new, orig = get_orig_and_new_preds1(col, new_filter, tab)

        if new != orig:
            mark += 1
            append_to_list(reveal_globals.global_filter_aoa, (tab, col, op, c[0], c[1]))
            if flag == 1 and (op == '<=' or op == '<'):
                xyz = ofst + mark
                maxval = get_max_val(c[0], c[1], reveal_globals.global_attrib_types_dict)
                update_tab_set_col_val_plus_delta(c[0], c[1], reveal_globals.global_attrib_types_dict, maxval, -xyz,
                                                  c[0], c[1])
                # run executable
                new_chk_res = executable.getExecOutput()
                if len(new_chk_res) < 2:
                    execute_sql(['Update ' + c[0] + ' set ' + c[1] + ' = ' + str(val) + ';'])
                    update_tab_set_col_val_plus_delta(tab, col, reveal_globals.global_attrib_types_dict, val, -1, tab,
                                                      col)
                else:
                    xyz = ofst + mark + 1
                    maxval = get_max_val(tab, col, reveal_globals.global_attrib_types_dict)
                    update_tab_set_col_val_plus_delta(tab, col, reveal_globals.global_attrib_types_dict, maxval, -xyz,
                                                      tab, col)

                new_chk_res = executable.getExecOutput()
                if len(new_chk_res) < 2:
                    update_tab_set_col_val_plus_delta(tab, col, reveal_globals.global_attrib_types_dict, val, -1, tab,
                                                      col)

                mark += 2
            elif flag == 1 and (op == '>=' or op == '>'):
                xyz = ofst + mark
                minval = get_min_val(c[0], c[1], reveal_globals.global_attrib_types_dict)
                update_tab_set_col_val_plus_delta(tab, col, reveal_globals.global_attrib_types_dict, minval, xyz, c[0],
                                                  c[1])
                # run executable
                new_chk_res = executable.getExecOutput()
                if len(new_chk_res) < 2:
                    execute_sql(['Update ' + c[0] + ' set ' + c[1] + ' = ' + str(val) + ';'])
                    update_tab_set_col_val_plus_delta(c[0], c[1], reveal_globals.global_attrib_types_dict, val, 1, tab,
                                                      col)
                else:
                    xyz = ofst + mark - 1
                    maxval = get_max_val(c[0], c[1], reveal_globals.global_attrib_types_dict)
                    update_tab_set_col_val_plus_delta(c[0], c[1], reveal_globals.global_attrib_types_dict, maxval, -xyz,
                                                      tab, col)
                new_chk_res = executable.getExecOutput()
                if len(new_chk_res) < 2:
                    update_tab_set_col_val_plus_delta(tab, col, reveal_globals.global_attrib_types_dict, val, 1, tab,
                                                      col)

                mark += 2
    # if mark != 0:
    return mark, reveal_globals
    # return 0, reveal_globals


def get_orig_and_new_preds1(col, new_filter, tab):
    new = next((new_pred for new_pred in new_filter if new_pred[0] == tab and new_pred[1] and col), ())
    orig = next((orig_pred for orig_pred in orig_filter if orig_pred[0] == tab and orig_pred[1] and col), ())
    return new, orig
