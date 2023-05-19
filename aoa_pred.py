import copy
import functools
import sys

from UNplus.algo_utils import get_val_plus_delta, append_to_list, update_tab_set_col_val_plus_delta, \
    get_max_val, get_min_val, update_tab_set_col_val_plus_delta_both, get_val_from_new_tab, \
    get_datatype, bin_search
from UNplus.dbcon import execute_sql, execute_sql_fetchone

sys.path.append('../')
import time
import executable
import where_clause
from collections import defaultdict

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
    local_start_time = time.time()  # aman
    for pred in filter_predicates:  # [table, col, op, cut-off val]
        print("Step 1")
        if pred[2] == '=':
            handle_eq(involved_columns, pred, reveal_globals)

        print("Step 2")
        less_than = True if pred[2] == '<=' else False if pred[2] == '>=' else None
        if less_than is not None:
            op, ope, pos_op, pos_ope = handle_op(involved_columns, pred, less_than, reveal_globals)
            validate_ainea_predicates(0, 0, op, ope, pos_op, pos_ope, pred, reveal_globals)
    print("Step1-2: ", time.time() - local_start_time)  # aman

    step3(involved_columns, pred, reveal_globals)

    # sneha
    for tab in reveal_globals.global_core_relations:
        execute_sql(['Drop table if exists new_' + tab + ';'])

    return reveal_globals


def step3(involved_columns, pred, reveal_globals):
    print("Step 3")
    local_start_time = time.time()  # aman

    for tab in reveal_globals.global_core_relations:
        execute_sql(['Delete from ' + tab + ';',
                     'Insert into ' + tab + ' select * from new_' + tab + ';'])

    attr_list = {(pred[0], pred[1]) for pred in reveal_globals.global_filter_aoa} | {(pred[3], pred[4]) for pred in
                                                                                     reveal_globals.global_filter_aoa}
    toNum = {att: i for i, att in enumerate(attr_list)}
    toAtt = {i: att for att, i in toNum.items()}
    n = len(attr_list)
    pred, topo_order = make_DAG(n, pred, toNum, reveal_globals.global_filter_aoa)

    traverse_reverse_topo_order(involved_columns, pred, reveal_globals, toAtt, topo_order, False)

    stack = functools.reduce(lambda x, y: [y] + x, topo_order, [])

    for tab in reveal_globals.global_core_relations:
        execute_sql(['Delete from ' + tab + ';',
                     'Insert into ' + tab + ' select * from new_' + tab + ';'])
    # reverse topo order
    traverse_reverse_topo_order(involved_columns, pred, reveal_globals, toAtt, stack, True)

    print("aman_aoa", reveal_globals.global_filter_aoa)
    print("aman_aeq", reveal_globals.global_filter_aeq)
    for tab in reveal_globals.global_core_relations:
        execute_sql(['Delete from ' + tab + ';',
                     'Insert into ' + tab + ' select * from new_' + tab + ';'])
    print("Step3: ", time.time() - local_start_time)  # aman
    return reveal_globals


def traverse_reverse_topo_order(involved_columns, pred, reveal_globals, toAtt, order, rev_topo):
    for att in order:
        att_info = (toAtt[att][0], toAtt[att][1])
        val = update_attr_for_val(att_info[0], att_info[1], 0, rev_topo, reveal_globals)
        new_res = executable.getExecOutput()
        if len(new_res) < 2:
            left, right = bin_search(att_info, 0, reveal_globals, val, rev_topo)
            isOpeAinea, isOpAinea = 0, 0
            op, ope, pos_op, pos_ope = handle_op(involved_columns, pred, rev_topo, reveal_globals)
            if pos_ope:
                isOpeAinea = ainea(1, 0, pred[0], pred[1], pos_ope, ope, reveal_globals)
            if pos_op:
                isOpAinea = ainea(1, 0, pred[0], pred[1], pos_op, op, reveal_globals)
            if isOpeAinea == 0 and isOpAinea == 0:
                append_boundary_val_to_global_filter_aoa(att_info[0], att_info[1], left,
                                                         reveal_globals, not rev_topo, ope)


def append_boundary_val_to_global_filter_aoa(tab, col, mid, reveal_globals, boundary, op):
    val_boundary = get_max_val(tab, col, reveal_globals.global_attrib_types_dict) if boundary else \
        get_min_val(tab, col, reveal_globals.global_attrib_types_dict)
    left, right = (mid, val_boundary) if boundary else (val_boundary, mid)
    append_to_list(reveal_globals.global_filter_aoa, (tab, col, op, left, right))


def update_attr_for_val(tab, col, i, boundary, reveal_globals):
    val_boundary = get_max_val(tab, col, reveal_globals.global_attrib_types_dict) if boundary else \
        get_min_val(tab, col, reveal_globals.global_attrib_types_dict)
    val = get_val_from_new_tab(col, reveal_globals, tab)
    datatype = get_datatype(tab, col, reveal_globals)
    update_val = str(get_val_plus_delta(datatype, val_boundary, i))
    execute_sql(["Update " + tab + " set " + col + " = '" + update_val + "';"])
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
    aeqa(pred[0], pred[1], pos_e, reveal_globals) if pos_e else reveal_globals


def datatype_compare(datatype, val, pred_val, delta, c, pos_op, pos_ope):
    val_plus_delta = get_val_plus_delta(datatype, val, delta)
    if type(pred_val) == type(val):
        if pred_val == val_plus_delta:
            pos_op.append(c)
        elif pred_val == val:
            pos_ope.append(c)
    return pos_op, pos_ope


def populate_pos_based_on_datatype(c, reveal_globals, pred, prev, delta, pos_op, pos_ope, d_plus_value):
    datatype = get_datatype(c[0], c[1], reveal_globals)
    if datatype in ('int', 'numeric'):
        val = int(prev)
    elif datatype == 'date':
        val = d_plus_value[c[1]]
    pos_op, pos_ope = datatype_compare(datatype, val, pred[3], delta, c, pos_op, pos_ope)
    return pos_op, pos_ope


def handle_op(cols, pred, plus, reveal_globals):
    d_plus_value = reveal_globals.global_d_plus_value
    pos_op, pos_ope = [], []
    op, ope = ('<', '<=') if plus else ('>', '>=')
    delta = 1 if plus else -1

    for c in cols:
        if c[1] != pred[1]:
            prev = execute_sql_fetchone("SELECT " + c[1] + " FROM new_" + c[0] + " ;")
            pos_op, pos_ope = populate_pos_based_on_datatype(c, reveal_globals,
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
    return mark
    # return 0, reveal_globals


def get_orig_and_new_preds1(col, new_filter, tab):
    new = next((new_pred for new_pred in new_filter if new_pred[0] == tab and new_pred[1] and col), ())
    orig = next((orig_pred for orig_pred in orig_filter if orig_pred[0] == tab and orig_pred[1] and col), ())
    return new, orig
