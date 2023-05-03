from datetime import datetime
from datetime import date

import executable
import sys
import math
import copy
import reveal_globals
import db_minimizer
import where_clause
import time

potential_in_attrib = []
filter_table_list = {}
new_filter_list = []
temp_list = []


def extract(level, att, new_list, attrib_types_dict):
    local_start = time.time()

    for tabname in reveal_globals.global_core_relations:
        cur = reveal_globals.global_conn.cursor()
        query1 = 'truncate table ' + tabname + ' ;'
        cur.execute(query1)
        condition = "true "

        condition_unifier = []

        condition = get_condition_string(att, attrib_types_dict, condition, new_list, tabname, condition_unifier)

        query2 = "Insert into " + tabname + " (select * from " + tabname + "_restore where " + condition + ");"
        temp_query_time = time.time()
        print("runnning query :" + query2)
        cur.execute(query2)
        cur.close()

        cur = reveal_globals.global_conn.cursor()
        cur.execute("Select count(*) from " + tabname + ";")
        res = cur.fetchall()
        print(res)
        cur.close()
        print("temp query time: " + str(time.time() - temp_query_time))

    res = executable.getExecOutput()

    if len(res) > 1:
        print("possible IN at:" + att[0] + ',' + att[1])

        temp_min_start = time.time()
        if db_minimizer.reduce_Database_Instance(reveal_globals.global_core_relations):
            # if (reduce_Database_Instance(reveal_globals.global_core_relations)):
            temp = " "
            print("temp min time:" + str(time.time() - temp_min_start))
            reveal_globals.global_filter_predicates = where_clause.get_filter_predicates()
            print(reveal_globals.global_filter_predicates)
            fp = reveal_globals.global_filter_predicates
            new_temp = []

            for e2 in fp:
                include_flag = True
                for ele in reveal_globals.global_filter_aeq:
                    if ele[1] == e2[1] and e2[2] == '=':
                        include_flag = False
                if include_flag:
                    new_temp.append(e2)

            reveal_globals.global_filter_predicates = new_temp

            for item in reveal_globals.global_filter_predicates:
                if item not in potential_in_attrib:
                    temp = item

            if temp != " ":
                new_list.append(temp)
                temp_list.append(temp)
                if temp[0] not in filter_table_list.keys():
                    filter_table_list[temp[0]] = [temp]
                # else:
                #     filter_table_list[temp[0]].append(temp)
                print("new condition: " + str(temp))

                cur = reveal_globals.global_conn.cursor()
                query = "select data_type from information_schema.columns where table_name ='" + temp[
                    0] + "' and column_name = '" + temp[1] + "' order by ordinal_position; "
                cur.execute(query)
                res = cur.fetchall()
                for row in res:
                    attrib_types_dict[(temp[0], temp[1])] = row[0]
                print("local disjunction time: " + str(time.time() - local_start))
                cur.close()
                extract(level + 1, att, new_list, attrib_types_dict)

        else:
            print('error with minimizer')
            cur.close()
    else:
        print('No disjunction at :  ' + att[0] + ',' + att[1])


def is_coverage_satisfied(filters, condition_unifier):
    sat = False
    for term in condition_unifier:
        if filters[1] == term[1] and filters[3] <= term[3] and filters[4] >= term[4]:
            sat = True
            break
    return sat


def get_condition_string(att, attrib_types_dict, condition, new_list, tabname, condition_unifier):
    if tabname in filter_table_list.keys():
        condition = "true "
        for filters in filter_table_list[tabname]:
            if is_coverage_satisfied(filters, condition_unifier):
                continue
            if filters == att:
                condition = get_disjunction_condition_string(attrib_types_dict, condition, filters)
                continue
            else:
                if is_number_attr(attrib_types_dict, filters):
                    condition = condition + "and (" + filters[1] + " >= " + str(filters[3]) + " and " + filters[
                        1] + " <=" + str(filters[4]) + ") "
                elif 'date' in attrib_types_dict[(filters[0], filters[1])]:
                    condition = condition + "and (" + filters[1] + " >= date '" + str(filters[3]) + "' and " + \
                                filters[1] + " <= date '" + str(filters[4]) + "') "
                else:
                    condition = condition + "and (" + filters[1] + " >= '" + str(filters[3]) + "' and " + filters[
                        1] + " <= '" + str(filters[4]) + " ') "
            #condition_unifier.append(filters)
        for filters in new_list:
            if is_coverage_satisfied(filters, condition_unifier):
                continue
            if filters[0] == tabname:
                condition = get_disjunction_condition_string(attrib_types_dict, condition, filters)
            #condition_unifier.append(filters)
    return condition


def get_disjunction_condition_string(attrib_types_dict, condition, filters):
    if 'date' in attrib_types_dict[(filters[0], filters[1])]:
        condition = append_predicate_on_date_attr(condition, filters)
    elif is_number_attr(attrib_types_dict, filters):
        condition = append_pred_on_number_attr(condition, filters)
    else:
        condition = append_pred_for_other_types(condition, filters)
    return condition


def append_pred_for_other_types(condition, filters):
    condition = condition + "and (" + filters[1] + " < '" + str(filters[3]) + "' or " + filters[
        1] + " > '" + str(filters[4]) + "') "
    return condition


def is_number_attr(attrib_types_dict, filters):
    return 'int' in attrib_types_dict[(filters[0], filters[1])] or 'numeric' in attrib_types_dict[
        (filters[0], filters[1])]


def get_attr_type_dict():
    attrib_types_dict = {}
    for entry in reveal_globals.global_attrib_types:
        attrib_types_dict[(entry[0], entry[1])] = entry[2]
    return attrib_types_dict


def append_pred_on_number_attr(condition, filters):
    if filters[3] == min_int:
        condition = condition + "and " + filters[1] + " > " + str(
            filters[4]) + " "
    elif filters[4] == max_int:
        condition = condition + "and " + filters[1] + " < " + str(filters[3]) + " "
    else:
        condition = condition + "and (" + filters[1] + " < " + str(filters[3]) + " or " + filters[1] + " > " + str(
            filters[4]) + ") "
    return condition


def append_predicate_on_date_attr(condition, filters):
    if filters[3] == date(1, 1, 1):
        condition = condition + "and " + filters[
            1] + " > date\'" + str(filters[4]) + "\' "
    else:
        condition = condition + "and (" + filters[1] + " < date \'" + str(filters[3]) + "\' or " + filters[
            1] + " > date\'" + str(filters[4]) + "\') "
    return condition


def in_extract(level=1):
    global_start = time.time()
    attrib_types_dict = get_attr_type_dict()
    print(reveal_globals.global_filter_predicates)
    # load all data for level1
    count_in = 0
    cur = reveal_globals.global_conn.cursor()
    for tabname in reveal_globals.global_core_relations:
        cur.execute("Alter table " + tabname + "4 rename to " + tabname + "_in_temp;")
    cur.close()

    for att in reveal_globals.global_filter_predicates:
        try:
            filter_table_list[att[0]].append(att)
        except:
            filter_table_list[att[0]] = [att]
        # if att[1] not in reveal_globals.global_key_attributes:
        potential_in_attrib.append(att)

    for att in potential_in_attrib:
        temp_list.clear()
        temp_list.append(att)
        extract(level, att, [], attrib_types_dict)
        new_filter_list.append(copy.deepcopy(temp_list))
    print('Disjunction Time: ' + str(time.time() - global_start))

    reveal_globals.global_filter_predicates_disj = copy.deepcopy(new_filter_list)
    cur = reveal_globals.global_conn.cursor()
    for tabname in reveal_globals.global_core_relations:
        cur.execute('drop table if exists ' + tabname + '4;')
        cur.execute("Alter table " + tabname + "_in_temp rename to " + tabname + "4;")
    cur.close()


min_int = -214748364888.0
max_int = 214748364788.0
