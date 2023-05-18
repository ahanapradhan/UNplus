import os
import sys
import csv
import copy
import math
import datetime
import random
import executable

sys.path.append('../')
import reveal_globals
import psycopg2


def generateCombos(val):
    res = [[0]]
    for i in range(1, val):
        new_add = []
        for elt in res:
            temp = copy.deepcopy(elt)
            temp.append(i)
            new_add.append(temp)
        for elt in new_add:
            if len(elt) < val:
                res.append(copy.deepcopy(elt))
    return copy.deepcopy(res)


def get_all_combo_lists(max_list_len):
    result = {0: [], 1: []}
    for i in range(2, max_list_len + 1):
        result[i] = generateCombos(i)
    return result


def get_join_graph(reveal_globals):
    reveal_globals = get_init_data(reveal_globals)
    max_list_len = 2
    join_graph = []
    key_attributes = []
    for elt in reveal_globals.global_key_lists:
        if len(elt) > max_list_len:
            max_list_len = len(elt)
    combo_dict_of_lists = get_all_combo_lists(max_list_len)
    attrib_types_dict = {}
    for entry in reveal_globals.global_attrib_types:
        attrib_types_dict[(entry[0], entry[1])] = entry[2]
    dummy_int = 2
    dummy_char = 65  # to avoid having space/tab
    dummy_date = datetime.date(1000, 1, 1)
    # For each list, test its presence in join graph
    # This will either add the list in join graph or break it
    reveal_globals.global_attrib_dict['join'] = []
    k = 0
    while not (not reveal_globals.global_key_lists):
        curr_list = reveal_globals.global_key_lists[0]
        if len(curr_list) <= 1:
            reveal_globals.global_key_lists.remove(curr_list)
            continue
        k += 1
        reveal_globals.global_attrib_dict['join'].append("Component-" + str(k))
        reveal_globals.global_join_instance_dict['Component-' + str(k)] = []
        reveal_globals.global_component_dict['Component-' + str(k)] = curr_list
        # Try for all possible combinations
        for elt in combo_dict_of_lists[len(curr_list)]:
            reveal_globals.local_other_info_dict = {}
            list1 = []
            list_type = ''
            list2 = []
            for index in elt:
                list1.append(curr_list[index])
                if list_type == '':
                    list_type = attrib_types_dict[curr_list[index]]
            for val in curr_list:
                if val not in list1:
                    list2.append(val)
                # Assign two different values to two lists in database
            val1 = str(dummy_int)
            val2 = str(dummy_int + 1)
            if 'date' in list_type:
                val1 = str(dummy_date)
                val2 = str(dummy_date + datetime.timedelta(days=1))
            elif not ('int' in list_type or 'numeric' in list_type):
                val1 = str(chr(dummy_char))
                val2 = str(chr(dummy_char + 1))
            cur = reveal_globals.global_conn.cursor()
            temp_copy = {}
            for tab in reveal_globals.global_core_relations:
                temp_copy[tab] = reveal_globals.global_min_instance_dict[tab]
            for val in list1:
                cur.execute("update " + val[0] + " set " + val[1] + " = " + val1 + ";")
                index = temp_copy[val[0]][0].index(val[1])
                mutated_list = copy.deepcopy(list(temp_copy[val[0]][1]))
                mutated_list[index] = str(val1)
                temp_copy[val[0]][1] = tuple(mutated_list)
            cur.close()
            cur = reveal_globals.global_conn.cursor()
            for val in list2:
                cur.execute("update " + val[0] + " set " + val[1] + " = " + val2 + ";")
                index = temp_copy[val[0]][0].index(val[1])
                mutated_list = copy.deepcopy(list(temp_copy[val[0]][1]))
                mutated_list[index] = str(val2)
                temp_copy[val[0]][1] = tuple(mutated_list)
            cur.close()
            # Hardcoding for demo, need to be revised
            reveal_globals.global_join_instance_dict['Component-' + str(k)].append(
                u"" + list1[0][1] + u"\u2014" + list2[0][1])
            reveal_globals.local_other_info_dict['Current Mutation'] = 'Mutation of ' + list1[0][
                1] + ' with value ' + str(val1) + " and " + list2[0][1] + ' with value ' + str(val2)
            for tabname in reveal_globals.global_core_relations:
                reveal_globals.global_min_instance_dict[
                    'join_' + reveal_globals.global_attrib_dict['join'][-1] + '_' + tabname + '_' +
                    reveal_globals.global_join_instance_dict['Component-' + str(k)][-1]] = copy.deepcopy(
                    temp_copy[tabname])
            ########################################
            # CHECK THE RESULT
            new_result = executable.getExecOutput()
            reveal_globals.global_result_dict['join_' + reveal_globals.global_attrib_dict['join'][-1] + '_' +
                                              reveal_globals.global_join_instance_dict['Component-' + str(k)][
                                                  -1]] = new_result
            reveal_globals.local_other_info_dict['Result Cardinality'] = len(new_result) - 1
            if len(new_result) > 1:
                reveal_globals.local_other_info_dict[
                    'Conclusion'] = 'Selected edge(s) are not present in the join graph'
                reveal_globals.global_key_lists.remove(curr_list)
                reveal_globals.global_key_lists.append(copy.deepcopy(list1))
                reveal_globals.global_key_lists.append(copy.deepcopy(list2))
                break
        if curr_list in reveal_globals.global_key_lists:
            reveal_globals.global_key_lists.remove(curr_list)
            join_graph.append(copy.deepcopy(curr_list))
            reveal_globals.local_other_info_dict['Conclusion'] = u'Edge ' + list1[0][1] + u"\u2014" + list2[0][
                1] + ' is present in the join graph'
        # Assign same values in all cur_lists to get non-empty output
        reveal_globals.global_other_info_dict['join_' + reveal_globals.global_attrib_dict['join'][-1] + '_' +
                                              reveal_globals.global_join_instance_dict['Component-' + str(k)][
                                                  -1]] = copy.deepcopy(reveal_globals.local_other_info_dict)
        cur = reveal_globals.global_conn.cursor()
        for val in curr_list:
            cur.execute("Insert into " + val[0] + " Select * from " + val[0] + "4;")
            # cur.execute("copy " + val[0] + " from " + "'" + reveal_globals.global_reduced_data_path + val[0] + ".csv' " + "delimiter ',' csv header;")
        cur.close()
    # refine join graph and get all key attributes
    reveal_globals.global_join_graph = []
    reveal_globals.global_key_attributes = []
    for elt in join_graph:
        temp = []
        for val in elt:
            temp.append(val[1])
            reveal_globals.global_key_attributes.append(val[1])
        reveal_globals.global_join_graph.append(copy.deepcopy(temp))
    return


# SUPPORT FUNCTIONS FOR FILTER PREDICATES

def update_other_data(tabname, attrib, attrib_type, val, result, other_info_list, reveal_globals):
    reveal_globals.local_other_info_dict = {}
    if 'text' not in attrib_type and other_info_list != []:
        low = str(other_info_list[0])
        mid = str(other_info_list[1])
        high = str(other_info_list[2])
        low_next = str(other_info_list[3])
        high_next = str(other_info_list[4])
        reveal_globals.local_other_info_dict['Current Search Range'] = '[' + low + ', ' + high + ']'
        reveal_globals.local_other_info_dict[
            'Current Mutation'] = 'Mutation of attribute ' + attrib + ' with value ' + str(val)
        reveal_globals.local_other_info_dict['Result Cardinality'] = (len(result) - 1)
        reveal_globals.local_other_info_dict['New Search Range'] = '[' + low_next + ', ' + high_next + ']'
    else:
        reveal_globals.local_other_info_dict[
            'Current Mutation'] = 'Mutation of attribute ' + attrib + ' with value ' + str(val)
        reveal_globals.local_other_info_dict['Result Cardinality'] = str(len(result) - 1)
    temp = copy.deepcopy(reveal_globals.global_min_instance_dict[tabname])
    index = temp[0].index(attrib)
    mutated_list = copy.deepcopy(list(temp[1]))
    mutated_list[index] = str(val)
    temp[1] = mutated_list
    for tab in reveal_globals.global_core_relations:
        reveal_globals.global_min_instance_dict[
            'filter_' + attrib + '_' + tab + '_D_mut' + str(reveal_globals.local_instance_no)] = \
            reveal_globals.global_min_instance_dict[tab]
    reveal_globals.global_min_instance_dict[
        'filter_' + attrib + '_' + tabname + '_D_mut' + str(reveal_globals.local_instance_no)] = temp
    reveal_globals.global_result_dict[
        'filter_' + attrib + '_D_mut' + str(reveal_globals.local_instance_no)] = copy.deepcopy(result)
    reveal_globals.local_other_info_dict['Result Cardinality'] = str(len(result) - 1)
    reveal_globals.local_instance_list.append('D_mut' + str(reveal_globals.local_instance_no))
    reveal_globals.global_other_info_dict[
        'filter_' + attrib + '_D_mut' + str(reveal_globals.local_instance_no)] = copy.deepcopy(
        reveal_globals.local_other_info_dict)
    reveal_globals.local_instance_no += 1
    return reveal_globals


def is_int(s):
    try:
        int(s)
        return True
    except ValueError:
        return False


def get_init_data(reveal_globals):
    # Get all attributes, attribute types, max length and reduced db content
    reveal_globals.global_attrib_types = []
    reveal_globals.global_all_attribs = []
    reveal_globals.global_d_plus_value = {}
    reveal_globals.global_attrib_max_length = {}
    for tabname in reveal_globals.global_core_relations:
        '''
		#query to get key attributes
		query = "SELECT tc.table_schema,tc.constraint_name,tc.table_name,kcu.column_name,ccu.table_schema AS foreign_table_schema,\
		ccu.table_name AS foreign_table_name, ccu.column_name AS foreign_column_name FROM \
		information_schema.table_constraints AS tc JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name\
		AND tc.table_schema = kcu.table_schema JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name\
		AND ccu.table_schema = tc.table_schema\
		WHERE (tc.constraint_type = 'PRIMARY KEY' OR tc.constraint_type = 'FOREIGN KEY') AND tc.table_name = '" + tabname + "';"
		cur = reveal_globals.global_conn.cursor()
		cur.execute(query)
		res = cur.fetchall()
		cur.close()
		'''
        query = "select column_name, data_type, character_maximum_length from information_schema.columns where table_name = '" + tabname + "'; "
        cur = reveal_globals.global_conn.cursor()
        cur.execute(query)
        res = cur.fetchall()
        cur.close()
        tab_attribs = []
        for row in res:
            tab_attribs.append(row[0])
            reveal_globals.global_attrib_types.append((tabname, row[0], row[1]))
            if (is_int(str(row[2]))):
                reveal_globals.global_attrib_max_length[(tabname, row[0])] = int(str(row[2]))
        reveal_globals.global_all_attribs.append(copy.deepcopy(tab_attribs))
        # WRITE CODE TO GET INTEGER/DATE/FLOAT MIN AND MAX
        cur = reveal_globals.global_conn.cursor()
        query = 'select ' + tab_attribs[0]
        for i in range(1, len(tab_attribs)):
            query = query + ", " + tab_attribs[i]
        query = query + ' from ' + tabname + ';'
        cur.execute(query)
        res = cur.fetchall()
        cur.close()
        for row in res:
            for i in range(len(tab_attribs)):
                reveal_globals.global_d_plus_value[tab_attribs[i]] = row[i]
    return reveal_globals


def checkAttribValueEffect(tabname, attrib, val, reveal_globals):
    # updatequery
    query = "update " + tabname + " set " + attrib + " = " + str(val) + ";"
    cur = reveal_globals.global_conn.cursor()
    cur.execute(query)
    cur.close()
    new_result = executable.getExecOutput()
    if len(new_result) <= 1:
        cur = reveal_globals.global_conn.cursor()
        cur.execute("Truncate Table " + tabname + ";")
        cur.close()
        cur = reveal_globals.global_conn.cursor()
        # cur.execute("copy " + tabname + " from " + "'" + reveal_globals.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
        cur.execute("Insert into " + tabname + " Select * from " + tabname + "4;")
        cur.close()
    reveal_globals = update_other_data(tabname, attrib, 'int', val, new_result, [], reveal_globals)
    if len(new_result) > 1:
        return True, reveal_globals
    return False, reveal_globals


# mukul
def getFloatFilterValue(tabname, filter_attrib, min_val, max_val, operator, reveal_globals):
    query_front = "update " + str(tabname) + " set " + str(filter_attrib) + " = "
    cur = reveal_globals.global_conn.cursor()
    cur.execute("Truncate Table " + tabname + ";")
    # conn.commit()
    cur.close()
    cur = reveal_globals.global_conn.cursor()
    # cur.execute("copy " + tabname + " from " + "'" + reveal_globals.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
    cur.execute("Insert into " + tabname + " Select * from " + tabname + "4;")
    # conn.commit()
    cur.close()
    if operator == '<=':
        low = float(min_val)
        high = float(max_val)
        while (high - low) > 0.00001:
            mid_val = (low + high) / 2
            print("[low,high,mid]", low, high, mid_val)
            # updatequery
            query = query_front + " " + str(round(mid_val, 2)) + " ;"
            cur = reveal_globals.global_conn.cursor()
            cur.execute(query)
            cur.close()
            new_result = executable.getExecOutput()
            print(new_result, mid_val)
            if len(new_result) <= 1:
                # put filter_
                reveal_globals = update_other_data(tabname, filter_attrib, 'float', mid_val, new_result,
                                  [low, mid_val, high, low, mid_val - 0.01], reveal_globals)
                high = mid_val - 0.01
            else:
                # put filter_
                reveal_globals = update_other_data(tabname, filter_attrib, 'float', mid_val, new_result,
                                  [low, mid_val, high, mid_val, high], reveal_globals)
                low = mid_val
            cur = reveal_globals.global_conn.cursor()
            cur.execute('TRUNCATE table ' + tabname + ';')
            # cur.execute("copy " + tabname + " from " + "'" + reveal_globals.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
            cur.execute("Insert into " + tabname + " Select * from " + tabname + "4;")
            cur.close()
        return (low), reveal_globals

    if operator == '>=':
        low = float(min_val)
        high = float(max_val)
        while (high - low) > 0.00001:
            mid_val = (low + high) / 2
            print("[low,high,mid]", low, high, mid_val)
            # updatequery
            query = query_front + " " + str(round(mid_val, 2)) + " ;"
            cur = reveal_globals.global_conn.cursor()
            cur.execute(query)
            cur.close()
            new_result = executable.getExecOutput()
            if len(new_result) <= 1:
                # put filter_
                reveal_globals = update_other_data(tabname, filter_attrib, 'float', mid_val, new_result,
                                  [low, mid_val, high, mid_val + 0.01, high], reveal_globals)
                low = mid_val + 0.01
            else:
                # put filter_
                reveal_globals = update_other_data(tabname, filter_attrib, 'float', mid_val, new_result,
                                  [low, mid_val, high, low, mid_val], reveal_globals)
                high = mid_val
            cur = reveal_globals.global_conn.cursor()
            cur.execute('TRUNCATE table ' + tabname + ';')
            # cur.execute("copy " + tabname + " from " + "'" + reveal_globals.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
            cur.execute("Insert into " + tabname + " Select * from " + tabname + "4;")
            cur.close()
        return (high), reveal_globals
    return False, reveal_globals


def getIntFilterValue(tabname, filter_attrib, min_val, max_val, operator, reveal_globals):
    counter = 0
    query_front = "update " + tabname + " set " + filter_attrib + " = "
    query_back = ""
    firstflag = True
    cur = reveal_globals.global_conn.cursor()
    cur.execute("Truncate Table " + tabname + ";")
    # conn.commit()
    cur.close()
    cur = reveal_globals.global_conn.cursor()
    # cur.execute("copy " + tabname + " from " + "'" + reveal_globals.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
    # conn.commit()
    cur.execute("Insert into " + tabname + " Select * from " + tabname + "4;")
    cur.close()
    if operator == '<=':
        low = min_val
        high = max_val
        while (high - low) > 0:
            mid_val = int(math.ceil((low + high) / 2))
            # updatequery
            query = query_front + " " + str(mid_val) + " " + query_back + ";"
            cur = reveal_globals.global_conn.cursor()
            cur.execute(query)
            cur.close()
            new_result = executable.getExecOutput()
            if len(new_result) <= 1:
                # put filter_
                reveal_globals = update_other_data(tabname, filter_attrib, 'int', mid_val, new_result,
                                  [low, mid_val, high, low, mid_val - 1], reveal_globals)
                high = mid_val - 1
            else:
                # put filter_
                reveal_globals = update_other_data(tabname, filter_attrib, 'int', mid_val, new_result,
                                  [low, mid_val, high, mid_val, high], reveal_globals)
                low = mid_val
            cur = reveal_globals.global_conn.cursor()
            cur.execute('TRUNCATE table ' + tabname + ';')
            # cur.execute("copy " + tabname + " from " + "'" + reveal_globals.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
            cur.execute("Insert into " + tabname + " Select * from " + tabname + "4;")
            cur.close()
        return str(low), reveal_globals
    if operator == '>=':
        low = min_val
        high = max_val
        while (high - low) > 0:
            mid_val = int((low + high) / 2)
            # updatequery
            query = query_front + " " + str(mid_val) + " " + query_back + ";"
            cur = reveal_globals.global_conn.cursor()
            cur.execute(query)
            cur.close()
            new_result = executable.getExecOutput()
            if len(new_result) <= 1:
                # put filter_
                reveal_globals = update_other_data(tabname, filter_attrib, 'int', mid_val, new_result,
                                  [low, mid_val, high, mid_val + 1, high], reveal_globals)
                low = mid_val + 1
            else:
                # put filter_
                reveal_globals = update_other_data(tabname, filter_attrib, 'int', mid_val, new_result,
                                  [low, mid_val, high, low, mid_val], reveal_globals)
                high = mid_val
            cur = reveal_globals.global_conn.cursor()
            cur.execute('TRUNCATE table ' + tabname + ';')
            # cur.execute("copy " + tabname + " from " + "'" + reveal_globals.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
            cur.execute("Insert into " + tabname + " Select * from " + tabname + "4;")
            cur.close()
        return str(high), reveal_globals
    if operator == '=':
        low = min_val
        high = max_val
        flag_low = True
        flag_high = True
        # updatequery
        query = query_front + " " + str(low) + " " + query_back + ";"
        cur = reveal_globals.global_conn.cursor()
        cur.execute(query)
        cur.close()
        new_result = executable.getExecOutput()
        if len(new_result) <= 1:
            flag_low = False
        # put filter_
        reveal_globals = update_other_data(tabname, filter_attrib, 'int', low, new_result, [], reveal_globals)
        query = query_front + " " + str(high) + " " + query_back + ";"
        cur = reveal_globals.global_conn.cursor()
        cur.execute(query)
        cur.close()
        new_result = executable.getExecOutput()
        # put filter_
        reveal_globals = update_other_data(tabname, filter_attrib, 'int', high, new_result, [], reveal_globals)
        cur = reveal_globals.global_conn.cursor()
        cur.execute('TRUNCATE table ' + tabname + ';')
        # cur.execute("copy " + tabname + " from " + "'" + reveal_globals.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
        cur.execute("Insert into " + tabname + " Select * from " + tabname + "4;")
        cur.close()
        if len(new_result) <= 1:
            flag_high = False
        return (flag_low == False and flag_high == False), reveal_globals
    return False, reveal_globals


def getDateFilterValue(tabname, attrib, min_val, max_val, operator, reveal_globals):
    counter = 0
    query_front = "update " + tabname + " set " + attrib + " = "
    query_back = ""
    firstflag = True
    cur = reveal_globals.global_conn.cursor()
    cur.execute("Truncate Table " + tabname + ";")
    cur.close()
    cur = reveal_globals.global_conn.cursor()
    # cur.execute("copy " + tabname + " from " + "'" + reveal_globals.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
    cur.execute("Insert into " + tabname + " Select * from " + tabname + "4;")
    cur.close()
    if operator == '<=':
        low = min_val
        high = max_val
        while int((high - low).days) > 0:
            mid_val = low + datetime.timedelta(days=int(math.ceil(((high - low).days) / 2)))
            # updatequery
            query = query_front + " '" + str(mid_val) + "' " + query_back + ";"
            cur = reveal_globals.global_conn.cursor()
            cur.execute(query)
            cur.close()
            new_result = executable.getExecOutput()
            if len(new_result) <= 1:
                reveal_globals = update_other_data(tabname, attrib, 'int', mid_val, new_result,
                                  [low, mid_val, high, low, mid_val - datetime.timedelta(days=1)], reveal_globals)
                high = mid_val - datetime.timedelta(days=1)
            else:
                reveal_globals = update_other_data(tabname, attrib, 'int', mid_val, new_result, [low, mid_val, high, mid_val, high], reveal_globals)
                low = mid_val
            cur = reveal_globals.global_conn.cursor()
            cur.execute('TRUNCATE table ' + tabname + ';')
            # cur.execute("copy " + tabname + " from " + "'" + reveal_globals.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
            cur.execute("Insert into " + tabname + " Select * from " + tabname + "4;")
            cur.close()
        return low, reveal_globals
    if operator == '>=':
        low = min_val
        high = max_val
        while int((high - low).days) > 0:
            mid_val = low + datetime.timedelta(days=int(((high - low).days) / 2))
            # updatequery
            query = query_front + " '" + str(mid_val) + "' " + query_back + ";"
            cur = reveal_globals.global_conn.cursor()
            cur.execute(query)
            cur.close()
            new_result = executable.getExecOutput()
            if len(new_result) <= 1:
                reveal_globals = update_other_data(tabname, attrib, 'int', mid_val, new_result,
                                  [low, mid_val, high, mid_val + datetime.timedelta(days=1), high], reveal_globals)
                low = mid_val + datetime.timedelta(days=1)
            else:
                reveal_globals = update_other_data(tabname, attrib, 'int', mid_val, new_result, [low, mid_val, high, low, mid_val], reveal_globals)
                high = mid_val
            cur = reveal_globals.global_conn.cursor()
            cur.execute('TRUNCATE table ' + tabname + ';')
            # cur.execute("copy " + tabname + " from " + "'" + reveal_globals.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
            cur.execute("Insert into " + tabname + " Select * from " + tabname + "4;")
            cur.close()
        return high, reveal_globals
    if operator == '=':
        low = min_val
        high = max_val
        flag_low = True
        flag_high = True
        # updatequery
        query = query_front + " '" + str(low) + "' " + query_back + ";"
        cur = reveal_globals.global_conn.cursor()
        cur.execute(query)
        cur.close()
        new_result = executable.getExecOutput()
        reveal_globals = update_other_data(tabname, attrib, 'int', low, new_result, [], reveal_globals)
        if len(new_result) <= 1:
            flag_low = False
        query = query_front + " '" + str(high) + "' " + query_back + ";"
        cur = reveal_globals.global_conn.cursor()
        cur.execute(query)
        cur.close()
        new_result = executable.getExecOutput()
        reveal_globals = update_other_data(tabname, attrib, 'int', high, new_result, [], reveal_globals)
        cur = reveal_globals.global_conn.cursor()
        cur.execute('TRUNCATE table ' + tabname + ';')
        # cur.execute("copy " + tabname + " from " + "'" + reveal_globals.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
        cur.execute("Insert into " + tabname + " Select * from " + tabname + "4;")
        cur.close()
        if len(new_result) <= 1:
            flag_high = False
        return (flag_low == False and flag_high == False), reveal_globals
    return False, reveal_globals


def checkStringPredicate(tabname, attrib, reveal_globals):
    # updatequery
    val = ''
    if reveal_globals.global_d_plus_value[attrib] is not None and reveal_globals.global_d_plus_value[attrib][0] == 'a':
        query = "update " + tabname + " set " + attrib + " = " + "'b';"
        val = 'b'
    else:
        query = "update " + tabname + " set " + attrib + " = " + "'a';"
        val = 'a'
    cur = reveal_globals.global_conn.cursor()
    cur.execute(query)
    cur.close()
    new_result = executable.getExecOutput()
    reveal_globals = update_other_data(tabname, attrib, 'text', val, new_result, [], reveal_globals)
    if len(new_result) <= 1:
        cur = reveal_globals.global_conn.cursor()
        cur.execute("Truncate Table " + tabname + ";")
        # conn.commit()
        cur.close()
        cur = reveal_globals.global_conn.cursor()
        # cur.execute("copy " + tabname + " from " + "'" + reveal_globals.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
        # conn.commit()
        cur.execute("Insert into " + tabname + " Select * from " + tabname + "4;")
        cur.close()
        return True, reveal_globals
    query = "update " + tabname + " set " + attrib + " = " + "' ';"
    cur = reveal_globals.global_conn.cursor()
    cur.execute(query)
    cur.close()
    new_result = executable.getExecOutput()
    reveal_globals = update_other_data(tabname, attrib, 'text', "''", new_result, [], reveal_globals)
    if len(new_result) <= 1:
        cur = reveal_globals.global_conn.cursor()
        cur.execute("Truncate Table " + tabname + ";")
        cur.close()
        cur = reveal_globals.global_conn.cursor()
        # cur.execute("copy " + tabname + " from " + "'" + reveal_globals.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
        cur.execute("Insert into " + tabname + " Select * from " + tabname + "4;")
        cur.close()
        return True, reveal_globals
    return False, reveal_globals


def getStrFilterValue(tabname, attrib, representative, max_length):
    index = 0
    output = ""
    # currently inverted exclaimaination is being used assuming it will not be in the string
    # GET minimal string with _
    while (index < len(representative)):
        temp = list(representative)
        if temp[index] == 'a':
            temp[index] = 'b'
        else:
            temp[index] = 'a'
        temp = ''.join(temp)
        # updatequery
        query = "update " + tabname + " set " + attrib + " = " + "'" + temp + "';"
        cur = reveal_globals.global_conn.cursor()
        cur.execute(query)
        # conn.commit()
        cur.close()
        new_result = executable.getExecOutput()
        reveal_globals = update_other_data(tabname, attrib, 'text', temp, new_result, [], reveal_globals)
        if len(new_result) > 1:
            reveal_globals.local_other_info_dict['Conclusion'] = "'" + representative[
                index] + "' is a replacement for wildcard character '%' or '_'"
            reveal_globals.global_other_info_dict[
                'filter_' + attrib + '_D_mut' + str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(
                reveal_globals.local_other_info_dict)
            temp = copy.deepcopy(representative)
            temp = temp[:index] + temp[index + 1:]
            # updatequery
            query = "update " + tabname + " set " + attrib + " = " + "'" + temp + "';"
            cur = reveal_globals.global_conn.cursor()
            cur.execute(query)
            cur.close()
            new_result = executable.getExecOutput()
            reveal_globals = update_other_data(tabname, attrib, 'text', temp, new_result, [], reveal_globals)
            if len(new_result) > 1:
                reveal_globals.local_other_info_dict['Conclusion'] = "'" + representative[
                    index] + "' is a replacement from wildcard character '%'"
                reveal_globals.global_other_info_dict[
                    'filter_' + attrib + '_D_mut' + str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(
                    reveal_globals.local_other_info_dict)
                representative = representative[:index] + representative[index + 1:]
            else:
                reveal_globals.local_other_info_dict['Conclusion'] = "'" + representative[
                    index] + "' is a replacement from wildcard character '_'"
                reveal_globals.global_other_info_dict[
                    'filter_' + attrib + '_D_mut' + str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(
                    reveal_globals.local_other_info_dict)
                output = output + "_"
                representative = list(representative)
                representative[index] = u"\u00A1"
                representative = ''.join(representative)
                index = index + 1
        else:
            reveal_globals.local_other_info_dict['Conclusion'] = "'" + representative[
                index] + "' is an intrinsic character in filter value"
            reveal_globals.global_other_info_dict[
                'filter_' + attrib + '_D_mut' + str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(
                reveal_globals.local_other_info_dict)
            output = output + representative[index]
            index = index + 1
    if output == '':
        return output
    # GET % positions
    index = 0
    representative = copy.deepcopy(output)
    if (len(representative) < max_length):
        output = ""
        while index < len(representative):
            temp = list(representative)
            if temp[index] == 'a':
                temp.insert(index, 'b')
            else:
                temp.insert(index, 'a')
            temp = ''.join(temp)
            # updatequery
            query = "update " + tabname + " set " + attrib + " = " + "'" + temp + "';"
            cur = reveal_globals.global_conn.cursor()
            cur.execute(query)
            # conn.commit()
            cur.close()
            new_result = executable.getExecOutput()
            reveal_globals = update_other_data(tabname, attrib, 'text', temp, new_result, [], reveal_globals)
            if len(new_result) > 1:
                output = output + '%'
            output = output + representative[index]
            index = index + 1
        temp = list(representative)
        if temp[index - 1] == 'a':
            temp.append('b')
        else:
            temp.append('a')
        temp = ''.join(temp)
        # updatequery
        query = "update " + tabname + " set " + attrib + " = " + "'" + temp + "';"
        cur = reveal_globals.global_conn.cursor()
        cur.execute(query)
        # conn.commit()
        cur.close()
        new_result = executable.getExecOutput()
        reveal_globals = update_other_data(tabname, attrib, 'text', temp, new_result, [], reveal_globals)
        if len(new_result) > 1:
            output = output + '%'
    return output


# FILTER PREDICATES FUNCTION

def get_filter_predicates(reveal_globals):
    print(reveal_globals.global_all_attribs)
    reveal_globals.local_other_info_dict = {}
    reveal_globals.global_attrib_dict['filter'] = []
    reveal_globals = get_init_data(reveal_globals)
    filterAttribs = []
    total_attribs = 0
    attrib_types_dict = {}
    d_plus_value = copy.deepcopy(reveal_globals.global_d_plus_value)
    attrib_max_length = copy.deepcopy(reveal_globals.global_attrib_max_length)
    for entry in reveal_globals.global_attrib_types:
        attrib_types_dict[(entry[0], entry[1])] = entry[2]
        # aoa change
        reveal_globals.global_attrib_types_dict[(entry[0], entry[1])] = entry[2]
    result_pos = 0
    for i in range(len(reveal_globals.global_core_relations)):
        tabname = reveal_globals.global_core_relations[i]
        attrib_list = reveal_globals.global_all_attribs[i]
        total_attribs = total_attribs + len(attrib_list)
        for attrib in attrib_list:
            if attrib not in reveal_globals.global_key_attributes:
                reveal_globals.global_attrib_dict['filter'].append(attrib)
                reveal_globals.local_other_info_dict = {}
                reveal_globals.local_instance_no = 1
                reveal_globals.global_instance_dict[attrib] = []
                reveal_globals.local_instance_list = []
                if 'int' in attrib_types_dict[(tabname, attrib)]:
                    # NUMERIC HANDLING
                    # min and max domain values (initialize based on data type)
                    min_val_domain = -2147483648
                    max_val_domain = 2147483647
                    flag_min, reveal_globals = checkAttribValueEffect(tabname, attrib,
                                                      min_val_domain,
                                                      reveal_globals)  # True implies row was still present
                    flag_max, reveal_globals = checkAttribValueEffect(tabname, attrib,
                                                      max_val_domain,
                                                      reveal_globals)  # True implies row was still present
                    # inference based on flag_min and flag_max
                    if flag_max and flag_min:
                        reveal_globals.local_other_info_dict['Conclusion'] = 'No filter on attribute ' + attrib
                        reveal_globals.global_other_info_dict[
                            'filter_' + attrib + '_D_mut' + str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(
                            reveal_globals.local_other_info_dict)
                    elif not flag_min and not flag_max:
                        reveal_globals.local_other_info_dict[
                            'Conclusion'] = 'Filter predicate on ' + attrib + ' with operator between'
                        reveal_globals.global_other_info_dict[
                            'filter_' + attrib + '_D_mut' + str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(
                            reveal_globals.local_other_info_dict)
                        print('identifying value for Int filter(range) attribute..', attrib)
                        equalto_flag, reveal_globals = getIntFilterValue(tabname, attrib, int(d_plus_value[attrib]) - 1,
                                                         int(d_plus_value[attrib]) + 1, '=', reveal_globals)
                        if equalto_flag:
                            filterAttribs.append(
                                (tabname, attrib, '=', int(d_plus_value[attrib]), int(d_plus_value[attrib])))
                            reveal_globals.local_other_info_dict[
                                'Conclusion'] = u'Filter predicate is \u2013 ' + attrib + ' = ' + str(
                                d_plus_value[attrib])
                            reveal_globals.global_other_info_dict['filter_' + attrib + '_D_mut' + str(
                                reveal_globals.local_instance_no - 1)] = copy.deepcopy(
                                reveal_globals.local_other_info_dict)
                        else:
                            val1, reveal_globals = getIntFilterValue(tabname, attrib, int(d_plus_value[attrib]), max_val_domain - 1,
                                                     '<=', reveal_globals)
                            val2, reveal_globals = getIntFilterValue(tabname, attrib, min_val_domain + 1, int(d_plus_value[attrib]),
                                                     '>=', reveal_globals)
                            filterAttribs.append((tabname, attrib, 'range', int(val2), int(val1)))
                            reveal_globals.local_other_info_dict[
                                'Conclusion'] = u'Filter Predicate is \u2013 ' + attrib + ' between ' + str(
                                val2) + ' and ' + str(val1)
                            reveal_globals.global_other_info_dict['filter_' + attrib + '_D_mut' + str(
                                reveal_globals.local_instance_no - 1)] = copy.deepcopy(
                                reveal_globals.local_other_info_dict)
                    elif flag_min and not flag_max:
                        reveal_globals.local_other_info_dict[
                            'Conclusion'] = 'Filter predicate on ' + attrib + ' with operator <='
                        reveal_globals.global_other_info_dict[
                            'filter_' + attrib + '_D_mut' + str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(
                            reveal_globals.local_other_info_dict)
                        print('identifying value for Int filter attribute', attrib)
                        val, reveal_globals = getIntFilterValue(tabname, attrib, int(d_plus_value[attrib]), max_val_domain - 1, '<=', reveal_globals)
                        filterAttribs.append((tabname, attrib, '<=', int(min_val_domain), int(val)))
                        reveal_globals.local_other_info_dict[
                            'Conclusion'] = u'Filter Predicate is \u2013 ' + attrib + ' <= ' + str(val)
                        reveal_globals.global_other_info_dict[
                            'filter_' + attrib + '_D_mut' + str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(
                            reveal_globals.local_other_info_dict)
                    elif not flag_min and flag_max:
                        reveal_globals.local_other_info_dict[
                            'Conclusion'] = 'Filter predicate on ' + attrib + ' with operator >='
                        reveal_globals.global_other_info_dict[
                            'filter_' + attrib + '_D_mut' + str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(
                            reveal_globals.local_other_info_dict)
                        print('identifying value for Int filter attribute', attrib)
                        val, reveal_globals = getIntFilterValue(tabname, attrib, min_val_domain + 1, int(d_plus_value[attrib]), '>=', reveal_globals)
                        filterAttribs.append((tabname, attrib, '>=', int(val), int(max_val_domain)))
                        reveal_globals.local_other_info_dict[
                            'Conclusion'] = u'Filter Predicate is \u2013 ' + attrib + ' >= ' + str(val)
                        reveal_globals.global_other_info_dict[
                            'filter_' + attrib + '_D_mut' + str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(
                            reveal_globals.local_other_info_dict)
                elif 'text' in attrib_types_dict[(tabname, attrib)] or 'char' in attrib_types_dict[
                    (tabname, attrib)] or 'varbit' == attrib_types_dict[(tabname, attrib)]:
                    # STRING HANDLING
                    # ESCAPE CHARACTERS IN STRING REMAINING
                    done, reveal_globals = checkStringPredicate(tabname, attrib, reveal_globals)
                    if done:
                        # returns true if there is predicate on this string attribute
                        reveal_globals.local_other_info_dict['Conclusion'] = 'Filter Predicate on ' + attrib
                        reveal_globals.global_other_info_dict[
                            'filter_' + attrib + '_D_mut' + str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(
                            reveal_globals.local_other_info_dict)
                        print('identifying value for String filter attribute', attrib)
                        representative = str(d_plus_value[attrib])
                        max_length = 100000
                        if (tabname, attrib) in attrib_max_length.keys():
                            max_length = attrib_max_length[(tabname, attrib)]
                        val = getStrFilterValue(tabname, attrib, representative, max_length)
                        if ('%' in val or '_' in val):
                            filterAttribs.append((tabname, attrib, 'LIKE', val, val))
                            reveal_globals.local_other_info_dict[
                                'Conclusion'] = u'Filter Predicate is \u2013 ' + attrib + ' LIKE ' + str(val)
                        else:
                            reveal_globals.local_other_info_dict[
                                'Conclusion'] = u'Filter Predicate is \u2013 ' + attrib + ' = ' + str(val)
                            filterAttribs.append((tabname, attrib, 'equal', val, val))
                        reveal_globals.global_other_info_dict[
                            'filter_' + attrib + '_D_mut' + str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(
                            reveal_globals.local_other_info_dict)
                    else:
                        reveal_globals.local_other_info_dict['Conclusion'] = 'No Filter predicate on ' + attrib
                        reveal_globals.global_other_info_dict[
                            'filter_' + attrib + '_D_mut' + str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(
                            reveal_globals.local_other_info_dict)
                    # update table so that result is not empty
                    cur = reveal_globals.global_conn.cursor()
                    cur.execute("Truncate table " + tabname + ';')
                    # cur.execute("copy " + tabname + " from " + "'" + reveal_globals.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
                    cur.execute("Insert into " + tabname + " Select * from " + tabname + "4;")
                    cur.close()
                elif 'date' in attrib_types_dict[(tabname, attrib)]:
                    # min and max domain values (initialize based on data type)
                    # PLEASE CONFIRM THAT DATE FORMAT IN DATABASE IS YYYY-MM-DD
                    min_val_domain = datetime.date(1, 1, 1)
                    max_val_domain = datetime.date(9999, 12, 31)
                    flag_min, reveal_globals = checkAttribValueEffect(tabname, attrib, "'" + str(
                        min_val_domain) + "'", reveal_globals)  # True implies row was still present
                    flag_max, reveal_globals = checkAttribValueEffect(tabname, attrib, "'" + str(
                        max_val_domain) + "'", reveal_globals)  # True implies row was still present
                    # inference based on flag_min and flag_max
                    if flag_max and flag_min:
                        reveal_globals.local_other_info_dict['Conclusion'] = 'No Filter predicate on ' + attrib
                        reveal_globals.global_other_info_dict[
                            'filter_' + attrib + '_D_mut' + str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(
                            reveal_globals.local_other_info_dict)
                    elif not flag_min and not flag_max:
                        reveal_globals.local_other_info_dict[
                            'Conclusion'] = 'Filter predicate on ' + attrib + ' with operator between '
                        reveal_globals.global_other_info_dict[
                            'filter_' + attrib + '_D_mut' + str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(
                            reveal_globals.local_other_info_dict)
                        print('identifying value for Date filter(range) attribute..', attrib)
                        equalto_flag, reveal_globals = getDateFilterValue(tabname, attrib,
                                                          d_plus_value[attrib] - datetime.timedelta(days=1),
                                                          d_plus_value[attrib] + datetime.timedelta(days=1), '=', reveal_globals)
                        if equalto_flag:
                            filterAttribs.append((tabname, attrib, '=', d_plus_value[attrib], d_plus_value[attrib]))
                            reveal_globals.local_other_info_dict[
                                'Conclusion'] = u'Filter Predicate is \u2013 ' + attrib + ' = ' + str(
                                d_plus_value[attrib])
                            reveal_globals.global_other_info_dict['filter_' + attrib + '_D_mut' + str(
                                reveal_globals.local_instance_no - 1)] = copy.deepcopy(
                                reveal_globals.local_other_info_dict)
                        else:
                            val1, reveal_globals = getDateFilterValue(tabname, attrib, d_plus_value[attrib],
                                                      max_val_domain - datetime.timedelta(days=1), '<=', reveal_globals)
                            val2, reveal_globals = getDateFilterValue(tabname, attrib, min_val_domain + datetime.timedelta(days=1),
                                                      d_plus_value[attrib], '>=', reveal_globals)
                            filterAttribs.append((tabname, attrib, 'range', val2, val1))
                            reveal_globals.local_other_info_dict[
                                'Conclusion'] = u'Filter Predicate is \u2013 ' + attrib + ' between ' + str(
                                val2) + ' and ' + str(val1)
                            reveal_globals.global_other_info_dict['filter_' + attrib + '_D_mut' + str(
                                reveal_globals.local_instance_no - 1)] = copy.deepcopy(
                                reveal_globals.local_other_info_dict)
                    elif flag_min and not flag_max:
                        reveal_globals.local_other_info_dict[
                            'Conclusion'] = 'Filter predicate on ' + attrib + ' with operator <='
                        reveal_globals.global_other_info_dict[
                            'filter_' + attrib + '_D_mut' + str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(
                            reveal_globals.local_other_info_dict)
                        print('identifying value for Date filter attribute', attrib)
                        val, reveal_globals = getDateFilterValue(tabname, attrib, d_plus_value[attrib],
                                                 max_val_domain - datetime.timedelta(days=1), '<=', reveal_globals)
                        filterAttribs.append((tabname, attrib, '<=', min_val_domain, val))
                        reveal_globals.local_other_info_dict[
                            'Conclusion'] = u'Filter Predicate is \u2013 ' + attrib + ' <= ' + str(val)
                        reveal_globals.global_other_info_dict[
                            'filter_' + attrib + '_D_mut' + str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(
                            reveal_globals.local_other_info_dict)
                    elif not flag_min and flag_max:
                        reveal_globals.local_other_info_dict[
                            'Conclusion'] = 'Filter predicate on ' + attrib + ' with operator >= '
                        reveal_globals.global_other_info_dict[
                            'filter_' + attrib + '_D_mut' + str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(
                            reveal_globals.local_other_info_dict)
                        print('identifying value for Date filter attribute', attrib)
                        val, reveal_globals = getDateFilterValue(tabname, attrib, min_val_domain + datetime.timedelta(days=1),
                                                 d_plus_value[attrib], '>=', reveal_globals)
                        filterAttribs.append((tabname, attrib, '>=', val, max_val_domain))
                        reveal_globals.local_other_info_dict[
                            'Conclusion'] = u'Filter Predicate is \u2013 ' + attrib + ' >= ' + str(val)
                        reveal_globals.global_other_info_dict[
                            'filter_' + attrib + '_D_mut' + str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(
                            reveal_globals.local_other_info_dict)
                elif 'numeric' in attrib_types_dict[(tabname, attrib)]:
                    # NUMERIC HANDLING
                    # min and max domain values (initialize based on data type)
                    min_val_domain = -214748364888
                    max_val_domain = 214748364788
                    # PRECISION TO BE GET FROM SCHEMA GRAPH
                    precision = 2
                    flag_min, reveal_globals = checkAttribValueEffect(tabname, attrib,
                                                      min_val_domain,
                                                      reveal_globals)  # True implies row was still present
                    flag_max, reveal_globals = checkAttribValueEffect(tabname, attrib,
                                                      max_val_domain,
                                                      reveal_globals)  # True implies row was still present
                    # inference based on flag_min and flag_max
                    if flag_max and flag_min:
                        reveal_globals.local_other_info_dict['Conclusion'] = 'No Filter predicate on ' + attrib
                    elif not flag_min and not flag_max:
                        print('identifying value for Int filter(range) attribute..', attrib)
                        equalto_flag, reveal_globals = getIntFilterValue(tabname, attrib, float(d_plus_value[attrib]) - .01,
                                                         float(d_plus_value[attrib]) + .01, '=', reveal_globals)
                        if equalto_flag:
                            filterAttribs.append(
                                (tabname, attrib, '=', float(d_plus_value[attrib]), float(d_plus_value[attrib])))
                        else:
                            val1, reveal_globals = getIntFilterValue(tabname, attrib, math.ceil(float(d_plus_value[attrib])),
                                                     max_val_domain, '<=', reveal_globals)
                            val2, reveal_globals = getIntFilterValue(tabname, attrib, min_val_domain,
                                                     math.floor(float(d_plus_value[attrib])), '>=', reveal_globals)
                            filterAttribs.append((tabname, attrib, 'range', float(val2), float(val1)))
                    elif flag_min and not flag_max:
                        print('identifying value for Int filter attribute', attrib)
                        val, reveal_globals = getIntFilterValue(tabname, attrib, math.ceil(float(d_plus_value[attrib])) - 5,
                                                max_val_domain, '<=', reveal_globals)
                        val = float(val)
                        val1, reveal_globals = getFloatFilterValue(tabname, attrib, val, val + 0.99, '<=', reveal_globals)
                        filterAttribs.append((tabname, attrib, '<=', float(min_val_domain), float(round(val1, 2))))
                    elif not flag_min and flag_max:
                        print('identifying value for Int filter attribute', attrib)
                        val, reveal_globals = getIntFilterValue(tabname, attrib, min_val_domain,
                                                math.floor(float(d_plus_value[attrib]) + 5), '>=', reveal_globals)
                        val = float(val)
                        val1, reveal_globals = getFloatFilterValue(tabname, attrib, val - 1, val, '>=', reveal_globals)
                        filterAttribs.append((tabname, attrib, '>=', float(round(val1, 2)), float(max_val_domain)))
                reveal_globals.global_instance_dict['filter_' + attrib] = copy.deepcopy(
                    reveal_globals.local_instance_list)
    print("filterAttribs", filterAttribs)
    return filterAttribs, reveal_globals
