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
max_int_val =  2147483647
orig_filter = []

min_date_val = datetime.date(1,1,1)
max_date_val = datetime.date(9999,12,31)

class Graph:
    def __init__(self,n):
        self.graph = defaultdict(list)
        self.N = n
    def addEdge(self,m,n):
        self.graph[m].append(n)
    def sortUtil(self,n,visited,stack):
        visited[n] = True
        for element in self.graph[n]:
            if visited[element] == False:
                self.sortUtil(element,visited,stack)
        stack.insert(0,n)
    def topologicalSort(self):
        visited = [False]*self.N
        stack =[]
        for element in range(self.N):
            if visited[element] == False:
                self.sortUtil(element,visited,stack)
        print(stack)
        return stack

def extract_aoa():
    reveal_globals.global_proj = reveal_globals.global_filter_predicates
    # print("In aoa", reveal_globals.global_filter_predicates)
    reveal_globals.global_AoA = 1
    # instance_dict = copy.deepcopy(reveal_globals.global_result_dict)
    orig_filter = copy.deepcopy(reveal_globals.global_filter_predicates)
    for tab in reveal_globals.global_core_relations:
        cur = reveal_globals.global_conn.cursor()
        cur.execute('Drop table if exists new_' + tab + ';') 
        cur.execute('Create unlogged table new_' + tab + ' (like ' + tab + '4);')
        cur.execute('Insert into new_' + tab + ' select * from ' + tab + '4;')
        cur.execute('alter table new_' + tab + ' add primary key(' + reveal_globals.global_pk_dict[tab] + ');')
        cur.close()
    reveal_globals.local_start_time = time.time() #aman
    C = []
    cols = []
    final_pred = []
    for c in orig_filter:
        # yet to handle join case
        if c[2] == 'range':
            # add geq and leq
            lst = []
            lst.append(c[0])
            lst.append(c[1])
            lst.append('>=')
            lst.append(c[3])
            C.append(lst)
            lst = []
            lst.append(c[0])
            lst.append(c[1])
            lst.append('<=')
            lst.append(c[4])
            C.append(lst)
            cols.append((c[0],c[1]))
        elif c[2] == '>=':
            lst =[]
            lst.append(c[0])
            lst.append(c[1])
            lst.append('>=')
            lst.append(c[3])
            C.append(lst)
            cols.append((c[0],c[1]))
        elif c[2] == '<=':
            lst = []
            lst.append(c[0])
            lst.append(c[1])
            lst.append('<=')
            lst.append(c[4])
            C.append(lst)
            cols.append((c[0],c[1]))
        elif c[2] == '=':
            lst = []
            lst.append(c[0])
            lst.append(c[1])
            lst.append('=')
            lst.append(c[3])
            C.append(lst)
            cols.append((c[0],c[1]))
    print("Filter pred: ", C)
    print("Var Cols: ", cols)
    
    
    
    #step1
    # for checking possibility of equality condition between two attributes
    start_time = time.time()
    for pred in C:
        print("Step 1")
        chk = 0
        pos_e = []
        if pred[2] == '=':
            for c in cols:
                
                if c[1] == pred[1]:
                    continue
                # SQL QUERY for checking value
                cur = reveal_globals.global_conn.cursor()
                cur.execute("SELECT " + c[1] + " FROM new_" + c[0] + " ;")
                prev = cur.fetchone()
                prev = prev[0]
                cur.close()
                if 'int' in reveal_globals.global_attrib_types_dict[(c[0],c[1])]:
                    val = int(prev) #for INT type
                elif 'date' in reveal_globals.global_attrib_types_dict[(c[0],c[1])]:
                    val = reveal_globals.global_d_plus_value[c[1]] #datetime.strptime(prev, '%y-%m-%d') #for DATE type
                else:
                    val=int(prev)
                if pred[3] == val: #sneha tab and moved to left
                    chk = 1
                    pos_e.append(c)
            if chk == 1:
                aeqa(pred[0], pred[1], pos_e)
    end_time = time.time()
    # print("Join with AoA time: ", end_time - start_time)
    print("Step1: ", time.time() - reveal_globals.local_start_time) #aman
    
    
    
    #step2
    reveal_globals.local_start_time = time.time() #aman
    for pred in C:
        print("Step 2")
        if pred[2] == '<=':
            chk1 = 0
            chk2 = 0
            pos_le = []
            pos_l = []
            try:
                for c in cols:
                    if c[1] == pred[1]:
                        continue
                    # SQL QUERY for checking value
                    cur = reveal_globals.global_conn.cursor()
                    cur.execute("SELECT " + c[1] + " FROM new_" + c[0] + " ;")
                    prev = cur.fetchone()
                    prev = prev[0]
                    cur.close()
                    print(reveal_globals.global_attrib_types_dict[(c[0],c[1])])
                    if 'int' in reveal_globals.global_attrib_types_dict[(c[0],c[1])]:
                        val = int(prev) #for INT type
                        if type(pred[3]) == type(val):
                            if pred[3] == val+1:
                                chk2 = 1
                                pos_l.append(c)
                            elif pred[3] == val:
                                # print("------------------------------")
                                chk1 = 1
                                pos_le.append(c)
                    elif 'numeric' in reveal_globals.global_attrib_types_dict[(c[0],c[1])]:#sneha
                        val = int(prev) #for INT type
                        if type(pred[3]) == type(val):
                            if int(pred[3]) == val+1:
                                chk2 = 1
                                pos_l.append(c)
                            elif int(pred[3]) == val:
                                # print("------------------------------")
                                chk1 = 1
                                pos_le.append(c)
                    elif 'date' in reveal_globals.global_attrib_types_dict[(c[0],c[1])]:
                        val = reveal_globals.global_d_plus_value[c[1]] #datetime.strptime(prev, '%y-%m-%d') #for DATE type
                        if type(pred[3]) == type(val):
                            if pred[3] == val+datetime.timedelta(days= 1):
                                chk2 = 1
                                pos_l.append(c)
                            elif pred[3] == val:
                                # print("------------------------------")
                                chk1 = 1
                                pos_le.append(c)
            except:
                print("nsjsfvkj")
            #snehajsjsd
            if chk1 == 1:
                print("pos_le", pos_le)
                isAoA = ainea(0, 0, pred[0], pred[1], pos_le, '<=')
            if chk2 == 1:
                isAoA = ainea(0, 0, pred[0], pred[1], pos_l, '<')

        elif pred[2] == '>=':
            chk3 = 0
            chk4 = 0
            pos_ge = []
            pos_g = []
            try :
                for c in cols:
                    if c[1] == pred[1]:
                        continue
                    # SQL QUERY for checking value
                    cur = reveal_globals.global_conn.cursor()
                    cur.execute("SELECT " + c[1] + " FROM new_" + c[0] + " ;")
                    print("SELECT " + c[1] + " FROM new_" + c[0] + " ;")
                    prev = cur.fetchone()
                    prev = prev[0]
                    
                    cur.close()
                    print(reveal_globals.global_attrib_types_dict[(c[0],c[1])])
                    if 'int' in reveal_globals.global_attrib_types_dict[(c[0],c[1])]:
                        val = int(prev) #for INT type
                        if type(pred[3]) == type(val):
                            if pred[3] == val-1:
                                chk4 = 1
                                pos_g.append(c)
                            elif pred[3] == val:
                                chk3 = 1
                                pos_ge.append(c)
                    elif 'date' in reveal_globals.global_attrib_types_dict[(c[0],c[1])]:
                        val = reveal_globals.global_d_plus_value[c[1]] #datetime.strptime(prev, '%y-%m-%d') #for DATE type
                        if type(pred[3]) == type(val):
                            if pred[3] == val-datetime.timedelta(days= 1):
                                chk4 = 1
                                pos_g.append(c)
                            elif pred[3] == val:
                                chk3 = 1
                                pos_ge.append(c)
                    elif 'numeric' in reveal_globals.global_attrib_types_dict[(c[0],c[1])]:
                        val = int(prev) #for INT type
                        if type(pred[3]) == type(val):
                            if int(pred[3]) == val-1:
                                chk4 = 1
                                pos_g.append(c)
                            elif int(pred[3]) == val:
                                chk3 = 1
                                pos_ge.append(c)
            except:
                print("djvhb")
            if chk3 == 1:
                print("pos_ge", pos_ge)
                isAoA = ainea(0, 0, pred[0], pred[1], pos_ge, '>=')
            if chk4 == 1:
                isAoA = ainea(0, 0, pred[0], pred[1], pos_g, '>')
    
    
    print("Step2: ", time.time() - reveal_globals.local_start_time) #aman
    
    #step3
    reveal_globals.local_start_time = time.time() #aman
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
        if 'int' in reveal_globals.global_attrib_types_dict[(toAtt[att][0],toAtt[att][1])]:
            val = int(prev)
            cur = reveal_globals.global_conn.cursor()
            cur.execute('Update ' + toAtt[att][0] + ' set ' + toAtt[att][1] + ' = ' + str(min_int_val+i) + ';') #for integer domain
            cur.close()
        if 'numeric' in reveal_globals.global_attrib_types_dict[(toAtt[att][0],toAtt[att][1])]:
            val = int(prev)
            cur = reveal_globals.global_conn.cursor()
            cur.execute('Update ' + toAtt[att][0] + ' set ' + toAtt[att][1] + ' = ' + str(min_int_val+i) + ';') #for integer domain
            cur.close()
        elif 'date' in reveal_globals.global_attrib_types_dict[(toAtt[att][0],toAtt[att][1])]:
            val = reveal_globals.global_d_plus_value[toAtt[att][1]]
            cur = reveal_globals.global_conn.cursor()
            cur.execute("Update " + toAtt[att][0] + " set " + toAtt[att][1] + " = '" + str(min_date_val+datetime.timedelta(days= i)) + "';") #for date domain
            cur.close()
        stack.insert(0,att)
        new_res = executable.getExecOutput()
        if len(new_res) < 2:
            if 'int' in reveal_globals.global_attrib_types_dict[(toAtt[att][0],toAtt[att][1])]:
                left = min_int_val+i
                right = val
                while int(right - left) > 0:#left < right:
                    mid = left + int(math.floor((right-left)/2))
                    cur = reveal_globals.global_conn.cursor()
                    cur.execute('Update ' + toAtt[att][0] + ' set ' + toAtt[att][1] + ' = ' + str(mid) + ';') #for integer domain
                    cur.close()
                    new_res = executable.getExecOutput()
                    if len(new_res) < 2:
                        left = mid+1
                    else:
                        right = mid
                    mid = int(math.ceil((left+right)/2))
                cur = reveal_globals.global_conn.cursor()
                cur.execute('Update ' + toAtt[att][0] + ' set ' + toAtt[att][1] + ' = ' + str(right) + ';') #for integer domain
                cur.close()
            elif 'numeric' in reveal_globals.global_attrib_types_dict[(toAtt[att][0],toAtt[att][1])]:#sneha
                left = min_int_val+i
                right = val
                while int(right - left) > 0:#left < right:
                    mid = left + int(math.floor((right-left)/2))
                    cur = reveal_globals.global_conn.cursor()
                    cur.execute('Update ' + toAtt[att][0] + ' set ' + toAtt[att][1] + ' = ' + str(mid) + ';') #for integer domain
                    cur.close()
                    new_res = executable.getExecOutput()
                    if len(new_res) < 2:
                        left = mid+1
                    else:
                        right = mid
                    mid = int(math.ceil((left+right)/2))
                cur = reveal_globals.global_conn.cursor()
                cur.execute('Update ' + toAtt[att][0] + ' set ' + toAtt[att][1] + ' = ' + str(right) + ';') #for integer domain
                cur.close()
            elif 'date' in reveal_globals.global_attrib_types_dict[(toAtt[att][0],toAtt[att][1])]:
                left = min_date_val + datetime.timedelta(days= i)
                right = val
                while int((right - left).days) > 0:#left < right:
                    # print(left, mid, right)
                    mid = left + datetime.timedelta(days= int(math.floor(((right - left).days)/2)))
                    cur = reveal_globals.global_conn.cursor()
                    cur.execute("Update " + toAtt[att][0] + " set " + toAtt[att][1] + " = '" + str(mid) + "';") #for date domain
                    cur.close()
                    new_res = executable.getExecOutput()
                    if len(new_res) < 2:
                        left = mid + datetime.timedelta(days= 1)
                    else:
                        right = mid
                cur = reveal_globals.global_conn.cursor()
                cur.execute("Update " + toAtt[att][0] + " set " + toAtt[att][1] + " = '" + str(right) + "';") #for date domain
                cur.close()
            chk3 = 0
            chk4 = 0
            isAoA3 = 0
            isAoA4 = 0
            pos_ge = []
            pos_g = []
            for c in cols:
                if c[1] == pred[1]:
                    continue
                # SQL QUERY for checking value
                cur = reveal_globals.global_conn.cursor()
                cur.execute("SELECT " + c[1] + " FROM new_" + c[0] + " ;")
                prev = cur.fetchone()
                prev = prev[0]
                cur.close()
                if 'int' in reveal_globals.global_attrib_types_dict[(c[0],c[1])]:
                    val = int(prev) #for INT type
                    if pred[3] == val-1:
                        chk2 = 1
                        pos_g.append(c)
                elif 'numeric' in reveal_globals.global_attrib_types_dict[(c[0],c[1])]:#sneha
                    val = int(prev) #for INT type
                    if pred[3] == val-1:
                        chk2 = 1
                        pos_g.append(c)
                elif 'date' in reveal_globals.global_attrib_types_dict[(c[0],c[1])]:
                    val = reveal_globals.global_d_plus_value[c[1]] #datetime.strptime(prev, '%y-%m-%d') #for DATE type
                    if pred[3] == val-datetime.timedelta(days= 1):
                        chk2 = 1
                        pos_g.append(c)
                if pred[3] == val:
                    chk3 = 1
                    pos_ge.append(c)

                #sneha- below 3 ifs moved to right by one tab
            if chk3 == 1:
                isAoA3 = ainea(1, i, pred[0], pred[1], pos_ge, '>=')
            if chk4 == 1:
                isAoA4 = ainea(1, i, pred[0], pred[1], pos_g, '>')
            if isAoA3 == 0 and isAoA4 == 0:
                if 'int' in reveal_globals.global_attrib_types_dict[(toAtt[att][0],toAtt[att][1])]:
                    reveal_globals.global_filter_aoa.append((toAtt[att][0],toAtt[att][1],'>=',right,max_int_val))
                elif 'date' in reveal_globals.global_attrib_types_dict[(toAtt[att][0],toAtt[att][1])]:
                    reveal_globals.global_filter_aoa.append((toAtt[att][0],toAtt[att][1],'>=',right,max_date_val))
        # i += 1
    
    
    
    #reverse topo order
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
        if 'int' in reveal_globals.global_attrib_types_dict[(toAtt[att][0],toAtt[att][1])]:
            val = int(prev)
            cur = reveal_globals.global_conn.cursor()
            cur.execute('Update ' + toAtt[att][0] + ' set ' + toAtt[att][1] + ' = ' + str(max_int_val-i) + ';') #for integer domain
            cur.close()
        elif 'date' in reveal_globals.global_attrib_types_dict[(toAtt[att][0],toAtt[att][1])]:
            val = reveal_globals.global_d_plus_value[toAtt[att][1]]
            cur = reveal_globals.global_conn.cursor()
            cur.execute("Update " + toAtt[att][0] + " set " + toAtt[att][1] + " = '" + str(max_date_val-datetime.timedelta(days= i)) + "';") #for date domain
            cur.close()
        new_res = executable.getExecOutput()
        if len(new_res) < 2:
            if 'int' in reveal_globals.global_attrib_types_dict[(toAtt[att][0],toAtt[att][1])]:
                left = val
                right = max_int_val-i
                while int((right - left)) > 0:#left < right:
                    mid = left + int(math.ceil((right-left)/2))
                    cur = reveal_globals.global_conn.cursor()
                    cur.execute('Update ' + toAtt[att][0] + ' set ' + toAtt[att][1] + ' = ' + str(mid) + ';') #for integer domain
                    cur.close()
                    new_res = executable.getExecOutput()
                    if len(new_res) < 2:
                        right = mid-1
                    else:
                        left = mid
                cur = reveal_globals.global_conn.cursor()
                cur.execute('Update ' + toAtt[att][0] + ' set ' + toAtt[att][1] + ' = ' + str(left) + ';') #for integer domain
                cur.close()
            elif 'date' in reveal_globals.global_attrib_types_dict[(toAtt[att][0],toAtt[att][1])]:
                left = val
                right = max_date_val - datetime.timedelta(days= i)
                while int((right - left).days) > 0:#left < right:
                    mid = left + datetime.timedelta(days= int(math.ceil(((right - left).days)/2)))
                    cur = reveal_globals.global_conn.cursor()
                    cur.execute("Update " + toAtt[att][0] + " set " + toAtt[att][1] + " = '" + str(mid) + "';") #for date domain
                    cur.close()
                    new_res = executable.getExecOutput()
                    if len(new_res) < 2:
                        right = mid - datetime.timedelta(days= 1)
                    else:
                        left = mid
                cur = reveal_globals.global_conn.cursor()
                cur.execute("Update " + toAtt[att][0] + " set " + toAtt[att][1] + " = '" + str(left) + "';") #for date domain
                cur.close()
            chk1 = 0
            chk2 = 0
            isAoA1 = 0
            isAoA2 = 0
            pos_le = []
            pos_l = []
            for c in cols:
                if c[1] == pred[1]:
                    continue
                # SQL QUERY for checking value
                cur = reveal_globals.global_conn.cursor()
                cur.execute("SELECT " + c[1] + " FROM new_" + c[0] + " ;")
                prev = cur.fetchone()
                prev = prev[0]
                cur.close()
                if 'int' in reveal_globals.global_attrib_types_dict[(c[0],c[1])]:
                    val = int(prev) #for INT type
                    if pred[3] == val+1:
                        chk2 = 1
                        pos_l.append(c)
                elif 'date' in reveal_globals.global_attrib_types_dict[(c[0],c[1])]:
                    val = reveal_globals.global_d_plus_value[c[1]] #datetime.strptime(prev, '%y-%m-%d') #for DATE type
                    if pred[3] == val+datetime.timedelta(days= 1):
                        chk2 = 1
                        pos_l.append(c)
                if pred[3] == val:
                    chk1 = 1
                    pos_le.append(c)
            if chk1 == 1:
                isAoA1 = ainea(1, i, pred[0], pred[1], pos_le, '<=')
            if chk2 == 1:
                isAoA2 = ainea(1, i, pred[0], pred[1], pos_l, '<')
            if isAoA1 == 0 and isAoA2 == 0:
                if 'int' in reveal_globals.global_attrib_types_dict[(toAtt[att][0],toAtt[att][1])]:
                    reveal_globals.global_filter_aoa.append((toAtt[att][0],toAtt[att][1],'<=',min_int_val,left))
                elif 'date' in reveal_globals.global_attrib_types_dict[(toAtt[att][0],toAtt[att][1])]:
                    reveal_globals.global_filter_aoa.append((toAtt[att][0],toAtt[att][1],'<=',min_date_val,left))
        #i += 1
    print("aman_aoa", reveal_globals.global_filter_aoa)
    print("aman_aeq", reveal_globals.global_filter_aeq)
    for tab in reveal_globals.global_core_relations:
        cur = reveal_globals.global_conn.cursor()
        cur.execute('Delete from ' + tab + ';')
        cur.execute('Insert into ' + tab + ' select * from new_' + tab + ';')
        cur.close()
    # reveal_globals.globalAoA = 0
    # reveal_globals.global_filter_predicates = where_clause.get_filter_predicates()
    print("Step3: ", time.time() - reveal_globals.local_start_time) #aman

    #sneha 
    for tab in reveal_globals.global_core_relations:
        cur = reveal_globals.global_conn.cursor()
        cur.execute('Drop table if exists new_' + tab + ';') 
        cur.close()
        
    return

#referenced only in extract_aoa
def aeqa(tab, col, pos): #A=A
    chk = 0
    cur = reveal_globals.global_conn.cursor()
    cur.execute("SELECT " + col + " FROM new_" + tab + " ;")
    prev = cur.fetchone()
    prev = prev[0]
    cur.close()
    if 'int' in reveal_globals.global_attrib_types_dict[(tab,col)]:
        val = int(prev) #for INT type
    elif 'date' in reveal_globals.global_attrib_types_dict[(tab,col)]:
        val = reveal_globals.global_d_plus_value[col] #datetime.strptime(prev, '%y-%m-%d') #for DATE type
    
    
    for c in pos:
        # SQL Query for inc c's val
        cur = reveal_globals.global_conn.cursor()
        cur.execute("delete from "+ c[0] + ";")
        cur.execute("Insert into " + c[0] + " select * from new_" + c[0] + ";")
        cur.execute("delete from "+ tab + ";")
        cur.execute("Insert into " + tab + " select * from new_" + tab + ";")
        cur.close()  
            
        if 'int' in reveal_globals.global_attrib_types_dict[(tab,col)]:
            cur = reveal_globals.global_conn.cursor()
            cur.execute("update " + tab + " set " + col + " = " + str(val+1) + " ;")
            cur.execute("update " + c[0] + " set " + c[1] + " = " + str(val+1) + " ;")
            cur.close()
        elif 'date' in reveal_globals.global_attrib_types_dict[(tab,col)]:
            cur = reveal_globals.global_conn.cursor()
            cur.execute("update " + tab + " set " + col + " = '" + str(val+datetime.timedelta(days= 1)) + "' ;")
            cur.execute("update " + c[0] + " set " + c[1] + " = '" + str(val+datetime.timedelta(days= 1)) + "' ;")
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
            reveal_globals.global_filter_aeq.append((tab, col, '=', c[0], c[1]))
            break
            #return 1
    if chk == 0:
        reveal_globals.global_filter_aeq.append((tab, col, '=', val, val))
    return #0

#referenced only on extract_aoa
def ainea(flag, ofst, tab, col, pos, op): #AoA, op={<,<=,>,>=}
    #check wether A=A is present in col or pos
    mark = 0
    for c in pos:
        cur = reveal_globals.global_conn.cursor()
        cur.execute("SELECT " + c[1] + " FROM new_" + c[0] + " ;")
        prev = cur.fetchone()
        prev = prev[0]
        cur.close()
        if 'int' in reveal_globals.global_attrib_types_dict[(c[0],c[1])]:
            val = int(prev) #for INT type
        elif 'date' in reveal_globals.global_attrib_types_dict[(c[0],c[1])]:
            val = reveal_globals.global_d_plus_value[c[1]] #datetime.strptime(prev, '%y-%m-%d') #for DATE type
        # SQL Query for inc c's val
        cur = reveal_globals.global_conn.cursor()
        cur.execute("delete from "+ c[0] + ";")
        cur.execute("Insert into " + c[0] + " select * from new_" + c[0] + ";")
        cur.close()
        if 'int' in reveal_globals.global_attrib_types_dict[(c[0],c[1])]:
            cur = reveal_globals.global_conn.cursor()
            cur.execute("update " + c[0] + " set " + c[1] + " = " + str(val+1) + " ;")
            cur.close()
        elif 'date' in reveal_globals.global_attrib_types_dict[(c[0],c[1])]:
            # print("value", val)
            # print(c[1], reveal_globals.global_d_plus_value[c[1]])
            # print("new value", val+datetime.timedelta(days= 1))
            cur = reveal_globals.global_conn.cursor()
            cur.execute("update " + c[0] + " set " + c[1] + " = '" + str(val+datetime.timedelta(days= 1)) + "' ;")
            cur.close()
        new_result = executable.getExecOutput()
        if len(new_result) > 1:
            new_filter = where_clause.get_filter_predicates()
        else:
            cur = reveal_globals.global_conn.cursor()
            cur.execute("delete from "+ c[0] + ";")
            cur.execute("Insert into " + c[0] + " select * from new_" + c[0] + ";")
            cur.close()
            if 'int' in reveal_globals.global_attrib_types_dict[(c[0],c[1])]:
                cur = reveal_globals.global_conn.cursor()
                cur.execute("update " + c[0] + " set " + c[1] + " = " + str(val-1) + " ;")
                cur.close()
            elif 'date' in reveal_globals.global_attrib_types_dict[(c[0],c[1])]:
                cur = reveal_globals.global_conn.cursor()
                cur.execute("update " + c[0] + " set " + c[1] + " = '" + str(val-datetime.timedelta(days= 1)) + "' ;")
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
            reveal_globals.global_filter_aoa.append((tab,col,op,c[0],c[1]))
            if flag == 1 and (op == '<=' or op == '<'):
                if 'int' in reveal_globals.global_attrib_types_dict[(c[0],c[1])]:
                    cur = reveal_globals.global_conn.cursor()
                    cur.execute('Update ' + c[0] + ' set ' + c[1] + ' = ' + str(max_int_val-ofst-mark) + ';') #for integer domain
                    cur.close()
                elif 'date' in reveal_globals.global_attrib_types_dict[(c[0],c[1])]:
                    xyz = ofst+mark
                    cur = reveal_globals.global_conn.cursor()
                    cur.execute("Update " + c[0] + " set " + c[1] + " = '" + str(max_date_val-datetime.timedelta(days= xyz)) + "';") #for date domain
                    cur.close()
                # run executable
                new_chk_res = executable.getExecOutput()
                if len(new_chk_res) < 2:
                    cur = reveal_globals.global_conn.cursor()
                    cur.execute('Update ' + c[0] + ' set ' + c[1] + ' = ' + str(val) + ';')
                    cur.close()
                    if 'date' in reveal_globals.global_attrib_types_dict[(tab,col)]: #for date datatypes
                        cur = reveal_globals.global_conn.cursor()
                        cur.execute("Update " + tab + " set " + col + " = '" + str(val-datetime.timedelta(days= 1)) + "';")
                        cur.close()
                    else: #for int datatype
                        cur = reveal_globals.global_conn.cursor()
                        cur.execute('Update ' + tab + ' set ' + col + ' = ' + str(val-1) + ';')
                        cur.close()
                else:
                    if 'int' in reveal_globals.global_attrib_types_dict[(tab,col)]:
                        cur = reveal_globals.global_conn.cursor()
                        cur.execute('Update ' + tab + ' set ' + col + ' = ' + str(max_int_val-ofst-mark-1) + ';') #for integer domain
                        cur.close()
                    elif 'date' in reveal_globals.global_attrib_types_dict[(tab,col)]:
                        xyz = ofst+mark+1
                        cur = reveal_globals.global_conn.cursor()
                        cur.execute("Update " + tab + " set " + col + " = '" + str(max_date_val-datetime.timedelta(days= xyz)) + "';") #for date domain
                        cur.close()
                new_chk_res = executable.getExecOutput()
                if len(new_chk_res) < 2:
                    if 'int' in reveal_globals.global_attrib_types_dict[(tab,col)]:
                        cur = reveal_globals.global_conn.cursor()
                        cur.execute('Update ' + tab + ' set ' + col + ' = ' + str(val-1) + ';') #for integer domain
                        cur.close()
                    elif 'date' in reveal_globals.global_attrib_types_dict[(tab,col)]:
                        cur = reveal_globals.global_conn.cursor()
                        cur.execute("Update " + tab + " set " + col + " = '" + str(val-datetime.timedelta(days= 1)) + "';") #for date domain
                        cur.close()
                mark += 2
            elif flag == 1 and (op == '>=' or op == '>'):
                if 'int' in reveal_globals.global_attrib_types_dict[(tab,col)]:
                    cur = reveal_globals.global_conn.cursor()
                    cur.execute('Update ' + c[0] + ' set ' + c[1] + ' = ' + str(min_int_val+ofst+mark) + ';') #for integer domain
                    cur.close()
                elif 'date' in reveal_globals.global_attrib_types_dict[(tab,col)]:
                    xyz = ofst+mark
                    cur = reveal_globals.global_conn.cursor()
                    cur.execute("Update " + c[0] + " set " + c[1] + " = '" + str(min_date_val+datetime.timedelta(days= xyz)) + "';") #for date domain
                    cur.close()
                # run executable
                new_chk_res = executable.getExecOutput()
                if len(new_chk_res) < 2:
                    cur = reveal_globals.global_conn.cursor()
                    cur.execute('Update ' + c[0] + ' set ' + c[1] + ' = ' + str(val) + ';')
                    cur.close()
                    if 'date' in reveal_globals.global_attrib_types_dict[(c[0],c[1])]: #for date datatypes
                        cur = reveal_globals.global_conn.cursor()
                        cur.execute("Update " + tab + " set " + col + " = '" + str(val+datetime.timedelta(days= 1)) + "';")
                        cur.close()
                    else: #for int datatype
                        cur = reveal_globals.global_conn.cursor()
                        cur.execute('Update ' + tab + ' set ' + col + ' = ' + str(val+1) + ';')
                        cur.close()
                else:
                    if 'int' in reveal_globals.global_attrib_types_dict[(tab,col)]:
                        cur = reveal_globals.global_conn.cursor()
                        cur.execute('Update ' + tab + ' set ' + col + ' = ' + str(max_int_val-ofst-mark+1) + ';') #for integer domain
                        cur.close()
                    elif 'date' in reveal_globals.global_attrib_types_dict[(c[0],c[1])]:
                        xyz = ofst+mark-1
                        cur = reveal_globals.global_conn.cursor()
                        cur.execute("Update " + tab + " set " + col + " = '" + str(max_date_val-datetime.timedelta(days= xyz)) + "';") #for date domain
                        cur.close()
                new_chk_res = executable.getExecOutput()
                if len(new_chk_res) < 2:
                    if 'int' in reveal_globals.global_attrib_types_dict[(tab,col)]:
                        cur = reveal_globals.global_conn.cursor()
                        cur.execute('Update ' + tab + ' set ' + col + ' = ' + str(val+1) + ';') #for integer domain
                        cur.close()
                    elif 'date' in reveal_globals.global_attrib_types_dict[(tab,col)]:
                        cur = reveal_globals.global_conn.cursor()
                        cur.execute("Update " + tab + " set " + col + " = '" + str(val+datetime.timedelta(days= 1)) + "';") #for date domain
                        cur.close()
                mark += 2
    if mark != 0:
        return mark 
    return 0
