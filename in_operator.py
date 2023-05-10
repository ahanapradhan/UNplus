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

def extract(level,att,new_list):
    # temp_list = []
    
    attrib_types_dict = {}
    for entry in reveal_globals.global_attrib_types:
        attrib_types_dict[(entry[0], entry[1])] = entry[2]
        
    local_start=time.time()
    
    for tabname in reveal_globals.global_core_relations:
        cur=reveal_globals.global_conn.cursor()
        query1='truncate table '+ tabname+' ;'
        cur.execute(query1)
        condition = "true "
        
        if tabname in filter_table_list.keys():
            condition="true "
            for filters in filter_table_list[tabname]:
                if filters==att:
                    if 'date' in attrib_types_dict[(filters[0], filters[1])]:
                        condition=condition+"and ("+filters[1]+" < date \'"+str(filters[3])+"\' or "+filters[1]+ " > date\'"+str(filters[4])+"\') "
                    elif ('int' in attrib_types_dict[(filters[0], filters[1])] or 'numeric' in attrib_types_dict[(filters[0],filters[1])]):
                        condition=condition+"and ("+filters[1]+" < "+str(filters[3])+" or "+filters[1]+ " > "+str(filters[4])+") "            
                    else:
                        condition=condition+"and ("+filters[1]+" < '"+str(filters[3])+"' or "+filters[1]+ " > '"+str(filters[4])+"') "
                    continue
                else:
                    if ('int' in attrib_types_dict[(filters[0], filters[1])] or 'numeric' in attrib_types_dict[(filters[0],filters[1])]):    
                        condition=condition+"and ("+filters[1]+" >= "+str(filters[3])+" and "+filters[1]+ " <="+str(filters[4])+") "
                    elif 'date' in attrib_types_dict[(filters[0], filters[1])] :
                        condition=condition+"and ("+filters[1]+" >= date '"+str(filters[3])+"' and "+filters[1]+ " <= date '"+str(filters[4])+"') "
                    else:
                        condition=condition+"and ("+filters[1]+" >= '"+str(filters[3])+"' and "+filters[1]+ " <= '"+str(filters[4])+" ') "
            for filters in new_list:
                if filters[0] == tabname:
                    if 'date' in attrib_types_dict[(filters[0], filters[1])]:
                        condition=condition+"and ("+filters[1]+" < date \'"+str(filters[3])+"\' or "+filters[1]+ " > date\'"+str(filters[4])+"\') "
                    elif ('int' in attrib_types_dict[(filters[0], filters[1])] or 'numeric' in attrib_types_dict[(filters[0],filters[1])]):
                        condition=condition+"and ("+filters[1]+" < "+str(filters[3])+" or "+filters[1]+ " > "+str(filters[4])+") "            
                    else:
                        condition=condition+"and ("+filters[1]+" < '"+str(filters[3])+"' or "+filters[1]+ " > '"+str(filters[4])+"') "

        

        query2="Insert into " + tabname + " (select * from " + tabname + "_restore where "+condition+");"
        temp_query_time=time.time()
        print("runnning query :"+query2)                   
        cur.execute(query2)
        cur.close()
        
        cur = reveal_globals.global_conn.cursor()
        cur.execute("Select count(*) from " + tabname + ";")
        res = cur.fetchall()
        print(res)
        cur.close()
        print("temp query time: " + str(time.time()-temp_query_time))

    res=executable.getExecOutput()

    if len(res) > 1:
        print("possible IN at:" + att[0]+','+att[1])
        # for tabname in reveal_globals.global_core_relations:
        #     if level == 1:
        #         pass
        #         cur.execute("alter table "+tabname+"1 rename to "+tabname+"100 ;")
        #     else:
        #         cur.execute("drop table "+tabname+"1 ;")
            
        temp_min_start=time.time()
        if (db_minimizer.reduce_Database_Instance(reveal_globals.global_core_relations)):
        # if (reduce_Database_Instance(reveal_globals.global_core_relations)):
            temp=" "
            print("temp min time:"+str(time.time()-temp_min_start))
            reveal_globals.global_filter_predicates = where_clause.get_filter_predicates()
            print(reveal_globals.global_filter_predicates)
            fp = reveal_globals.global_filter_predicates
            new_temp=[]
    
            for e2 in fp:
                include_flag = True
                for ele in (reveal_globals.global_filter_aeq ):
                    if ele[1] == e2[1] and e2[2] == '=':
                        include_flag = False
                if include_flag:
                    new_temp.append(e2)
            
            reveal_globals.global_filter_predicates = new_temp
                    
            for item in reveal_globals.global_filter_predicates:
                if item not in potential_in_attrib:
                    temp=item
            
            
            if temp != " ":
                new_list.append(temp)
                temp_list.append(temp)
                if temp[0] not in  filter_table_list.keys():
                    filter_table_list[temp[0]]=[temp]
                # else:
                #     filter_table_list[temp[0]].append(temp)
                print("new condition: "+str(temp))
                
                cur = reveal_globals.global_conn.cursor()
                query = "select data_type from information_schema.columns where table_name ='"+temp[0]+"' and column_name = '" + temp[1] + "' order by ordinal_position; "
                cur.execute(query)
                res=cur.fetchall()
                for row in res:
                    attrib_types_dict[(temp[0],temp[1])]=row[0]
                print("local disjunction time: "+str(time.time()-local_start))
                cur.close()
                extract(level+1,att,new_list)
         
        else:
            print('error with minimizer')
            cur.close()
    else:
        print('No disjunction at :  '+ att[0]+','+att[1])
        # if level >1:
        #     for tabname in reveal_globals.global_core_relations:
        #         cur.execute("drop table "+tabname+"1 ;")
        #         cur.execute("alter table "+tabname+"100 rename to "+tabname+ "1 ;" )
        # cur.close()       

def in_extract(level=1):
    global_start=time.time()
    print(reveal_globals.global_filter_predicates)
    #load all data for level1
    count_in=0
    cur = reveal_globals.global_conn.cursor()
    for tabname in reveal_globals.global_core_relations:
        cur.execute("Alter table " + tabname + "4 rename to " + tabname + "_in_temp;")
    cur.close()
    
    for att in reveal_globals.global_filter_predicates:
        try:
            filter_table_list[att[0]].append(att)
        except:
            filter_table_list[att[0]]=[att]
        # if att[1] not in reveal_globals.global_key_attributes:
        potential_in_attrib.append(att)

    for att in potential_in_attrib:
        temp_list.clear()
        temp_list.append(att)
        cur = reveal_globals.global_conn.cursor()
        extract(level,att,[])
        new_filter_list.append(copy.deepcopy(temp_list))
    print('Disjunction Time: '+str(time.time() -global_start))
    
    reveal_globals.global_filter_predicates_disj = copy.deepcopy(new_filter_list)
    cur = reveal_globals.global_conn.cursor()
    for tabname in reveal_globals.global_core_relations:
        cur.execute('drop table if exists ' + tabname + '4;')
        cur.execute("Alter table " + tabname + "_in_temp rename to " + tabname + "4;")
    cur.close()
        
   