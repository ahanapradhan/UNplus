# import check_nullfree
import os
import copy
import executable
import reveal_globals
import time
import pandas as pd


def getCoreSizes(core_relations):
	core_sizes = {}
	for tabname in core_relations:
		try:
			cur = reveal_globals.global_conn.cursor()
			cur.execute('select count(*) from ' + tabname + ';')
			res = cur.fetchone()
			cur.close()
			core_sizes[tabname] = int(str(res[0]))
		except Exception as error:
			print("Error in getting table Sizes. Error: " + str(error))
	return core_sizes


def reduce_Database_Instance(core_relations, method = 'binary partition', max_no_of_rows = 1, executable_path = ""):
    if reveal_globals.cs_status == "PASS" :
        return reduce_Database_Instance_cs_pass(reveal_globals.global_core_relations)
    else:
        return reduce_Database_Instance_cs_fail(reveal_globals.global_core_relations)
        
        
        


def reduce_Database_Instance_cs_pass(core_relations, method = 'binary partition', max_no_of_rows = 1, executable_path = ""):
	reveal_globals.local_other_info_dict = {}
	core_sizes = getCoreSizes(core_relations)
	start_time=time.time()
	# print("YES1")
	'''
	cur = reveal_globals.global_conn.cursor()
	cur.execute("set synchronize_seqscans = 'OFF';")
	cur.close()
	'''
	for tabname in reveal_globals.global_core_relations:
     
		cur = reveal_globals.global_conn.cursor()
		cur.execute('alter table ' + tabname + ' rename to '+tabname+'1 ;')
		# cur.execute('create view ' + tabname + ' as select * from '+ tabname +'1;')
		cur.close()

		cur = reveal_globals.global_conn.cursor()
		cur.execute('select min(ctid), max(ctid) from '+ tabname+'1; ')
		rctid =cur.fetchone()
		cur.close()
		min_ctid = rctid[0]
		# print(min_ctid)
		min_ctid2 = min_ctid.split(",")
		start_page = int(min_ctid2[0][1:])
		max_ctid = rctid[1]
		print(max_ctid)
		max_ctid2 = max_ctid.split(",")
		end_page = int(max_ctid2[0][1:])
  
		# cur = reveal_globals.global_conn.cursor()
		# cur.execute('select max(ctid) from '+ tabname+'1; ')
		# max_ctid =cur.fetchone()
		# cur.close()
		# max_ctid = max_ctid[0]
		# print(max_ctid)
		# max_ctid2 = max_ctid.split(",")
		# end_page = int(max_ctid2[0][1:])

  
		start_ctid = min_ctid
		end_ctid = max_ctid
		# print("start_page= ",start_page, "end page= ",end_page )
		while (start_page < end_page-1):
			mid_page=int((start_page + end_page)/2)
			mid_ctid1 = "(" + str(mid_page) + ",1)"
			mid_ctid2 = "(" + str(mid_page) + ",2)"

			
			cur = reveal_globals.global_conn.cursor()
			# cur.execute('drop view '+ tabname + ';')
			cur.execute("create view " + tabname + " as select * from "+ tabname +"1 where ctid >= '" + str(start_ctid) + "' and ctid <= '" + str(mid_ctid1) + "'  ; ")
			cur.close()

			#Run query and analyze the result now
			
			# reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1
			# cur = reveal_globals.global_conn.cursor()
			# cur.execute('drop view '+ tabname + ';')
			# cur.close()
   
			new_result = executable.getExecOutput()
			if len(new_result) <= 1:
				#Take the lower half
				start_ctid = mid_ctid2
			else:
				#Take the upper half
				end_ctid = mid_ctid1
			# start_page=start_ctid[0]
   
			#UN+nf
			# if check_nullfree.getExecOutput() == False:
			# 	#Take the lower half
			# 	start_ctid = mid_ctid2
			# else:
			# 	#Take the upper half
			# 	end_ctid = mid_ctid1
    
			cur = reveal_globals.global_conn.cursor()
			cur.execute('drop view '+ tabname + ';')
			cur.close()

			start_ctid2 = start_ctid.split(",")
			start_page = int(start_ctid2[0][1:])
			end_ctid2 = end_ctid.split(",")
			end_page = int(end_ctid2[0][1:])
			# print("start_page= ",start_page, "end page= ",end_page )
			# print(start_ctid, end_ctid)
		cur = reveal_globals.global_conn.cursor()
  
		# cur.execute('drop view '+ tabname + ';')
		print("create table " + tabname + " as select * from "+ tabname +"1 where ctid >= '" + str(start_ctid) + "' and ctid <= '" + str(end_ctid) + "'  ; ")
		cur.execute("create table " + tabname + " as select * from "+ tabname +"1 where ctid >= '" + str(start_ctid) + "' and ctid <= '" + str(end_ctid) + "'  ; ")
		cur.execute('drop table ' + tabname + '1 ;')
		cur.close()
		cur = reveal_globals.global_conn.cursor()
		cur.execute("select count(*) from " + tabname + ";")
		size = int(cur.fetchone()[0])
		cur.close()
		core_sizes[tabname] = size
		print("REMAINING TABLE SIZE", core_sizes[tabname])

		print(start_ctid, end_ctid)

  
		
		while(int(core_sizes[tabname]) > max_no_of_rows):
			cur = reveal_globals.global_conn.cursor()
			cur.execute('alter table ' + tabname + ' rename to '+tabname+'1 ;')
			# cur.execute('create view ' + tabname + ' as select * from '+ tabname +'1;')
			cur.close()
			
			cur = reveal_globals.global_conn.cursor()
			cur.execute('select min(ctid) from '+ tabname+'1; ')
			min_ctid =cur.fetchone()
			cur.close()
			min_ctid = min_ctid[0]
			min_ctid= min_ctid[1:-1]
			min_ctid2 = min_ctid.split(",")
			print(min_ctid2)
			start_row = int(min_ctid2[1])
			start_page = int(min_ctid2[0])
	
			cur = reveal_globals.global_conn.cursor()
			cur.execute('select max(ctid) from '+ tabname+'1; ')
			max_ctid =cur.fetchone()
			cur.close()
			max_ctid = max_ctid[0]
			max_ctid =max_ctid[1:-1]
			max_ctid2 = max_ctid.split(",")
			print(max_ctid2)
			end_row = int(max_ctid2[1])
			end_page = int(max_ctid2[0])
			print("start_row= ",start_row, "end_row = ",end_row ,"#########")
   
			# mid_page=int((start_page + end_page)/2)
			start_ctid = "(" + str(start_page) + "," + str(start_row) + ")"
			end_ctid = "(" + str(end_page) + "," + str(end_row) + ")"

			# if start_page!= end_page:
			# 	#wish to know last row in start page
			# 	mid_ctid1="(" + str(end_page) + "," + str(0) + ")"
			# 	mid_ctid2="(" + str(0) + "," + str(mid_row+1) + ")"
			mid_row=int( core_sizes[tabname] /2)
   
			mid_ctid1="(" + str(0) + "," + str(mid_row) + ")"
			mid_ctid2="(" + str(0) + "," + str(mid_row+1) + ")"

			
			cur = reveal_globals.global_conn.cursor()
			# cur.execute('drop view '+ tabname + ';')
			cur.execute("create view " + tabname + " as select * from "+ tabname +"1 where ctid >= '" + str(start_ctid) + "' and ctid <= '" + str(mid_ctid1) + "'  ; ")
			cur.close()

			#Run query and analyze the result now
			
			# reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1
			# cur = reveal_globals.global_conn.cursor()
			# cur.execute('drop view '+ tabname + ';')
			# cur.close()
   
			new_result = executable.getExecOutput()
			if len(new_result) <= 1:
				#Take the lower half
				start_ctid = mid_ctid2
			else:
				#Take the upper half
				end_ctid = mid_ctid1
			# start_page=start_ctid[0]
   
			#UN+nf
			# if check_nullfree.getExecOutput() == False:
			# 	#Take the lower half
			# 	start_ctid = mid_ctid2
			# else:
			# 	#Take the upper half
			# 	end_ctid = mid_ctid1
    
			cur = reveal_globals.global_conn.cursor()
			cur.execute('drop view '+ tabname + ';')
			cur.close()
   
   
   
			cur = reveal_globals.global_conn.cursor()
			cur.execute("create table " + tabname + " as select * from "+ tabname +"1 where ctid >= '" + str(start_ctid) + "' and ctid <= '" + str(end_ctid) + "'  ; ")
			cur.execute('drop table ' + tabname + '1 ;')
			cur.close()
			cur = reveal_globals.global_conn.cursor()
			cur.execute("select count(*) from " + tabname + ";")
			size = int(cur.fetchone()[0])
			cur.close()
			core_sizes[tabname] = size
			print("REMAINING TABLE SIZE", core_sizes[tabname])

		#SANITY CHECK
		new_result = executable.getExecOutput()
		if len(new_result) <= 1:
			print("Error: Query out of extractable domain\n")
			return False

		#UN+nf
		# if check_nullfree.getExecOutput() == False:
		# 	print("Error: Query out of extractable domain\n")
		# 	return False


	#WRITE TO Reduced Data Directory
	#check for data directory existence, if not exists , create it
	if not os.path.exists(reveal_globals.global_reduced_data_path):
		os.makedirs(reveal_globals.global_reduced_data_path)
	for tabname in core_relations:
		cur = reveal_globals.global_conn.cursor()
		cur.execute("select * from " + tabname + ";")
		res=cur.fetchall()
		# cur.execute(" COPY " + tabname + " to " + "'" + reveal_globals.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
		cur.close()
		cur = reveal_globals.global_conn.cursor()
        # cur.execute('drop table'+ tabname + "4 )
		cur.execute("create table " + tabname + "4 as select * from " + tabname + ";")
		cur.close()
		print(tabname, "==", res)
	#SANITY CHECK
	new_result = executable.getExecOutput()
	if len(new_result) <= 1:
		print("Error: Query out of extractable domain\n")
		return False
	# if check_nullfree.getExecOutput() == False:
	# 	print("Error: Query out of extractable domain\n")
	# 	return False
	
	#populate screen data
	#POPULATE MIN INSTANCE DICT
	reveal_globals.view_min_time=time.time()-start_time
	conn=reveal_globals.global_conn
	for tabname in reveal_globals.global_core_relations:
		reveal_globals.global_min_instance_dict[tabname] = []
		sql_query = pd.read_sql_query("select * from "+tabname+";",conn)
		df = pd.DataFrame(sql_query)
		reveal_globals.global_min_instance_dict[tabname].append(tuple(df.columns))
		for index, row in df.iterrows():
			reveal_globals.global_min_instance_dict[tabname].append(tuple(row))
	#populate other data
	new_result=executable.getExecOutput()
	reveal_globals.global_result_dict['min'] = copy.deepcopy(new_result)
	reveal_globals.local_other_info_dict['Result Cardinality'] = str(len(new_result) - 1)
	reveal_globals.global_other_info_dict['min'] = copy.deepcopy(reveal_globals.local_other_info_dict)
	return True	


def reduce_Database_Instance_cs_fail(core_relations, method = 'binary partition', max_no_of_rows = 1, executable_path = ""):
	reveal_globals.local_other_info_dict = {}
	#Perform sampling
	#print(core_relations, reveal_globals.global_sample_size_percent, reveal_globals.global_sample_threshold, reveal_globals.global_max_sample_iter,"++++SAMPLING++++++++")
	# core_sizes = sample_Database_Instance(core_relations, reveal_globals.global_sample_size_percent, reveal_globals.global_sample_threshold, reveal_globals.global_max_sample_iter)
	# print("sneha here")
	# exit(0)
	
	# core_sizes = getCoreSizes(core_relations)
	core_sizes =  reveal_globals.global_core_sizes
	start_time=time.time()
	# print("YES1")

		
  
	print("xhcbhcb")
	'''
	cur = reveal_globals.global_conn.cursor()
	cur.execute("set synchronize_seqscans = 'OFF';")
	cur.close()
	'''
	for tabname in reveal_globals.global_core_relations:
     
		cur = reveal_globals.global_conn.cursor()
		cur.execute('alter table ' + tabname + ' rename to '+tabname+'_restore ;')
		# cur.execute('create view ' + tabname + ' as select * from '+ tabname +'1;')
		cur.close()

		cur = reveal_globals.global_conn.cursor()
		cur.execute('select min(ctid), max(ctid) from '+ tabname+'_restore; ')
		rctid =cur.fetchone()
		cur.close()
		min_ctid = rctid[0]
		# print(min_ctid)
		min_ctid2 = min_ctid.split(",")
		start_page = int(min_ctid2[0][1:])
		max_ctid = rctid[1]
		print(max_ctid)
		max_ctid2 = max_ctid.split(",")
		end_page = int(max_ctid2[0][1:])
  
		# cur = reveal_globals.global_conn.cursor()
		# cur.execute('select max(ctid) from '+ tabname+'1; ')
		# max_ctid =cur.fetchone()
		# cur.close()
		# max_ctid = max_ctid[0]
		# print(max_ctid)
		# max_ctid2 = max_ctid.split(",")
		# end_page = int(max_ctid2[0][1:])

  
		start_ctid = min_ctid
		end_ctid = max_ctid
		print("start_page= ",start_page, "end page= ",end_page )
		while (start_page < end_page-1):
			mid_page=int((start_page + end_page)/2)
			mid_ctid1 = "(" + str(mid_page) + ",1)"
			mid_ctid2 = "(" + str(mid_page) + ",2)"

			
			cur = reveal_globals.global_conn.cursor()
			# cur.execute('drop view '+ tabname + ';')
			cur.execute("create view " + tabname + " as select * from "+ tabname +"_restore where ctid >= '" + str(start_ctid) + "' and ctid <= '" + str(mid_ctid1) + "'  ; ")
			cur.close()

			#Run query and analyze the result now
			
			# reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1
			# cur = reveal_globals.global_conn.cursor()
			# cur.execute('drop view '+ tabname + ';')
			# cur.close()
   
			# new_result = executable.getExecOutput()
			new_result_flag = getExecOutput()

			# if len(new_result) <= 1:
			if new_result_flag == False: 
				#Take the lower half
				start_ctid = mid_ctid2
			else:
				#Take the upper half
				end_ctid = mid_ctid1
			# start_page=start_ctid[0]
   
			#UN+nf
			# if check_nullfree.getExecOutput() == False:
			# 	#Take the lower half
			# 	start_ctid = mid_ctid2
			# else:
			# 	#Take the upper half
			# 	end_ctid = mid_ctid1
    
			cur = reveal_globals.global_conn.cursor()
			cur.execute('drop view '+ tabname + ';')
			cur.close()

			start_ctid2 = start_ctid.split(",")
			start_page = int(start_ctid2[0][1:])
			end_ctid2 = end_ctid.split(",")
			end_page = int(end_ctid2[0][1:])
			print("start_page= ",start_page, "end page= ",end_page )
			print(start_ctid, end_ctid)
		cur = reveal_globals.global_conn.cursor()
  
		# cur.execute('drop view '+ tabname + ';')
		print("create table " + tabname + " as select * from "+ tabname +"_restore where ctid >= '" + str(start_ctid) + "' and ctid <= '" + str(end_ctid) + "'  ; ")
		cur.execute("create table " + tabname + " as select * from "+ tabname +"_restore where ctid >= '" + str(start_ctid) + "' and ctid <= '" + str(end_ctid) + "'  ; ")
		# cur.execute('drop table ' + tabname + '1 ;')
		cur.close()
		cur = reveal_globals.global_conn.cursor()
		cur.execute("select count(*) from " + tabname + ";")
		size = int(cur.fetchone()[0])
		cur.close()
		core_sizes[tabname] = size
		print("REMAINING TABLE SIZE", core_sizes[tabname])

		print(start_ctid, end_ctid)

  
		
		while(int(core_sizes[tabname]) > max_no_of_rows):
			cur = reveal_globals.global_conn.cursor()
			cur.execute('alter table ' + tabname + ' rename to '+tabname+'1 ;')
			# cur.execute('create view ' + tabname + ' as select * from '+ tabname +'1;')
			cur.close()
			
			cur = reveal_globals.global_conn.cursor()
			cur.execute('select min(ctid) from '+ tabname+'1; ')
			min_ctid =cur.fetchone()
			cur.close()
			min_ctid = min_ctid[0]
			min_ctid= min_ctid[1:-1]
			min_ctid2 = min_ctid.split(",")
			print(min_ctid2)
			start_row = int(min_ctid2[1])
			start_page = int(min_ctid2[0])
	
			cur = reveal_globals.global_conn.cursor()
			cur.execute('select max(ctid) from '+ tabname+'1; ')
			max_ctid =cur.fetchone()
			cur.close()
			max_ctid = max_ctid[0]
			max_ctid =max_ctid[1:-1]
			max_ctid2 = max_ctid.split(",")
			print(max_ctid2)
			end_row = int(max_ctid2[1])
			end_page = int(max_ctid2[0])
			print("start_row= ",start_row, "end_row = ",end_row ,"#########")
   
			# mid_page=int((start_page + end_page)/2)
			start_ctid = "(" + str(start_page) + "," + str(start_row) + ")"
			end_ctid = "(" + str(end_page) + "," + str(end_row) + ")"

			# if start_page!= end_page:
			# 	#wish to know last row in start page
			# 	mid_ctid1="(" + str(end_page) + "," + str(0) + ")"
			# 	mid_ctid2="(" + str(0) + "," + str(mid_row+1) + ")"
			mid_row=int( core_sizes[tabname] /2)
   
			mid_ctid1="(" + str(0) + "," + str(mid_row) + ")"
			mid_ctid2="(" + str(0) + "," + str(mid_row+1) + ")"

			
			cur = reveal_globals.global_conn.cursor()
			# cur.execute('drop view '+ tabname + ';')
			cur.execute("create view " + tabname + " as select * from "+ tabname +"1 where ctid >= '" + str(start_ctid) + "' and ctid <= '" + str(mid_ctid1) + "'  ; ")
			cur.close()

			#Run query and analyze the result now
			
			# reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1
			# cur = reveal_globals.global_conn.cursor()
			# cur.execute('drop view '+ tabname + ';')
			# cur.close()
   
			# new_result = executable.getExecOutput()
			new_result_flag = getExecOutput()

			# if len(new_result) <= 1:
			if new_result_flag == False: 
				#Take the lower half
				start_ctid = mid_ctid2
			else:
				#Take the upper half
				end_ctid = mid_ctid1
			# start_page=start_ctid[0]
   
			#UN+nf
			# if check_nullfree.getExecOutput() == False:
			# 	#Take the lower half
			# 	start_ctid = mid_ctid2
			# else:
			# 	#Take the upper half
			# 	end_ctid = mid_ctid1
    
			cur = reveal_globals.global_conn.cursor()
			cur.execute('drop view '+ tabname + ';')
			cur.close()
   
   
   
			cur = reveal_globals.global_conn.cursor()
			cur.execute("create table " + tabname + " as select * from "+ tabname +"1 where ctid >= '" + str(start_ctid) + "' and ctid <= '" + str(end_ctid) + "'  ; ")
			cur.execute('drop table ' + tabname + '1 ;')
			cur.close()
			cur = reveal_globals.global_conn.cursor()
			cur.execute("select count(*) from " + tabname + ";")
			size = int(cur.fetchone()[0])
			cur.close()
			core_sizes[tabname] = size
			print("REMAINING TABLE SIZE", core_sizes[tabname])

		#SANITY CHECK
		# new_result = executable.getExecOutput()
		new_result_flag = getExecOutput()

		# if len(new_result) <= 1:
		if new_result_flag == False: 
			print("Error: Query out of extractable domain\n")
			return False



	#WRITE TO Reduced Data Directory
	#check for data directory existence, if not exists , create it
	if not os.path.exists(reveal_globals.global_reduced_data_path):
		os.makedirs(reveal_globals.global_reduced_data_path)
	for tabname in core_relations:
		cur = reveal_globals.global_conn.cursor()
		cur.execute("select * from " + tabname + ";")
		res=cur.fetchall()
		# cur.execute(" COPY " + tabname + " to " + "'" + reveal_globals.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
		cur.close()
		cur = reveal_globals.global_conn.cursor()
        # cur.execute('drop table'+ tabname + "4 )
		cur.execute("create table " + tabname + "4 as select * from " + tabname + ";")
		cur.close()
		print(tabname, "==", res)
	#SANITY CHECK 
	new_result = executable.getExecOutput()
	if len(new_result) <= 1:
		print("Error: Query out of extractable domain\n")
		return False
	# if check_nullfree.getExecOutput() == False:
	# 	print("Error: Query out of extractable domain\n")
	# 	return False
	
	#populate screen data
	#POPULATE MIN INSTANCE DICT
	reveal_globals.view_min_time=time.time()-start_time
	conn=reveal_globals.global_conn
	for tabname in reveal_globals.global_core_relations:
		reveal_globals.global_min_instance_dict[tabname] = []
		sql_query = pd.read_sql_query("select * from "+tabname+";",conn)
		df = pd.DataFrame(sql_query)
		reveal_globals.global_min_instance_dict[tabname].append(tuple(df.columns))
		for index, row in df.iterrows():
			reveal_globals.global_min_instance_dict[tabname].append(tuple(row))
	#populate other data
	new_result=executable.getExecOutput()
	reveal_globals.global_result_dict['min'] = copy.deepcopy(new_result)
	reveal_globals.local_other_info_dict['Result Cardinality'] = str(len(new_result) - 1)
	reveal_globals.global_other_info_dict['min'] = copy.deepcopy(reveal_globals.local_other_info_dict)
	return True	


def getExecOutput():
    # print("inside :-- executable.getExecOutput")
    # result = []
    try:
        cur = reveal_globals.global_conn.cursor()
        query=reveal_globals.query1
        #print("query=",query)
        reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1
        cur.execute(query)
        res = cur.fetchall() #fetchone always return a tuple whereas fetchall return list
        #print(res)
        # colnames = [desc[0] for desc in cur.description] 
        cur.close()
        # result.append(tuple(colnames))
        #print(result) it will print 8 times for from clause 
            #it will append attribute name in the result
        if res is not None:
            for row in res:
                #CHECK IF THE WHOLE ROW IN NONE (SPJA Case)
                nullrow = True
                for val in row:
                    if val != None:
                        nullrow = False
                        break
                if nullrow == True:
                    continue
                else:
                    return True
                # original
                # temp = []
                # for val in row:
                #     temp.append(str(val))
                # result.append(tuple(temp))

                # below chnages for cs2_impr


    except Exception as error:
        # reveal_globals.error='Unmasque Error: \n Executable could not be run. Error: ' +  dict(error.args[0])['M']
        print('Executable could not be run. Error: ' + str(error))
        raise error
    return False