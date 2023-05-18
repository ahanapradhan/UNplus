import csv
import copy
import sys
sys.path.append('../')


def initialization(reveal_globals):
	print("inside -- initialization.initialization")
	#check for support files
	try:
		# 1 july
		# f1 = open(reveal_globals.global_support_files_path + "pkfkrelations.csv", 'rt')
		f1 = open("pkfkrelations.csv", 'rt')
		# f2 = open(reveal_globals.global_support_files_path + "create_indexes.sql", 'rt')
		f2 = open( "create_indexes.sql", 'rt')
	except IOError as error:
		reveal_globals.error='Unmasque Error: \n Support File Not Accessible. \n Postgres Error: \n ' + dict(error.args[0])['M']
		print('Support File Not Accessible. Error: ' + str(error))
		return [False]
	#GET PK_DICT and index_dict
	all_pkfk = []
	# with open(reveal_globals.global_support_files_path + 'pkfkrelations.csv', 'rt') as f:
	with open('pkfkrelations.csv', 'rt') as f:
		data = csv.reader(f)
		for row in data:
			all_pkfk.append(list(row))
		if len(all_pkfk) > 0:
			del all_pkfk[0] #it will delete the attribute name of the file
	temp = []
	#GET PK-FK COMPLETE GRAPHS IN FORM OF LISTS
	reveal_globals.global_key_lists = [[]] #it contains one element that is empty list
	for row in all_pkfk:
		if row[2] == 'y' or row[2] == 'Y': #this is used to check if the table contains one or more primary key
			if row[0] in temp:
				reveal_globals.global_pk_dict[row[0]] = reveal_globals.global_pk_dict[row[0]] + "," + str(row[1])
			else:
				reveal_globals.global_pk_dict[row[0]] = str(row[1])
			temp.append(row[0])
		found_flag = False
		for elt in reveal_globals.global_key_lists:
			if (row[0],row[1]) in elt or (row[4], row[5]) in elt:
				if (row[0],row[1]) not in elt and row[1] != '':
					elt.append((row[0], row[1]))
				if (row[4],row[5]) not in elt and row[4] != '':
					elt.append((row[4], row[5]))
				found_flag = True
				break
		if found_flag == True:
			continue
		if row[0] != '' and row[4] != '':
			reveal_globals.global_key_lists.append([(row[0], row[1]), (row[4], row[5])])
		elif row[0] != '':
			reveal_globals.global_key_lists.append([(row[0], row[1])])
	#refinement
	for elt in reveal_globals.global_key_lists:
		remove_list = []
		for val in elt:
			if val[0] not in reveal_globals.global_core_relations:
				remove_list.append(val)
		for val in remove_list:
			elt.remove(val)
	while [] in reveal_globals.global_key_lists:
		reveal_globals.global_key_lists.remove([])
	remove_list = []
	for elt in reveal_globals.global_key_lists:
		if len(elt) <=1:
			remove_list.append(elt)
	for elt in remove_list:
		reveal_globals.global_key_lists.remove(elt)
	#GET INDEXES
	# f = open(reveal_globals.global_support_files_path + 'create_indexes.sql', 'rt')
	f = open('create_indexes.sql', 'rt')
	for elt in reveal_globals.global_core_relations:
		reveal_globals.global_index_dict[elt] = []
	for row in f:
		for elt in reveal_globals.global_core_relations:
			if str(row).find(str(elt.upper() + "_")) >= 0:
				reveal_globals.global_index_dict[elt].append(str(row.split()[4]))
	f1.close()
	f2.close()
	return reveal_globals