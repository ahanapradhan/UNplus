import os

copy_min_time = 0
view_min_time = 0
cs_time = 0

sf = 1
database_in_use = 'tpch'
cs_status = ""

query1 = ""
output1 = ""
# error test
error = ""
# view_based_minimizer= False
# copy_based_minimizer= True

minimizer = "view_based"
correlated_sampling = "no"
mini_type = "kapil"
# default minimizer: copy-based


global_os_name = "linux"
global_db_engine_dropdown = ('Microsoft SQL Server', 'PostgreSQL')
global_db_instance_dropdown = (
'tpch1GB', 'tpch10GB', 'tpch100GB', 'tpch1TB', 'tpcds1GB', 'tpcds10GB', 'tpcds100GB', 'tpcds1TB')
global_qh = ""
global_qe_save_path = ""
global_qe_save_checkbox = ""
global_test_option = ''
db = ""

global_db_engine = ""
global_db_instance = ""
global_connection_string = ""
global_query_no = 0

global_input_type = ""
global_select_inp = ""
global_from_inp = ""
global_where_inp = ""
global_groupby_inp = ""
global_orderby_inp = ""
global_limit_inp = ""

global_select_op = ""
global_select_op_proc = ""
global_from_op = ""
global_where_op = ""
global_groupby_op = ""
global_orderby_op = ""
global_limit_op = ""

global_clauses_with_syntactic_changes = ""
global_number_of_query_invocations = ""
global_tot_ext_time = ""

copy_min_time = 0
view_min_time = 0
cs_time = 0
global_hashres_time = ""
global_projection_time = ""
global_min_time = ""
global_from_time = ""
global_aoa_time = ""
global_where_time = ""
global_filter_predicates_disj = ""
global_join_time = ""
global_in_extractor_time = ""
global_nep_time = ""
global_filter_time = ""
global_groupby_time = ""
global_orderby_time = ""
global_agg_time = ""
global_limit_time = ""
global_assemble_time = ""

global_conn = None
global_restore_flag = False

global_min_button = False
global_button_string = ""

global_attrib_type_dict = {}
global_min_instance_dict = {}
global_miniscule_instances_dict = {}

global_proc_prev_screen = ""
global_db_prev_screen = ""

global_no_execCall = 0

# global_support_files_path = "/services/app/support_files/"
global_reduced_data_path = "/Users/ahanapradhan/Desktop/Projects/UNMASQUE/reduced_data/"
global_output_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "./output/")
global_input_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "./input/")

global_sample_size_percent = 2
global_sample_threshold = 20000
global_max_sample_iter = 5

global_all_relations = []
global_pk_dict = {}
global_index_dict = {}

global_AoA = 0
global_proj = []

global_core_sizes = {}
global_core_relations = []
global_join_graph = []
global_filter_predicates = []
global_filter_aeq = []
global_filter_aoa = []
global_projected_attributes = []
global_groupby_attributes = []
global_aggregated_attributes = []
global_orderby_attributes = []
global_limit = 1000

global_key_lists = []
global_output_list = []
global_projection_names = []
global_groupby_flag = False
global_attrib_types = []
global_attrib_types_dict = {}
global_all_attribs = []
global_key_attributes = []
global_d_plus_value = {}
global_attrib_max_length = {}
global_result_dict = {}
global_other_info_dict = {}
local_other_info_dict = {}
global_extracted_info_dict = {}
local_instance_no = 1
global_instance_dict = {}
local_instance_list = []
global_attrib_dict = {}
global_join_instance_dict = {}
global_component_dict = {}
local_start_time = ''
local_end_time = ''

outer_join_flag = False
seed_sample_size_per = 1
