import MySQLdb
from field_mapper import 	mysql_converter, \
							datetime

DB_SETTINGS = { "name":"oneems",
				"host":"10.134.179.99",
				"user":"keshav",
				"passwd": "keshav"}

schema_map = 	{ 
					'oneems' : 
					{
						'discoveryres': 	{
									'id': 'INT NOT NULL AUTO_INCREMENT PRIMARY KEY',
									'deviceIpAddr': 'VARCHAR(60)',
									'devicename': 'TEXT',
									'model': 'TEXT',
									'deviceseries': 'TEXT',
									'deviceos': 'VARCHAR(255)',
									'nodeVersion': 'VARCHAR(10)',
									'region': 'VARCHAR(255)',
									'market': 'TEXT',
									'submarket': 'VARCHAR(20)',
									'csr_site_id': 'VARCHAR(15)',
									'csr_site_name': 'VARCHAR(255)',
									'datepolled': 'VARCHAR(11)',
									'timepolled': 'VARCHAR(60)',
									'lastpolled': 'TEXT',
									'sys_contact' : 'TEXT',
									'sys_location' : 'TEXT',
									'upsince' : 'VARCHAR(60)',
									'class': 'VARCHAR(4)'
								},
						'nodes':{}
					}
				}

GENERIC_KWARGS_ERROR = "[x] pass schema as `schema=<schema>,dbname=<dbname>,tblname=<tblname>`"
CURSOR = None
CONN = None
DEBUG = True

def run_once(function):
	def wrapper(*args, **kwargs):
		if not wrapper.has_run:
			wrapper.has_run = True
			return function(*args, **kwargs)
	wrapper.has_run = False
	return wrapper

@run_once
def create_connection(**kwargs):
	global CURSOR, CONN
	host = kwargs.get("host", False)
	user = kwargs.get("user", False)
	passwd = kwargs.get("passwd", False)
	db = kwargs.get("db", False)
	if ( host and user and passwd and db ):
		conn = MySQLdb.connect(	host=host,user=user,passwd=passwd,db=db )
		CURSOR = conn.cursor()
		CONN = conn
		return {"status":True,"data":"[i] created connection"}
	else:
		return {"status":False, "data":"[x] pass connection as host=<host>,user=<user>,passwd=<passwd>,db=<db>"}

@run_once
def create_database(**kwargs):
	global CURSOR, CONN
	db_names = kwargs.get("schema",None)
	if db_names:
		db_names = db_names.keys()
		for name in db_names:
			sql = "create database IF NOT EXISTS {db_name}".format(db_name=name)
			CURSOR.execute(sql)
			CONN.commit()
		return {"status":True,"data":"[i] created databases"}
	else:
		return {"status":False,"data":"[x] pass schema as `schema=<schema>`"}

def check_table_exists(**kwargs):
	dbname = kwargs.get("dbname",None)
	tblname = kwargs.get("tblname",None)
	if (dbname and tblname):
		sql = "select * from information_schema.TABLES where table_schema = '{dbname}' and table_name = '{tblname}'".format(dbname=dbname,tblname=tblname)
		CURSOR.execute(sql)
		return {"status":True,"data":CURSOR.fetchall()}
	else:
		return {"status":False,"data":"[x] pass tblname as `dbname=<dbname>,tblname=<tblname>`"}



@run_once
def create_table(**kwargs):
	global CURSOR, CONN
	db_name = kwargs.get("dbname",None)
	schema = kwargs.get("schema",None)
	if ( db_name and schema ):
		for tbl_name in schema.get(db_name):
			pre_check  = check_table_exists(dbname=db_name,tblname=tbl_name)
			if pre_check["status"]:
				if pre_check["data"]:
					return {"status":True,"data":"[i] tables exists"}
				else:
					mapped_columns = schema.get(db_name).get(tbl_name)
					column_query = "("
					for column in mapped_columns.keys():
						column_query += column + " " + mapped_columns[column] + " ,"
					column_query = column_query[:-1] + ")"
					sql = "create table IF NOT EXISTS {tbl_name} {column_query};".\
													format(	tbl_name=tbl_name,
															column_query=column_query )
					CURSOR.execute(sql)
					CONN.commit()
					return {"status":True,"data":"[i] tables created"}
			else:
				return {"status":True,"data":pre_check["data"]}
		return {"status":True,"data":"[i] created tables"}
	else:
		return {"status":False,"data":",".join(GENERIC_KWARGS_ERROR.split(",")[:-1]) }

def get_table_schema(**kwargs):
	tblname = kwargs.get("tblname",False)
	blank_frame = kwargs.get("blank",False)
	column_type = kwargs.get("ctype",False)
	if tblname:
		sql = "select * from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='{tblname}';".format(tblname=tblname)
		CURSOR.execute(sql)
		response_schema = {}
		index = 0
		for column_name in CURSOR.fetchall():
			ctype = column_name[7]
			clname = column_name[3]
			if clname not in response_schema:
				if not blank_frame:
					response_schema[clname] = index
				else:
					if column_type:
						response_schema[clname] = ctype
					else:
						response_schema[clname] = None
				index += 1
		return {"status":True,"data":response_schema}
	else:
		return {"status":False,"data":"[x] pass tblname as `tblname=<tblname>`"}


def get_dataframe(**kwargs):
	db_name = kwargs.get("dbname",None)
	tbl_name = kwargs.get("tblname",None)
	schema = kwargs.get("schema",None)
	status = create_database(schema=schema)
	if status is not None:
		if DEBUG:
			print "log"
	status = create_table(schema=schema,dbname=db_name)
	if status is not None:
		if DEBUG:
			print "log"
	if ( db_name and tbl_name and schema ):
		dataframe = {}
		mapped_columns = schema.get(db_name).get(tbl_name).keys()
		for column in mapped_columns:
			dataframe[column] = "'{value}'".format(value=None)
		return {"status":True,"data":dataframe}
	else:
		return {"status":False,"data":GENERIC_KWARGS_ERROR}

def column_datatype_reduce(**kwargs):
	value = kwargs.get("value",None)
	typecast = kwargs.get("typecast",None)
	if ( value and typecast ):
		if type(value) is typecast:
			if typecast is tuple:
				return {"status":True,"data":value[0]}
		return {"status":True,"data":value}
	else:
		return {"status":False,"data":"[x] pass values as `value=<value>,typecast=<typecast> value found {value}:{typecast}`".format(value=str(value),typecast='tuple')}


def fetch_from_db(**kwargs):
	tbl_name = kwargs.get("tblname",None)
	filter_ = kwargs.get("filter_",None)
	dataframe = get_table_schema(tblname=tbl_name,blank=False)
	response_frame = []
	if dataframe["status"]:
		dataframe = dataframe["data"]
	else:
		return {"status":dataframe["status"],"data":"[x] something went wrong"}
	and_keyword = " AND "
	if tbl_name:
		sql =  "select * from {tbl_name}".format(tbl_name=tbl_name)
		if filter_:
			filter_string = ""
			for fkey in filter_:
				str_template = "{fkey}='{value}'".format(fkey=fkey,value=filter_[fkey])
				if len(filter_.keys()) > 1:
					str_template += and_keyword
				filter_string += str_template
			if len(filter_.keys()) > 1:
				filter_string = filter_string[:-1*len(and_keyword)]
			sql = "select * from {tbl_name} where {filter_string} and id>3667".format(tbl_name=tbl_name,filter_string=filter_string)
		CURSOR.execute(sql)
		for resp in CURSOR.fetchall():
			temp = {}
			for clname in dataframe.keys():
				try:
					temp[clname] = resp[dataframe[clname]]
				except IndexError as e:
					if DEBUG:
						print "hi"
					pass
			response_frame.append(temp)

		return {"status":True,"data":response_frame}
	else:
		return {"status":False,"data":"[x] pass tblname as `tblname=<tblname>`"}

def df_kw_from_db(**kwargs):
	tblname = kwargs.get("tblname",None)
	filter_ = kwargs.get("filter_",None)
	kw = kwargs.get("kw",None)
	if( tblname and filter_ and kw ):
		frame = fetch_from_db(tblname=tblname,filter_=filter_)
		if frame["status"]:
			frame = frame["data"]
			if frame:
				label = filter_.keys()[0]
				frame = frame[0]
				if frame.get(label) == filter_.get(label):
					for clname in kw:
						if clname in frame.keys():
							kw[clname] = frame[clname]
			return {"status":True,"data":kw}
		else:
			return {"status":False,"data":"[x] Something went wrong"}
	else:
		return {"status":False,"data":"[x] pass kwargs as `tblname=<tblname>,filter_=<filter_>,kw=<kw>`"}

def update_row_table(**kwargs):
	tbl_name = kwargs.get("tblname",None)
	conditions = kwargs.get("conditions",None)
	values = kwargs.get("values",None)
	if( tbl_name and conditions and values ):
		conditions_to_check = {}
		for condition in conditions.keys():
			conditions_to_check[condition] = conditions[condition][0]
		check_if_exists = fetch_from_db(tblname=tbl_name,
										filter_=conditions_to_check)
		

		if check_if_exists["status"]:
			df_obtained = check_if_exists["data"]
			if len(df_obtained) == 1:
				sql = "update {tbl_name} set ".format(tbl_name=tbl_name)
				for value in values.keys():
					sql += "{column}='{value}',".format(column=value,value=values[value])
				sql = sql[:-1]
				sql += " where "
				for condition in conditions.keys():
					column = conditions
					value = conditions[condition]
					if len(value) == 1:
						sql += "{column}='{value}'".format(	column=condition,
															value=conditions[condition][0])
					if len(value) == 2:
						sql += "{column}='{value}' {operator} ".format(	column=condition,
																		value=conditions[condition][0],
																		operator=conditions[condition][1])
				sql = sql.strip()	
				if DEBUG:
					print "log"
				CURSOR.execute(sql)
				CONN.commit()
				return {"status":False,"data":sql}
			else:
				if len(df_obtained) == 0:
					return {"status":True,"data":None}
				return {"status":False,"data":None}
		else:
			print "Log"
		return {"status":True,"data":None}
	else:
		return {"status":False,"data":"[x] pass kwargs as `tblname=<tblname>,conditions=<conditions>,values=<values>`"}

def frame_dvalue_populate(**kwargs):
	tbl_name = kwargs.get("tblname",None)
	frame = kwargs.get("frame",None)
	vmapping = kwargs.get("mapping",None)
	if ( tbl_name and frame and vmapping ):
		dtype_column_info = get_table_schema(tblname=tbl_name,blank=True,ctype=True)
		if dtype_column_info["status"]:
			dtype_column_info = dtype_column_info["data"]
			for column in frame.keys():
				if frame[column] == None:
					dtype = dtype_column_info[column]
					if dtype in vmapping.keys():
						frame[column] = vmapping[dtype]
			return {"status":True,"data":frame}
		else:
			return{"status":False,"data":message}
	else:
		return {"status":False,"data":"[x] pass kwargs as `tblname=<tblname>,frame=<frame>,mapping=<mapping>`"}

def insert_into_table(**kwargs):
	global CURSOR, CONN
	db_name = kwargs.get("dbname",None)
	tbl_name = kwargs.get("tblname",None)
	schema = kwargs.get("schema",None)
	updates = kwargs.get("updates",None)
	ignores = kwargs.get("ignores",None)
	conditions = kwargs.get("conditions",None)
	use_blank_schema = kwargs.get("blank_schema",False)
	got_frame = get_dataframe(schema=schema,dbname=db_name,tblname=tbl_name)
	if got_frame["status"]:
		got_frame = got_frame.get("data",None)
		if ( db_name and tbl_name and schema and updates ):
			updates = frame_dvalue_populate(tblname=tbl_name,frame=updates,mapping={"int":"0"})
			if updates["status"]:
				updates = updates["data"]
			else:
				return {"status":False,"data":"[x] Frame DVALUE population failed"}
			mapped_columns = schema.get(db_name).get(tbl_name)
			if mapped_columns == {}:
				if use_blank_schema:
					mapped_columns = get_table_schema(tblname=tbl_name,blank=True)
					if mapped_columns["status"]:
						mapped_columns = mapped_columns["data"]
					else:
						return {"status":False,"data":None}
			if ignores:
				for ignore in ignores:
					if ignore in mapped_columns.keys():
						mapped_columns.pop(ignore)
					else: 
						print "log"
			sql = "insert into {tblname} (".format(tblname=tbl_name)
			values_push = ["'{value}'".format(value=None)]*len(mapped_columns.keys())
			for idx, column in enumerate(mapped_columns.keys()):
				sql +=  "{column}, ".format(column=column)
				if column in updates.keys():
					value_to_push = column_datatype_reduce(value=updates[column],typecast=tuple)
					if value_to_push["status"]:
						values_push[idx] = "'{value}'".format(value=mysql_converter.to_mysql(value_to_push["data"]))
					else:
						print "log"
			sql = sql[:-2]
			sql += ") values ( {values} )".format(values=",".join(values_push))
			insert_flag = True
			if conditions:
				if_exists = update_row_table(	tblname=tbl_name,
												conditions=conditions,
												values=updates )
				insert_flag = if_exists["status"]
			if insert_flag:
				if DEBUG:
					print "log"
				CURSOR.execute(sql)
				CONN.commit()
				return {"status":True,"data":"[i] added entry"}
			else:
				if DEBUG:
					print "log"
			return {"status":True,"data":"[i] updated entry"}
		else:
			return {"status":False,"data":GENERIC_KWARGS_ERROR}
	else:
		{"status":False,"data":"[x] something went wrong..."}

def mix_frames(**kwargs):
	frameA = kwargs.get("frameA",None)
	frameB = kwargs.get("frameB",None)
	labels = kwargs.get("labels",None)
	updates = kwargs.get("updates",None)

	if ( frameA and frameB ):
		if labels:
			for label in labels:
				if (label in frameB.keys()):
					frameA[label] = frameB[label]
				else:
					if DEBUG:
						print "log"
		else:
			if DEBUG:
				print "log"
			for label in frameB.keys():
				frameA[label] = frameB[label]
	else:
		if DEBUG:
			print "log"

	if frameA and updates:
		if updates:
			for label in updates.keys():
				frameA[label] = updates[label]
		else:
			if DEBUG:
				print "log"
	else:
		if DEBUG:
			print "log"
		return {"status":False,"data":None}
	return {"status":True,"data":frameA}


def compare_kw_frame(**kwargs):
	kw = kwargs.get("snmp_frame",None)
	frame = kwargs.get("node_frame",None)
	labels = kwargs.get("labels",None)
	if ( kw and frame ):
		if DEBUG:
			log_message = "[i] COMPARING  -> kw::frame {kw},{frame}".format(kw=str(kw),frame=str(frame))
		match_map = {}
		final_match = []
		for clname in kw.keys():
			if clname in frame:
				match_map[clname] = (kw[clname] == frame[clname])
			else:
				if DEBUG:
					print "log"
		for label in labels:
			if label in match_map:
				final_match.append(match_map[label])
			else:
				final_match.append(False)
		return {"status":True,"data":all(final_match)}
	else:
		if DEBUG:
			print "log"
		return {"status":False,"data":False}

def check_frame(**kwargs):
	frame = kwargs.get("frame",None)
	ignores = kwargs.get("ignore", None)
	response_frame = []
	ignore_mapping = {}
	if DEBUG:
		print "log"
	if ( frame ):
		if ignores:
			for ignore in ignores:
				if ignore in frame.keys():
					popped_value = frame.pop(ignore)
					ignore_mapping[ignore] = popped_value 
				else:
					if DEBUG:
						print "log"
		for clname in frame.keys():
			value =  frame[clname]
			if type(value) == str:
				value = value.strip().replace(" ","")
			if value:
				response_frame.append(True)
			else:
				response_frame.append(False)
		if ignores:
			for clname in ignore_mapping.keys():
				frame[clname] = ignore_mapping[clname]
		return {"status":True,"data":any(response_frame)}
	else:
		if DEBUG:
			print "log"
		return {"status":False,"data":[]}

def delete_frame(**kwargs):
	tbl_name = kwargs.get("tblname",None)
	conditions = kwargs.get("conditions",None)
	if ( tbl_name and conditions ):
		sql = "delete from {tblname} where".format(tblname=tbl_name)
		for condition in conditions.keys():
			column = condition
			value = conditions[condition]
			cvalue = value[0]
			operator = None
			if len(value) == 2:
				operator = value[1]
			if operator:
				sql += " {column}={value} {operator}".format( 	column=column,
																value=cvalue,
																operator=operator)
			else:
				sql += " {column}={value} ".format( 	column=column,
														value=cvalue 	)

		sql = sql.strip()
		if DEBUG:
			print "log"
		CURSOR.execute(sql)
		CONN.commit()

	else:
		return {"status":False,"data":"[x] pass kwargs as `tblname=<tblname>,conditions=<conditions>`"}



def run_test():
	"""
	insert_into_table(	schema 	=schema_map,
						dbname 	=DB_SETTINGS.get("name"),
						tblname ="discoverytmp",
						updates ={"deviceIpAddr":"8799"} ,
						conditions={"deviceIpAddr":["8796"]},
						ignores = ["id"]	)
	"""
	delete_frame(tblname="labb",conditions={"df":["val1","and"],
											"ef":["val2"]})

head = create_connection(	host	=DB_SETTINGS.get("host"),
							user	=DB_SETTINGS.get("user"),
							passwd	=DB_SETTINGS.get("passwd"),
							db 		=DB_SETTINGS.get("name") )

