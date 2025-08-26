from queries.duckdb import utils

if utils.settings.run.io_type == 'skip':
	from queries.common_utils import get_table_path
	import os
	import duckdb
	db_name = utils.get_persistent_path()
	if not os.path.isfile(db_name):
		con = duckdb.connect(db_name)
		table_list = ['customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier']
		for table_name in table_list:
			parquet_path = get_table_path(table_name)
			con.sql(f"create table {table_name} as from read_parquet('{parquet_path}')")

