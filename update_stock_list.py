import pandas as pd 
import tushare as ts
import datetime as dt 
from sql_conn import *
from pytz import timezone

def engine_config():
	source_db = 'postgresql'
	username = 'postgres'
	pwd = 'sunweiyao'
	ip = '127.0.0.1'
	port = 5432
	db = 'quant'
	engine = sql_engine(source_db=source_db,
	                   username=username,
	                   pwd=pwd,
	                   ip=ip,
	                   port=port,
	                   db=db)

	return engine

def clear_sql():
	sql_str = '''
		delete from equity.stock_basic_info
	'''
	ex_task(engine_config(), sql_str)

	return 

def china_time(value_type='string'):
	utc_now = dt.datetime.utcnow()
	tzchina = timezone('Asia/Shanghai')
	utc = timezone('UTC')
	china_now = utc_now.replace(tzinfo=utc).astimezone(tzchina)
	if value_type == 'string':
		return china_now.strftime('%Y-%m-%d %H:%M:%S')
	elif value_type == 'datetime':
		return china_now

def update_sql():
        df = ts.get_stock_basics()
        print('stock basic info retrieved.')
        df = df.reset_index()
        print('updating remote database...')
        df['time_stp'] = [china_time() for x in range(len(df))]
        df.to_sql('stock_basic_info',
		engine_config(),
		schema='equity',
		index=False,
		if_exists='append')
        print('update finished!')
        return 

clear_sql()
update_sql()