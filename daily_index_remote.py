import pandas as pd
import tushare as ts
import datetime as dt
from sql_conn import *
import asyncio
import os
from pytz import timezone
from pandas.tseries.offsets import Day

source_db = 'postgresql'
username = 'postgres'
pwd = 'sunweiyao'
ip = '119.28.222.122'
port = 5432
db = 'quant'
engine = sql_engine(source_db=source_db,
                   username=username,
                   pwd=pwd,
                   ip=ip,
                   port=port,
                   db=db)

def index_info():
	df_info = ts.get_index()
	return {code:name for code, name in zip(df_info['code'].tolist(), df_info['name'].tolist())}


def stock_history_price(code, end_date, index_dict):
	code = str(code)
	index_name = index_dict[code]
	print(index_name)
	df = ts.get_k_data(code, index=True,start=start_date(index_name), end=end_date)
	now = china_time()
	df['time_stp'] = [now for x in range(len(df))]
	df['index_name'] = [index_name for x in range(len(df))]
	if len(df) > 0:
		df.to_sql(
			'daily_index',
			engine,
			schema='equity',
			if_exists='append',
			index=False)
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


def split_task(task_list, n):
	return [task_list[i: i+n] for i in range(len(task_list)) if i%n == 0]


def start_date(index_name):
	sql_str = '''
	select max(date) as date
	from equity.daily_index
	where index_name = '@@index_name'
	'''
	sql_str = sql_str.replace('@@index_name', index_name)
	try:
		df = pd.read_sql_query(sql_str, engine)
	except:
		return '2000-01-01'
	if df.at[0, 'date'] is not None:
		current = dt.datetime.strptime(df.at[0, 'date'], '%Y-%m-%d') + Day(1)
		return current.strftime('%Y-%m-%d')
	else:
		return '2000-01-01'

index_dict = index_info()
code_list = list(index_dict.keys())
last_day_dt = china_time(value_type='datetime')
last_day = last_day_dt.strftime('%Y-%m-%d')
sub_code_lists = split_task(code_list, 100)

for codes in sub_code_lists:
	async def single(code):
		loop = asyncio.get_event_loop()
		await loop.run_in_executor(None, stock_history_price, code, last_day, index_dict,)

	async def multiple(n, code):
		print('协程%s启动' % n)
		await single(code)

	tasks = []
	for i in range(len(codes)):
		tasks.append(multiple(i, codes[i]))

	loop = asyncio.get_event_loop()
	loop.run_until_complete(asyncio.wait(tasks))
