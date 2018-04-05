import tushare as ts
import pandas as pd
import datetime as dt
import asyncio 
from pytz import timezone
from pandas.tseries.offsets import Day
from sql_conn import *
from time import sleep
from progress_bar import *
import traceback, os

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

def get_date_list():
	sql_str = '''
	select distinct date from equity.daily_price
	order by date
	'''
	df = pd.read_sql_query(sql_str, engine)
	return df['date'].tolist()

def get_code_list():
	sql_str = '''
	select distinct code from equity.daily_price
	order by code
	'''
	df = pd.read_sql_query(sql_str, engine)
	return df['code'].tolist()

def split_task(task_list, n):
	return [task_list[i: i+n] for i in range(len(task_list)) if i%n == 0]

def china_time(value_type='string'):
	utc_now = dt.datetime.utcnow()
	tzchina = timezone('Asia/Shanghai')
	utc = timezone('UTC')
	china_now = utc_now.replace(tzinfo=utc).astimezone(tzchina)
	if value_type == 'string':
		return china_now.strftime('%Y-%m-%d %H:%M:%S')
	elif value_type == 'datetime':
		return china_now
	
def tick_data(code, date, pause):
	code = str(code)
	try:
		df = ts.get_tick_data(code, date, pause=pause, retry_count=10)
	except:
		traceback.print_exc()
		sleep(5)
		return 
	df = df.dropna()
	if len(df) > 0:
		df['code'] = [code for x in range(len(df))]
		df['trade_date'] = [date for x in range(len(df))]
		now = china_time()
		df['time_stp'] = [now for x in range(len(df))]

		# path = 'C:\\Users\\Public\\data\\' + '%s_%s.json' % (code, date)
		df.to_sql('daily_tick',
			engine,
			schema='equity',
			if_exists='append',
			index=False)

	return 

def download(code_list, date, pause):
	bar = ProgressBar(total=len(code_list))
	for i in range(len(code_list)):
		tick_data(code_list[i], date, pause)
		counter = (i + 1) / len(code_list)
		bar.move()
		bar.log('%s completed...' % ('{:.2%}'.format(counter)))


if __name__ == '__main__':
	for last_day in ['2018-03-09']:
		print('开始下载历史分笔数据...')
		print('正在获取股票代码列表...')
		code_list = get_code_list()
		last_day_dt = china_time(value_type='datetime') - Day(1)
		# last_day = last_day_dt.strftime('%Y-%m-%d')
		print('开始下载，数据日期：%s' % last_day)
		download(code_list, last_day, pause=1)
