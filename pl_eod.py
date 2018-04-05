import pandas as pd
import tushare as ts
import datetime as dt
from sql_conn import *
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

def get_pl_u(df, code, value_date, origin_date):
	# unrealized pl
	df = df[df['code']==code]
	close_p = df[df['close_date']==value_date]['close_price'].values[0]
	origin_p = df[df['close_date']==origin_date]['close_price'].values[0]
	close_amt = df[df['close_date']==value_date]['amt'].values[0]
	
	return (close_p - origin_p) * close_amt

def inventory_daily_pl_u(value_date, origin_date):
	# unrealized pl
	sql_str = '''
	select * 
	from equity.stock_inventory
	where close_date in ('@@value_date', '@@origin_date')
	order by close_date
	'''
	sql_str = sql_str.replace('@@value_date', value_date)
	sql_str = sql_str.replace('@@origin_date', origin_date)
	df = pd.read_sql_query(sql_str, engine)
	codes = list(df['code'].unique())
	names = []
	pls = []
	for code in codes:
		names.append(df[df['code']==code]['name'].values[0])
		pls.append(get_pl_u(df, code, value_date, origin_date))
	df_daily_pl = pd.DataFrame()
	df_daily_pl['code'] = codes
	df_daily_pl['name'] = names
	df_daily_pl['daily_pl'] = pls
	df_daily_pl['pl_date'] = [value_date for x in range(len(df_daily_pl))]
	df_daily_pl['time_stp'] = [dt.datetime.now() for x in range(len(df_daily_pl))]
	
	df_daily_pl.to_sql('daily_pl_unrealized',
					  engine,
					  schema='equity',
					  if_exists='append',
					  index=False)
	print(value_date)
	
def pl_eod_u():
	sql_close_dates = '''
	select distinct close_date 
	from equity.stock_inventory
	order by close_date
	'''
	df = pd.read_sql_query(sql_close_dates, engine)
	dates = df['close_date'].tolist()
	value_dates, origin_dates = dates[1:], dates[:-1]
	for value_date, origin_date in zip(value_dates, origin_dates):
		inventory_daily_pl_u(value_date, origin_date)

if __name__ == '__main__':
	pl_eod_u()