import pandas as pd
import tushare as ts
import datetime as dt
from sql_conn import *
import os
from pytz import timezone
from pandas.tseries.offsets import Day
import numpy as np 
%matplotlib inline

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



def pl_by_period(stock='total', period='D'):
	# 返回Daily/Weekly/Monthly/Quarterly/Yearly的PL，默认为Daily
	# 可制定股票代码或股票名称，默认为全部
	sql_str = '''
	select * from equity.daily_pl_unrealized
	order by pl_date desc 
	'''
	df = pd.read_sql_query(sql_str, engine)

	if stock == 'total':
		df_group = df[['pl_date', 'daily_pl']].groupby('pl_date').sum()
	elif len(stock) == 6:
		df_group = df[df['code']==str(stock)][['pl_date', 'daily_pl']].groupby('pl_date').sum()
	elif len(stock) == 4:
		df_group = df[df['name']==stock][['pl_date', 'daily_pl']].groupby('pl_date').sum()
	df_group.reset_index('pl_date', inplace=True)
	df_group['pl_date'] = [dt.datetime.strptime(x, '%Y-%m-%d') for x in df_group['pl_date']]
	df_group.set_index('pl_date', inplace=True)
	period_dict = {
		'D':'daily_pl',
		'W':'weekly_pl',
		'M':'monthly_pl',
		'Q':'quarterly_pl',
		'Y':'yearly_pl'
	}
	df_group.columns = [period_dict[period]]
	return df_group.resample(period).sum()