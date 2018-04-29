import pandas as pd
import tushare as ts
import datetime as dt
from sql_conn import *
import os
from pytz import timezone
from pandas.tseries.offsets import Day
import numpy as np

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


def cash_balance_eod(date=dt.datetime.now().strftime('%Y-%m-%d')):
	print('UPDATING CASH BALANCE OF %s' % date)
	date = dt.datetime.strptime(date, '%Y-%m-%d')
	sql_cash = '''
		select cast(trade_date as text) as trade_date,
		cash_flow, flow_source, portfolio
		from equity.cash_history
		order by trade_date
	'''
	df_cash = pd.read_sql_query(sql_cash, engine)
	df = df_cash[['trade_date', 'portfolio','cash_flow']].groupby(['trade_date', 'portfolio']).sum().reset_index()
	df.columns = ['trade_date', 'portfolio', 'cash_balance']
	df['trade_date'] = [dt.datetime.strptime(x, '%Y%m%d') for x in df['trade_date']]

	datadates, cash_balances = [], []
	df_tmp = df[df['trade_date'] <= date]
	datadates.append(date.strftime('%Y-%m-%d'))
	cash_balances.append(float(df['cash_balance'].sum()))
	df_cash_balance = pd.DataFrame()
	df_cash_balance['datadate'] = datadates
	df_cash_balance['cash_balance'] = cash_balances
	df_cash_balance['time_stp'] = [dt.datetime.now() for x in range(len(df_cash_balance))]
	df_cash_balance.to_sql('cash_balance',
							 engine,
							 schema='equity',
							 if_exists='append',
							 index=False)
	print('UPDATE FINISHED!')

if __name__ == '__main__':
	cash_balance_eod()