import pandas as pd
import tushare as ts
import datetime as dt
import os
from sql_conn import *

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
def eod_price(date, code):
    df = ts.get_k_data(code)
    if date in df['date'].tolist():
        df = df[df['date']==date].reset_index()
        return df.at[0, 'close']
    else:
        df['date_dt'] = [dt.datetime.strptime(x, '%Y-%m-%d') for x in df['date']]
        date_dt = dt.datetime.strptime(date, '%Y-%m-%d')
        df = df[df['date_dt']<=date_dt].reset_index()
        return df.at[len(df)-1, 'close']

def stock_eod(date = dt.datetime.now().strftime('%Y-%m-%d')):
    print("STOCK EOD START...")
    sql_str = '''
        select * from equity.trade_history
    '''
    df_trade = pd.read_sql_query(sql_str, engine)
    df_trade['trade_value'] = [x * y for x, y in zip(df_trade['amt'], df_trade['trade_price'])]
    df_inventory = df_trade[['name', 'code', 'amt']].groupby(['name', 'code']).sum().reset_index()
    df_inventory['close_price'] = [eod_price(date, x) for x in df_inventory['code']]
    df_inventory['close_value'] = [x * y for x, y in zip(df_inventory['amt'], df_inventory['close_price'])]
    df_inventory['close_date'] = [date for x in range(len(df_inventory))]
    df_inventory['time_stp'] = [dt.datetime.now() for x in range(len(df_inventory))]
    
    df_inventory.to_sql('stock_inventory',
                        engine,
                        schema='equity',
                        if_exists='append',
                        index=False)
    print('STOCK EOD FINISHED!')
    print('CURRENT EOD DATE: %s' % date)

if __name__ == '__main__':
	stock_eod()