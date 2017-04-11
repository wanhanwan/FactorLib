import pandas as pd
import os

file_path = os.path.abspath('../resource/stock_info.csv')
stock_info = pd.read_csv(file_path, header=0)
stock_info.columns = ['IDs','Code','Name','list_date','delist_date']
stock_info['IDs'] = stock_info['IDs'].str[:6]
stock_info['list_date'] = stock_info['list_date'].fillna(99999999)
stock_info['list_date'] = stock_info['list_date'].astype('int').astype('str')
stock_info['delist_date'] = stock_info['delist_date'].fillna(99999999)
stock_info['delist_date'] = stock_info['delist_date'].astype('int').astype('str')
stock_info['Date'] = '11111111'
stock_info = stock_info.set_index(['Date','IDs'])

store = pd.HDFStore('/Users/wanhanwan/Documents/data/h5DB/stocks.h5')
try:
    store.remove('stock_info')
except:
    pass
store.append('stock_info', stock_info, data_columns=['list_date','delist_date'])
store.close()


