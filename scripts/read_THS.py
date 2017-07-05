from data_source import h5
import pandas as pd

_l = []
for i in range(1, 8):
    data = pd.read_table("C:/Users/wanshuai/Downloads/THS_%d.txt" % i,
                         header=0, sep='|', dtype={'代码': 'str'}, parse_dates=['日期'])
    _l.append(data[['日期', '代码', '比例']])
rslt = pd.concat(_l, ignore_index=True).drop_duplicates(subset=['日期', '代码'])
rslt.columns = ['date', 'IDs', 'ths_click_ratio']
rslt = rslt.set_index(['date', 'IDs']).sort_index()
h5.save_factor(rslt, '/stock_alternative/')
    
    