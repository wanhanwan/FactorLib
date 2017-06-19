import pandas as pd
from iFinDPy import *
from const import THS_USERID, THS_PASSWORD
from utils.tool_funcs import tradecode_to_windcode
from data_source import sec, tc, h5

THS_iFinDLogin(THS_USERID, THS_PASSWORD)


def avg_price(start, end):
    filed = 'avgprice'
    default_params = "period:D,pricetype:6,rptcategory:0,fqdate:1900-01-01,hb:YSHB,fill:Previous"
    all_dates = tc.get_trade_days(start, end)

    _l = []
    for d in all_dates:
        print(d)
        date_param = '-'.join([d[:4], d[4:6], d[-2:]])
        ids = sec.get_history_ashare(d).index.get_level_values(1).unique()
        ids = map(tradecode_to_windcode, ids)
        codes_params = ",".join(ids)
        data = THS_HistoryQuotes(codes_params, filed, default_params, date_param, date_param)
        data_frame = THS_Trans2DataFrame(data)
        _l.append(data_frame)
    data = pd.concat(_l)
    data['IDs'] = data['thscode'].str[:6]
    data.rename(columns={'time': 'date'}, inplace=True)
    data['date'] = pd.DatetimeIndex(data['date'])
    data.set_index(['date', 'IDs'], inplace=True)
    data = data[pd.notnull(data.index.get_level_values(0))]
    h5.save_factor(data[['avgprice']].astype('float32'), '/stocks/')


def avg_price_dailyfiles(start, end, file_path):
    import os
    filed = 'avgprice'
    default_params = "period:D,pricetype:7,rptcategory:0,fqdate:1900-01-01,hb:YSHB,fill:Previous"
    all_dates = tc.get_trade_days(start, end)

    for d in all_dates:
        print(d)
        date_param = '-'.join([d[:4], d[4:6], d[-2:]])
        ids = sec.get_history_ashare(d).index.get_level_values(1).unique()
        ids = map(tradecode_to_windcode, ids)
        codes_params = ",".join(ids)
        data = THS_HistoryQuotes(codes_params, filed, default_params, date_param, date_param)
        data_frame = THS_Trans2DataFrame(data)
        data_frame.to_csv(os.path.join(file_path, d+'.csv'), index=False)

def close_houfuquan_dailyfiles(start, end, file_path):
    import os
    filed = 'close'
    default_params = "period:D,pricetype:7,rptcategory:0,fqdate:1900-01-01,hb:YSHB,fill:Previous"
    all_dates = tc.get_trade_days(start, end)

    for d in all_dates:
        print(d)
        date_param = '-'.join([d[:4], d[4:6], d[-2:]])
        ids = sec.get_history_ashare(d).index.get_level_values(1).unique()
        ids = map(tradecode_to_windcode, ids)
        codes_params = ",".join(ids)
        data = THS_HistoryQuotes(codes_params, filed, default_params, date_param, date_param)
        data_frame = THS_Trans2DataFrame(data)
        data_frame.to_csv(os.path.join(file_path, d+'.csv'), index=False)


def high_houfuquan_dailyfiles(start, end, file_path):
    import os
    field = 'high'
    default_params = "period:D,pricetype:7,rptcategory:0,fqdate:1900-01-01,hb:YSHB,fill:Previous"
    all_dates = tc.get_trade_days(start, end)

    for d in all_dates:
        print(d)
        date_param = '-'.join([d[:4], d[4:6], d[-2:]])
        ids = sec.get_history_ashare(d).index.get_level_values(1).unique()
        ids = map(tradecode_to_windcode, ids)
        codes_params = ",".join(ids)
        data = THS_HistoryQuotes(codes_params, field, default_params, date_param, date_param)
        data_frame = THS_Trans2DataFrame(data)
        data_frame.to_csv(os.path.join(file_path, d+'.csv'), index=False)


def low_houfuquan_dailyfiles(start, end, file_path):
    import os
    field = 'low'
    default_params = "period:D,pricetype:7,rptcategory:0,fqdate:1900-01-01,hb:YSHB,fill:Previous"
    all_dates = tc.get_trade_days(start, end)

    for d in all_dates:
        print(d)
        date_param = '-'.join([d[:4], d[4:6], d[-2:]])
        ids = sec.get_history_ashare(d).index.get_level_values(1).unique()
        ids = map(tradecode_to_windcode, ids)
        codes_params = ",".join(ids)
        data = THS_HistoryQuotes(codes_params, field, default_params, date_param, date_param)
        data_frame = THS_Trans2DataFrame(data)
        data_frame.to_csv(os.path.join(file_path, d+'.csv'), index=False)


def lastclose_houfuquan_dailyfiles(start, end, file_path):
    import os
    field = 'lastclose'
    default_params = "period:D,pricetype:7,rptcategory:0,fqdate:1900-01-01,hb:YSHB,fill:Previous"
    all_dates = tc.get_trade_days(start, end)

    for d in all_dates:
        print(d)
        date_param = '-'.join([d[:4], d[4:6], d[-2:]])
        ids = sec.get_history_ashare(d).index.get_level_values(1).unique()
        ids = map(tradecode_to_windcode, ids)
        codes_params = ",".join(ids)
        data = THS_HistoryQuotes(codes_params, field, default_params, date_param, date_param)
        data_frame = THS_Trans2DataFrame(data)
        data_frame.to_csv(os.path.join(file_path, d+'.csv'), index=False)

def open_houfuquan_dailyfiles(start, end, file_path):
    import os
    field = 'open'
    default_params = "period:D,pricetype:7,rptcategory:0,fqdate:1900-01-01,hb:YSHB,fill:Previous"
    all_dates = tc.get_trade_days(start, end)

    for d in all_dates:
        print(d)
        date_param = '-'.join([d[:4], d[4:6], d[-2:]])
        ids = sec.get_history_ashare(d).index.get_level_values(1).unique()
        ids = map(tradecode_to_windcode, ids)
        codes_params = ",".join(ids)
        data = THS_HistoryQuotes(codes_params, field, default_params, date_param, date_param)
        data_frame = THS_Trans2DataFrame(data)
        data_frame.to_csv(os.path.join(file_path, d+'.csv'), index=False)

high_houfuquan_dailyfiles('20070101', '20170601', 'D:/data/CSV/high_houfuquan')
low_houfuquan_dailyfiles('20070101', '20170601', 'D:/data/CSV/low_houfuquan')
lastclose_houfuquan_dailyfiles('20070101', '20170601', 'D:/data/CSV/lastclose_houfuquan')




