import pandas as pd
from datetime import datetime
from WindPy import *

from utils.datetime_func import DateStr2Datetime
from data_source import mongo, mysql_engine
from data_source.wind_plugin import get_history_bar
from data_source.data_api import get_trade_days, trade_day_offset

#-----------------------------------Mongo部分-------------------------------------
engine = mysql_engine

def update_price(start, end):
    """更新价量行情数据"""
    field_names = "收盘价 涨跌幅 最高价 最低价 成交量"
    data = get_history_bar(field_names.split(),start,end,**{'复权方式':'不复权'})
    data.columns = ['close','daily_returns_%','high','low','vol']
    data['vol'] = data['vol'] / 100
    data['daily_returns'] = data['daily_returns_%'] / 100
    mongo.saveFactor(data,'stocks')

    field_names = "总市值 A股市值(不含限售股)"
    data = get_history_bar(field_names.split(),start,end)
    data.columns = ['total_mkt_value','float_mkt_value']
    data = data / 10000
    mongo.saveFactor(data,'stocks')

    field_names = "收盘价"
    data = get_history_bar(field_names.split(),start,end,**{'复权方式':'后复权'})
    data.columns = ['adj_close']
    mongo.saveFactor(data,'stocks')

    sql_str = 'select '+','.join(['IDs','Dates','pb','pe','pcf'])
    sql_str+=' from pe_pb_pcf where Dates between %s and %s' % (start, end)
    data = pd.read_sql(sql_str,engine)
    if not data.empty:
        data.columns = ['IDs','date','pb','pe','pcf']
        data.date = pd.DatetimeIndex(data.date)
        data.set_index(['date','IDs'],inplace=True)
        data.sort_index(inplace=True)
        mongo.saveFactor(data,'stocks')

    field_names = "换手率 换手率(基准.自由流通股本)"
    data = get_history_bar(field_names.split(),start,end)
    data.columns = ['turn','freeturn']
    mongo.saveFactor(data,'stocks')

    field_names = "开盘价 最高价 最低价 收盘价 成交量 成交额"
    data = get_history_bar(field_names.split(),start,end,id_type='index')
    data.columns = ['open','high','low','close','vol','amt']
    data['amt'] = data['amt'] / 10000
    data['vol'] = data['vol'] / 100
    mongo.saveFactor(data,'indexprices')

def update_sector(start, end):
    """更新成分股信息"""

    all_dates = get_trade_days(start,end)

    w.start()
    _000300 = []
    _000905 = []
    _399102 = []
    _880011 = []
    _st = []
    for date in all_dates:

        d = w.wset("sectorconstituent","date={date};windcode=000300.SH".format(date=date))
        d = d.Data[1]
        d = pd.DataFrame(d,columns=['IDs'])
        d['_000300']=1
        d['IDs'] = d['IDs'].str[:6]
        d['date'] = DateStr2Datetime(date)
        _000300.append(d)

        d = w.wset("sectorconstituent","date={date};windcode=000905.SH".format(date=date))
        d = d.Data[1]
        d = pd.DataFrame(d,columns=['IDs'])
        d['_000905']=1
        d['IDs'] = d['IDs'].str[:6]
        d['date'] = DateStr2Datetime(date)
        _000905.append(d)

        d = w.wset("sectorconstituent","date={date};windcode=399102.SZ".format(date=date))
        d = d.Data[1]
        d = pd.DataFrame(d,columns=['IDs'])
        d['_399102']=1
        d['IDs'] = d['IDs'].str[:6]
        d['date'] = DateStr2Datetime(date)
        _399102.append(d)

        d = w.wset("sectorconstituent","date={date};windcode=881001.WI".format(date=date))
        d = d.Data[1]
        d = pd.DataFrame(d,columns=['IDs'])
        d['_880011']=1
        d['IDs'] = d['IDs'].str[:6]
        d['date'] = DateStr2Datetime(date)
        _880011.append(d)

        d = w.wset("sectorconstituent","date={date};sectorid=1000006526000000".format(date=date))
        d = d.Data[1]
        d = pd.DataFrame(d,columns=['IDs'])
        d['is_st']=1
        d['IDs'] = d['IDs'].str[:6]
        d['date'] = DateStr2Datetime(date)
        _st.append(d)

    d = pd.concat(_000300,ignore_index=True)
    d = d.set_index(['date','IDs']).sort_index()
    mongo.saveFactor(d,'indexes')

    d = pd.concat(_000905,ignore_index=True)
    d = d.set_index(['date','IDs']).sort_index()
    mongo.saveFactor(d,'indexes')

    d = pd.concat(_399102,ignore_index=True)
    d = d.set_index(['date','IDs']).sort_index()
    mongo.saveFactor(d,'indexes')

    d = pd.concat(_880011,ignore_index=True)
    d = d.set_index(['date','IDs']).sort_index()
    mongo.saveFactor(d,'indexes')

    d = pd.concat(_st,ignore_index=True)
    d = d.set_index(['date','IDs']).sort_index()
    mongo.saveFactor(d,'stocks')


    # 全部A股
    d=w.wset("sectorconstituent","date={date};sectorid=a001010100000000".format(date=all_dates[-1]))
    d=d.Data[1]

    industry = w.wsd(d, "industry_sw", all_dates[0], all_dates[-1], "industryType=1")
    industry = pd.DataFrame(industry.Data,index=d,columns=all_dates).T
    industry.index = pd.DatetimeIndex(industry.index)
    industry.columns = industry.columns.str[:6]
    industry = industry.stack().to_frame().rename(columns={0:'sw_level_1'})
    industry.index.names=['date','IDs']
    mongo.saveFactor(industry,'indexes')

    industry = w.wsd(d, "industry_citic", all_dates[0], all_dates[-1], "industryType=1")
    industry = pd.DataFrame(industry.Data,index=d,columns=all_dates).T
    industry.index = pd.DatetimeIndex(industry.index)
    industry.columns = industry.columns.str[:6]
    industry = industry.stack().to_frame().rename(columns={0:'cs_level_1'})
    industry.index.names=['date','IDs']
    mongo.saveFactor(industry,'indexes')
    w.close()

def update_idx_weight(start, end):
    """更新指数权重"""
    sql_str = 'select '+','.join(['IDs','Dates','Weight'])
    sql_str+=' from idx_weight_zz500 where Dates between %s and %s' % (start, end)
    data = pd.read_sql(sql_str,engine)
    if not data.empty:
        data.columns = ['IDs','date','_000905_weight']
        data.date = pd.DatetimeIndex(data.date)
        data.set_index(['date','IDs'],inplace=True)
        data.sort_index(inplace=True)
        mongo.saveFactor(data,'indexes')

    sql_str = 'select '+','.join(['IDs','Dates','Weight'])
    sql_str+=' from idx_weight_hs300 where Dates between %s and %s' % (start, end)
    data = pd.read_sql(sql_str,engine)
    if not data.empty:
        data.columns = ['IDs','date','_000300_weight']
        data.date = pd.DatetimeIndex(data.date)
        data.set_index(['date','IDs'],inplace=True)
        data.sort_index(inplace=True)
        mongo.saveFactor(data,'indexes')

mongoUpdateFuncs = [update_price, update_sector, update_idx_weight]

for iFunc in mongoUpdateFuncs:
    iFunc('20170331', '20170410')