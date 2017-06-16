import pandas as pd
import os
from datetime import datetime
from WindPy import *

from utils.tool_funcs import get_industry_code
from utils.datetime_func import DateStr2Datetime
from const import SW_INDUSTRY_DICT_REVERSE, CS_INDUSTRY_DICT_REVERSE
from data_source import h5, mysql_engine, sec
from data_source.wind_plugin import get_history_bar
from data_source.data_api import get_trade_days, trade_day_offset
from data_source.financial_data_source import BalanceSheet, IncomeSheet

#-----------------------------------Mongo部分-------------------------------------
engine = mysql_engine

def update_price(start, end):
    """更新价量行情数据"""
    #field_names = "收盘价 涨跌幅 最高价 最低价 成交量"
    #data = get_history_bar(field_names.split(),start,end,**{'复权方式':'不复权'})
    #data.columns = ['close','daily_returns_%','high','low','vol']
    #data['vol'] = data['vol'] / 100
    #data['daily_returns'] = data['daily_returns_%'] / 100
    #h5.save_factor(data,'/stocks/')

    field_names = "总市值 A股市值(不含限售股)"
    data = get_history_bar(field_names.split(),start,end)
    data.columns = ['total_mkt_value','float_mkt_value']
    data = data / 10000
    h5.save_factor(data,'/stocks/')

    field_names = "收盘价"
    data = get_history_bar(field_names.split(),start,end,**{'复权方式':'后复权'})
    data.columns = ['adj_close']
    h5.save_factor(data,'/stocks/')

    field_names = "换手率 换手率(基准.自由流通股本)"
    data = get_history_bar(field_names.split(),start,end)
    data.columns = ['turn','freeturn']
    h5.save_factor(data,'/stock_liquidity/')

    field_names = "开盘价 最高价 最低价 收盘价 成交量 成交额"
    data = get_history_bar(field_names.split(),start,end,id_type='index')
    data.columns = ['open','high','low','close','vol','amt']
    data['amt'] = data['amt'] / 10000
    data['vol'] = data['vol'] / 100
    h5.save_factor(data,'/indexprices/')

def update_sector(start, end):
    """更新成分股信息"""

    all_dates = get_trade_days(start,end)

    w.start()
    _000300 = []
    _000905 = []
    _399102 = []
    _880011 = []
    _st = []
    _000906 = []
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
        
        d = w.wset("sectorconstituent","date={date};windcode=000906.SH".format(date=date))
        d = d.Data[1]
        d = pd.DataFrame(d,columns=['IDs'])
        d['_000906']=1
        d['IDs'] = d['IDs'].str[:6]
        d['date'] = DateStr2Datetime(date)
        _000906.append(d)        

        d = w.wset("sectorconstituent","date={date};sectorid=1000006526000000".format(date=date))
        d = d.Data[1]
        d = pd.DataFrame(d,columns=['IDs'])
        d['is_st']=1
        d['IDs'] = d['IDs'].str[:6]
        d['date'] = DateStr2Datetime(date)
        _st.append(d)

    d = pd.concat(_000300,ignore_index=True)
    d = d.set_index(['date','IDs']).sort_index()
    h5.save_factor(d,'/indexes/')

    d = pd.concat(_000905,ignore_index=True)
    d = d.set_index(['date','IDs']).sort_index()
    h5.save_factor(d,'/indexes/')

    d = pd.concat(_399102,ignore_index=True)
    d = d.set_index(['date','IDs']).sort_index()
    h5.save_factor(d,'/indexes/')

    d = pd.concat(_880011,ignore_index=True)
    d = d.set_index(['date','IDs']).sort_index()
    h5.save_factor(d,'/indexes/')
    
    d = pd.concat(_000906,ignore_index=True)
    d = d.set_index(['date','IDs']).sort_index()
    h5.save_factor(d, '/indexes/')    

    d = pd.concat(_st,ignore_index=True)
    d = d.set_index(['date','IDs']).sort_index()
    h5.save_factor(d,'/stocks/')

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
        h5.save_factor(data,'/indexes/')

    sql_str = 'select '+','.join(['IDs','Dates','Weight'])
    sql_str+=' from idx_weight_hs300 where Dates between %s and %s' % (start, end)
    data = pd.read_sql(sql_str,engine)
    if not data.empty:
        data.columns = ['IDs','date','_000300_weight']
        data.date = pd.DatetimeIndex(data.date)
        data.set_index(['date','IDs'],inplace=True)
        data.sort_index(inplace=True)
        h5.save_factor(data,'/indexes/')
    
    sql_str = 'select '+','.join(['IDs','Dates','Weight'])
    sql_str+=' from idx_weight_000991 where Dates between %s and %s' % (start, end)
    data = pd.read_sql(sql_str,engine)
    if not data.empty:
        data.columns = ['IDs','date','_000991_weight']
        data.date = pd.DatetimeIndex(data.date)
        data.set_index(['date','IDs'],inplace=True)
        data.sort_index(inplace=True)
        h5.save_factor(data,'/indexes/')        


def update_sw_level_1(start, end):
    excel_path = os.sep.join(os.path.abspath('.').split(os.sep)[:-2]+['resource', 'sw_level_1.xlsx'])
    sw_level_1 = pd.read_excel(excel_path, header=0, converters={'entry_date': str, 'remove_dt': str, 'IDs': str})
    sw_level_1.loc[pd.isnull(sw_level_1['remove_dt']), 'remove_dt'] = '21000101'
    all_dates = get_trade_days(start, end)
    _l = []
    for idate in all_dates:
        data = sw_level_1.query("entry_date<=@idate & remove_dt>@idate" )[['IDs', 'sw_level_1']]
        data['date'] = DateStr2Datetime(idate)
        _l.append(data)
    df = pd.concat(_l)
    df.set_index(['date', 'IDs'], inplace=True)
    df['industry_code'] = df['sw_level_1'].apply(lambda x: int(SW_INDUSTRY_DICT_REVERSE[x]))
    h5.save_factor(df[['industry_code']].rename(columns={'industry_code': 'sw_level_1'}), '/indexes/')

def update_cs_level_1(start, end):
    excel_path = os.sep.join(os.path.abspath('.').split(os.sep)[:-2]+['resource', 'cs_level_1.xlsx'])
    sw_level_1 = pd.read_excel(excel_path, header=0, converters={'entry_date': str, 'remove_dt': str, 'IDs': str})
    sw_level_1.loc[pd.isnull(sw_level_1['remove_dt']), 'remove_dt'] = '21000101'
    all_dates = get_trade_days(start, end)
    _l = []
    for idate in all_dates:
        data = sw_level_1.query("entry_date<=@idate & remove_dt>@idate" )[['IDs', 'cs_level_1']]
        data['date'] = DateStr2Datetime(idate)
        _l.append(data)
    df = pd.concat(_l)
    df.set_index(['date', 'IDs'], inplace=True)
    df['industry_code'] = df['cs_level_1'].apply(lambda x: int(CS_INDUSTRY_DICT_REVERSE[x][2:]))
    h5.save_factor(df[['industry_code']].rename(columns={'industry_code': 'cs_level_1'}), '/indexes/')

def update_cs_level_2(start, end):
    excel_path = os.sep.join(os.path.abspath('.').split(os.sep)[:-2]+['resource', 'cs_level_2.xlsx'])
    cs_level_2 = pd.read_excel(excel_path, header=0, converters={'entry_dt': str, 'remove_dt': str, 'IDs': str})
    all_dates = get_trade_days(start, end)
    _l = []
    for idate in all_dates:
        data = cs_level_2.query("entry_dt<=@idate & remove_dt>@idate" )[['IDs', 'cs_level_2']]
        data['date'] = DateStr2Datetime(idate)
        _l.append(data)
    df = pd.concat(_l)
    df.set_index(['date', 'IDs'], inplace=True)
    h5.save_factor(get_industry_code('cs_level_2', df), '/indexes/')

def update_sw_level_2(start, end):
    excel_path = os.sep.join(os.path.abspath('.').split(os.sep)[:-2]+['resource', 'sw_level_2.xlsx'])
    sw_level_2 = pd.read_excel(excel_path, header=0, converters={'entry_dt': str, 'remove_dt': str, 'IDs': str})
    all_dates = get_trade_days(start, end)
    _l = []
    for idate in all_dates:
        data = sw_level_2.query("entry_dt<=@idate & remove_dt>@idate" )[['IDs', 'sw_level_2']]
        data['date'] = DateStr2Datetime(idate)
        _l.append(data)
    df = pd.concat(_l)
    df.set_index(['date', 'IDs'], inplace=True)
    h5.save_factor(get_industry_code('sw_level_2', df), '/indexes/')

def update_ashare(start, end):
    excel_path = os.sep.join(os.path.abspath('.').split(os.sep)[:-2]+['resource', 'stock_info.csv'])
    stock_info = pd.read_csv(excel_path, header=0, converters={'S_INFO_LISTDATE':str,'S_INFO_DELISTDATE':str})
    stock_info.loc[pd.isnull(stock_info['S_INFO_DELISTDATE']), 'S_INFO_DELISTDATE'] = '21000101'
    all_dates = get_trade_days(start, end)
    _l = []
    for idate in all_dates:
        data = stock_info.query("S_INFO_LISTDATE<=@idate & S_INFO_DELISTDATE>@idate" )[['S_INFO_WINDCODE', 'S_INFO_LISTDATE']]
        data['date'] = DateStr2Datetime(idate)
        data['ashare'] = 1
        data['IDs'] = data['S_INFO_WINDCODE'].str[:6]
        _l.append(data)
    df = pd.concat(_l)
    df.set_index(['date', 'IDs'], inplace=True)    
    h5.save_factor(df[['ashare']], '/indexes/')

def update_trade_status(start, end):
    dates = get_trade_days(start, end)
    
    st = sec.get_st(dates)
    suspend = sec.get_suspend(dates)
    uplimit = sec.get_uplimit(dates)
    downlimit = sec.get_downlimit(dates)

    trade_status = pd.concat([st,suspend,uplimit,downlimit],axis=1)
    trade_status = trade_status.where(pd.isnull(trade_status), other=1)
    trade_status.fillna(0,inplace=True)
    trade_status.columns = ['st','suspend','uplimit','downlimit']
    trade_status['no_trading'] = trade_status.any(axis=1).astype('int32')
    
    h5.save_factor(trade_status, '/trade_status/')

def update_financial_data(start, end):
    dates = get_trade_days(start, end)
    icmsheet = IncomeSheet()
    balcsheet = BalanceSheet()
    
    # 净资产
    net_assets = balcsheet.last_quater(['tot_shrhldr_eqy_excl_min_int'], dates)
    net_assets.columns = ['net_asset_last_quater']
    h5.save_factor(net_assets, '/stock_financial_data/')
    
    # 净利润ttm
    net_profit_ttm = icmsheet.last_ttm(['net_profit_excl_min_int_inc'], dates)
    net_profit_ttm.columns = ['net_profit_last_ttm']
    h5.save_factor(net_profit_ttm, '/stock_financial_data/')
    
    # 净利润最近年报
    net_profit_last_year = icmsheet.last_year(['net_profit_excl_min_int_inc'], dates)
    net_profit_last_year.columns = ['net_profit_last_year']
    h5.save_factor(net_profit_last_year, '/stock_financial_data/')
    
    # 主营业务收入ttm
    oper_rev_ttm = icmsheet.last_ttm(['oper_rev'], dates)
    oper_rev_ttm.columns = ['oper_rev_last_ttm']
    h5.save_factor(oper_rev_ttm, '/stock_financial_data/')
    
    # 主营业务收入回溯一期
    oper_rev_ttm_back_1P = icmsheet.last_ttm_back_nperiod(['oper_rev'], 1, dates)
    oper_rev_ttm_back_1P.columns = ['oper_rev_last_ttm_back_1P']
    h5.save_factor(oper_rev_ttm_back_1P, '/stock_financial_data/')
    
UpdateFuncs = [update_price, update_sector, update_idx_weight,
                    update_sw_level_1, update_cs_level_1, update_cs_level_2, update_sw_level_2, 
                    update_ashare, update_trade_status, update_financial_data]
UpdateFuncs = [update_sw_level_1]
for iFunc in UpdateFuncs:
    iFunc('20170425', '20170502')