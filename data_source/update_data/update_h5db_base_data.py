import pandas as pd
import os
from datetime import datetime
from functools import lru_cache
from WindPy import *

from utils.tool_funcs import get_industry_code
from utils.datetime_func import DateStr2Datetime
from data_source import h5, sec
from data_source.wind_plugin import get_history_bar
from data_source.data_api import get_trade_days, trade_day_offset
from data_source.financial_data_source import BalanceSheet, IncomeSheet
from update_data import index_members, sector_members, index_weights, industry_classes

w.start()

@lru_cache()
def get_ashare(date):
    d = w.wset("sectorconstituent","date={date};sectorid=a001010100000000".format(date=idate))
    return d.Data[1]

@lru_cache()
def index_weight(index_id, date):
    d = w.wset("indexconstituent","date=%s;windcode=%s"%(date, index_id))
    ids = [x[:6] for x in d.Data[1]]
    weight = d.Data[3]
    return ids, weight

def update_price(start, end):
    """更新价量行情数据"""
    field_names = "收盘价 涨跌幅 最高价 最低价 成交量"
    data = get_history_bar(field_names.split(),start,end,**{'复权方式':'不复权'})
    data.columns = ['close','daily_returns_%','high','low','vol']
    data['vol'] = data['vol'] / 100
    data['daily_returns'] = data['daily_returns_%'] / 100
    h5.save_factor(data,'/stocks/')

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

def updateSectorConstituent(dates, windcode):
    """更新某一个指数在时间序列上的成分股"""
    l = []
    for date in dates:
        d = w.wset("sectorconstituent","date={date};windcode={windcode}".format(
            date=date, windcode=windcode))
        d = d.Data[1]
        d = pd.DataFrame(d, columns=['IDs'])
        d['_%s'%windcode[:6]] = 1
        d['IDs'] = d['IDs'].str[:6]
        d['date'] = DateStr2Datetime(date)
        l.append(d)
    d = pd.concat(l, ignore_index=True)
    d = d.set_index(['date','IDs']).sort_index()
    return d

def updateSectorConstituent2(dates, sectorid, column_mark):
    l = []
    for date in dates:
        d = w.wset("sectorconstituent","date={date};sectorid={sectorid}".format(
            date=date, sectorid=windcode))
        d = d.Data[1]
        d = pd.DataFrame(d, columns=['IDs'])
        d[column_mark] = 1
        d['IDs'] = d['IDs'].str[:6]
        d['date'] = DateStr2Datetime(date)
        l.append(d)
    d = pd.concat(l, ignore_index=True)
    d = d.set_index(['date','IDs']).sort_index()
    return d    

def update_sector(start, end):
    """更新成分股信息"""

    all_dates = get_trade_days(start, end)
    for index_id in index_members:
        d = updateSectorConstituent(all_dates, index_id)
        h5.save_factor(d, '/indexes/')
    
    for sectorid, column_mark in sector_members.items():
        d = updateSectorConstituent2(all_dates, sectorid, column_mark)
        h5.save_factor(d, '/stocks/')


def index_weight_panel(dates, index_id):
    months = (trade_day_offset(x, -1, '1m') for x in dates)
    l = []
    for i, m in enumerate(months):
        ids, weight = index_weight(index_id, m)
        idx = pd.MultiIndex.from_product([[DateStr2Datetime(dates[i])], ids], names=['date','IDs'])
        l.append(pd.Series(weight, index=idx))
    d = pd.concat(l).to_frame().rename(columns={0:'_%s_weight'%index_id[:6]})
    return d

def update_idx_weight(start, end):
    """更新指数权重"""
    all_dates = get_trade_days(start, end)
    for index_id in index_weights:
        d = index_weight_panel(all_dates, index_id)
        h5.save_factor(d, '/indexes/')

def get_stock_industryname(stocks, date, industryid, industrytype):
    data = w.wsd(stocks, industryid, date, date, "industryType=%s"%industrytype)
    idx = pd.MultiIndex.from_product([[DateStr2Datetime(date)], [x[:6] for x in stocks]])
    d = pd.Series(data.Data[0], index=idx)
    return d

def update_industry_name(start, end):
    all_dates = get_trade_days(start, end)
    for column, indutryparams in industry_classes.items():
        l = []
        for idate in all_dates:
            ids = get_ashare(idate)
            l.append(get_stock_industryname(ids, idate, *industry_classes))
        industry = pd.concat(l).to_frame().rename(columns={0:column})
        h5.save_factor(industry, '/indexes/')

def update_trade_status(start, end):
    dates = get_trade_days(start, end)
    
    st = sec.get_st(dates)
    suspend = sec.get_suspend(dates)
    uplimit = sec.get_uplimit(dates)
    downlimit = sec.get_downlimit(dates)

    trade_status = pd.concat([st,suspend,uplimit,downlimit], axis=1)
    trade_status = trade_status.where(pd.isnull(trade_status), other=1)
    trade_status.fillna(0, inplace=True)
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
    
UpdateFuncs = [update_price, update_sector, update_idx_weight,update_industry_name,
                update_trade_status, update_financial_data]

# UpdateFuncs = [update_sw_level_1]
for iFunc in UpdateFuncs:
    iFunc('20170425', '20170502')